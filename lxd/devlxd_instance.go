package main

import (
	"encoding/json"
	"maps"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/canonical/lxd/lxd/device/filters"
	"github.com/canonical/lxd/lxd/instance"
	"github.com/canonical/lxd/lxd/response"
	"github.com/canonical/lxd/lxd/util"
	"github.com/canonical/lxd/shared/api"
)

type deviceAccessCheckFunc func(device map[string]string) bool

var devLXDInstanceEndpoint = devLXDAPIEndpoint{
	Path: "instances/{name}",
	Get:  devLXDAPIEndpointAction{Handler: devLXDInstanceGetHandler},
	Put:  devLXDAPIEndpointAction{Handler: devLXDInstancePutHandler},
}

func devLXDInstanceGetHandler(d *Daemon, r *http.Request) response.Response {
	inst, err := getInstanceFromContextAndCheckSecurityFlags(r.Context(), devLXDSecurityKey, devLXDSecurityMgmtVolumesKey)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	// Allow access only to the projectName where current instance is running.
	projectName := inst.Project().Name
	targetInstName := mux.Vars(r)["name"]

	// TODO: Get actual service account ID.
	serviceAccountID := ""

	// Fetch instance.
	targetInst := api.Instance{}

	url := api.NewURL().Path("1.0", "instances", targetInstName).WithQuery("recursion", "1").WithQuery("project", projectName).URL
	req, err := NewRequestWithContext(r.Context(), http.MethodGet, url.String(), nil, "")
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	resp := instanceGet(d, req)
	_, err = RenderToStruct(req, resp, &targetInst)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	// Filter accessible devices.
	deviceAccessChecker := newDeviceAccessCheckFunc(inst)
	devices, _ := getAccessibleDevices(targetInst, serviceAccountID, deviceAccessChecker)

	// Map to devLXD type.
	respInst := api.DevLXDInstance{
		Name:    targetInst.Name,
		Devices: devices,
	}

	// Use custom etag for devLXD instances.
	//
	// It is important that we track "owned" devices, not all devices.
	// If the devLXD access changes, the LXD instance ETag remains the
	// same, but from the perspective of devLXD the instance might have
	// changed (list of accessible devices is different).
	etag, err := util.EtagHash(respInst)
	if err != nil {
		return response.DevLXDErrorResponse(api.StatusErrorf(http.StatusInternalServerError, "Failed to generate ETag: %w", err))
	}

	return response.DevLXDResponseETag(http.StatusOK, respInst, "json", etag)
}

func devLXDInstancePutHandler(d *Daemon, r *http.Request) response.Response {
	inst, err := getInstanceFromContextAndCheckSecurityFlags(r.Context(), devLXDSecurityKey, devLXDSecurityMgmtVolumesKey)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	// Allow access only to the project where current instance is running.
	projectName := inst.Project().Name
	targetInstName := mux.Vars(r)["name"]

	// TODO: Get actual service account ID.
	serviceAccountID := ""

	var reqInst api.DevLXDInstance

	err = json.NewDecoder(r.Body).Decode(&reqInst)
	if err != nil {
		return response.DevLXDErrorResponse(api.StatusErrorf(http.StatusInternalServerError, "Failed to parse request: %w", err))
	}

	// Fetch instance.
	targetInst := api.Instance{}

	url := api.NewURL().Path("1.0", "instances", targetInstName).WithQuery("recursion", "1").WithQuery("project", projectName).URL
	req, err := NewRequestWithContext(r.Context(), http.MethodGet, url.String(), nil, "")
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	resp := instanceGet(d, req)
	etag, err := RenderToStruct(req, resp, &targetInst)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	// Check ETag.
	//
	// ETag returned to the client is the devLXD instance ETag, not the ETag returned by LXD.
	// Using this ETag, we detect that the instance the accessible instance devices has not changed
	// since the last request, and we can proceed with the update.
	//
	// The LXD instance ETag received from the "instanceGet" is later used in "instancePatch" to
	// ensure nothing has changed between those two requests.
	reqETag := r.Header.Get("If-Match")
	if reqETag != "" {
		// Calculate devLXD instance ETag.
		deviceAccessChecker := newDeviceAccessCheckFunc(inst)
		devices, _ := getAccessibleDevices(targetInst, serviceAccountID, deviceAccessChecker)

		devLXDInst := api.DevLXDInstance{
			Name:    targetInst.Name,
			Devices: devices,
		}

		devLXDETag, err := util.EtagHash(devLXDInst)
		if err != nil {
			return response.DevLXDErrorResponse(api.StatusErrorf(http.StatusInternalServerError, "Failed to generate ETag: %w", err))
		}

		if reqETag != devLXDETag {
			return response.DevLXDErrorResponse(api.StatusErrorf(http.StatusPreconditionFailed, "ETag mismatch: %q != %q", reqETag, devLXDETag))
		}
	}

	// Merge new devices with existing ones.
	deviceChecker := newDeviceAccessCheckFunc(inst)
	err = updateInstanceDevices(&targetInst, reqInst, serviceAccountID, deviceChecker)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	// Update instance.
	reqBody := targetInst.Writable()

	url = api.NewURL().Path("1.0", "instances", targetInstName).WithQuery("project", projectName).URL
	req, err = NewRequestWithContext(r.Context(), http.MethodPut, url.String(), reqBody, etag)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	resp = instancePut(d, req)
	err = Render(req, resp)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	return response.DevLXDResponse(http.StatusOK, "", "raw")
}

// updateInstanceDevices updates an existing instance (api.Instance) with devices from the
// request instance (api.DevLXDInstance), and adjusts the device ownership configuration
// accordingly.
//
// Device actions are determined as follows:
// - Add:
//   - Condition: New device that devLXD can manage and is not present in the existing devices.
//   - Action:    Adds new device to the instance and set its owner in the instance config.
//
// - Update:
//   - Condition: Existing device that devLXD can manage and is present in the new devices.
//   - Action:    Updates existing device in the instance.
//
// - Remove:
//   - Condition: Existing device that devLXD can manage and is not present in the new devices.
//   - Action:    Removes existing device from the instance and removes its owner from the instance config.
func updateInstanceDevices(inst *api.Instance, req api.DevLXDInstance, serviceAccountID string, isDeviceAccessible deviceAccessCheckFunc) error {
	newDevices := make(map[string]map[string]string)

	// Pass local devices, as non-local devices cannot be owned.
	accessibleDevices, otherDevices := getAccessibleDevices(*inst, serviceAccountID, isDeviceAccessible)

	// Merge new devices into existing ones.
	for name, device := range req.Devices {
		// Make sure device is not nil.
		if device == nil {
			return api.StatusErrorf(http.StatusBadRequest, "Device %q cannot be nil", name)
		}

		// Ensure devLXD has sufficient permissions to manage the device.
		if isDeviceAccessible != nil && !isDeviceAccessible(device) {
			return api.StatusErrorf(http.StatusForbidden, "Not authorized to manage device %q", name)
		}

		// Ensure unaccessible device cannot be modified.
		_, exists := inst.ExpandedDevices[name]
		_, canAccess := accessibleDevices[name]
		if exists && !canAccess {
			return api.StatusErrorf(http.StatusForbidden, "Not authorized to manage device %q", name)
		}

		// Either new device is added or an existing one updated.
		// At this point we know that the ownership is correct (there is
		// no existing unowned device), so we can safely set the device owner
		// in instance configuration.
		newDevices[name] = device
		// inst.Config["volatile."+name+".devlxd.owner"] = serviceAccountID # TODO: Uncomment once available.
	}

	// Find removed devices, and remove their owner configuration keys.
	for name := range accessibleDevices {
		_, exists := req.Devices[name]
		if !exists {
			// Device is removed, so remove the owner config key.
			delete(inst.Config, "volatile."+name+".devlxd.owner")
		}
	}

	// Add non-owned existing devices to the existing list of devices.
	maps.Copy(newDevices, otherDevices)

	inst.Devices = newDevices
	return nil
}

// getAccessibleDevices extracts accessible instance devices. Two maps are returned,
// one containg accessible devices, and the other containing the remaining devices.
//
// Device is accessible if:
// - Device owner matches the service account ID.
// - Device type matches the allowed device types.
//
// Additionall restrictions::
// - Disk device is accessible only if it is a custom volume disk device.
func getAccessibleDevices(inst api.Instance, serviceAccountID string, isDeviceAccessible deviceAccessCheckFunc) (accessibleDevices map[string]map[string]string, otherDevices map[string]map[string]string) {
	accessibleDevices = make(map[string]map[string]string)
	otherDevices = make(map[string]map[string]string)

	for name, device := range inst.Devices {
		if isDeviceAccessible != nil && isDeviceAccessible(device) && inst.Config["volatile."+name+".devlxd.owner"] == serviceAccountID {
			accessibleDevices[name] = device
		} else {
			otherDevices[name] = device
		}
	}

	return accessibleDevices, otherDevices
}

// newDeviceAccessCheckFunc returns a device validator function that checks if the given
// device is accessible by the devLXD.
func newDeviceAccessCheckFunc(inst instance.Instance) deviceAccessCheckFunc {
	diskDeviceAllowed := hasSecurityFlags(inst, devLXDSecurityMgmtVolumesKey)

	return func(device map[string]string) bool {
		return filters.IsCustomVolumeDisk(device) && diskDeviceAllowed
	}
}
