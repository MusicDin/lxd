package main

import (
	"maps"
	"net/http"

	"github.com/canonical/lxd/lxd/device/filters"
	"github.com/canonical/lxd/lxd/instance"
	"github.com/canonical/lxd/shared/api"
)

type devLXDDeviceAccessValidator func(device map[string]string) bool

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
func updateInstanceDevices(inst *api.Instance, req api.DevLXDInstance, serviceAccountID string, isDeviceAccessible devLXDDeviceAccessValidator) error {
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
func getAccessibleDevices(inst api.Instance, serviceAccountID string, isDeviceAccessible devLXDDeviceAccessValidator) (accessibleDevices map[string]map[string]string, otherDevices map[string]map[string]string) {
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

// newDevLXDDeviceAccessValidator returns a device validator function that
// checks if the given device is accessible by the devLXD.
func newDevLXDDeviceAccessValidator(inst instance.Instance) devLXDDeviceAccessValidator {
	diskDeviceAllowed := hasInstanceSecurityFeatures(inst, devLXDSecurityMgmtVolumesKey)

	return func(device map[string]string) bool {
		return filters.IsCustomVolumeDisk(device) && diskDeviceAllowed
	}
}
