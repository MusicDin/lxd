package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/canonical/lxd/lxd/device/config"
	"github.com/canonical/lxd/lxd/response"
	"github.com/canonical/lxd/shared/api"
)

var devLXDInstanceEndpoint = devLXDAPIEndpoint{
	Path: "instances/{name}",
	Get:  devLXDAPIEndpointAction{Handler: devLXDInstanceGetHandler},
}

func devLXDInstanceGetHandler(d *Daemon, r *http.Request) response.Response {
	inst, err := getInstanceFromContextAndCheckSecurityFlags(r.Context(), devLXDSecurityKey, devLXDSecurityMgmtVolumesKey)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	// Allow access only to the projectName where current instance is running.
	projectName := inst.Project().Name
	targetInstName := mux.Vars(r)["name"]

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

	// Map to devLXD type.
	respInst := api.DevLXDInstance{
		Name:    targetInst.Name,
		Devices: targetInst.ExpandedDevices,
	}

	return response.DevLXDResponseETag(http.StatusOK, respInst, "json", etag)
}

func devLXDInstanceDevicesPostHandler(d *Daemon, r *http.Request) response.Response {
	inst, err := getInstanceFromContextAndCheckSecurityFlags(r.Context(), devLXDSecurityKey, devLXDSecurityMgmtVolumesKey)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	// Allow access only to the project where current instance is running.
	projectName := inst.Project().Name
	targetInstName := mux.Vars(r)["name"]

	var device map[string]string

	err = json.NewDecoder(r.Body).Decode(&device)
	if err != nil {
		return response.DevLXDErrorResponse(api.StatusErrorf(http.StatusInternalServerError, "Failed to parse request: %w", err))
	}

	var volName string
	var poolName string
	var mountPath string

	for k, v := range device {
		switch k {
		case "source":
			volName = v
		case "pool":
			poolName = v
		case "path":
			mountPath = v
		case "type":
			// Ensure the device type is provided.
			if v == "" {
				return response.DevLXDErrorResponse(api.NewStatusError(http.StatusBadRequest, "Device type is required"))
			}

			// Currently we allow attaching only disk devices.
			if v != "disk" {
				return response.DevLXDErrorResponse(api.StatusErrorf(http.StatusBadRequest, "Invalid device type %q", v))
			}

		default:
			return response.DevLXDErrorResponse(api.StatusErrorf(http.StatusBadRequest, "Invalid device property %q", k))
		}
	}

	// Quick check.
	if poolName == "" {
		return response.DevLXDErrorResponse(api.NewStatusError(http.StatusBadRequest, "Pool name is required"))
	}

	if volName == "" {
		return response.DevLXDErrorResponse(api.NewStatusError(http.StatusBadRequest, "Volume name is required"))
	}

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

	// Check if the device already exists.
	_, ok := targetInst.ExpandedDevices[volName]
	if ok {
		return response.DevLXDErrorResponse(api.NewStatusError(http.StatusConflict, fmt.Sprintf("Device %q already exists", volName)))
	}

	// Ensure devices map is initialized.
	if targetInst.Devices == nil {
		targetInst.Devices = make(map[string]map[string]string)
	}

	targetInst.Devices[volName] = config.Device{
		"type":   "disk",
		"pool":   poolName,
		"source": volName,
		"path":   mountPath,
	}

	// Update instance with the new device.
	reqBody := api.InstancePut{
		Devices: targetInst.Devices,
	}

	etag := r.Header.Get("If-Match")

	url = api.NewURL().Path("1.0", "instances", targetInstName).WithQuery("project", projectName).URL
	req, err = NewRequestWithContext(r.Context(), http.MethodPatch, url.String(), reqBody, etag)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	resp = instancePatch(d, req)
	err = Render(req, resp)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	return response.DevLXDResponse(http.StatusOK, "", "raw")
}
