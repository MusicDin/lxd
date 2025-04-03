package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/canonical/lxd/lxd/db"
	"github.com/canonical/lxd/lxd/device/config"
	"github.com/canonical/lxd/lxd/instance"
	"github.com/canonical/lxd/lxd/instance/instancetype"
	"github.com/canonical/lxd/lxd/response"
	"github.com/canonical/lxd/lxd/util"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/logger"
)

var devLXDInstanceDevicesEndpoint = devLXDAPIEndpoint{
	Path: "instances/{instanceName}/devices",
	Get:  devLXDAPIEndpointAction{Handler: devLXDInstanceDevicesGetHandler},
	Post: devLXDAPIEndpointAction{Handler: devLXDInstanceDevicesPostHandler},
}

func devLXDInstanceDevicesGetHandler(d *Daemon, r *http.Request) response.Response {
	inst, err := getInstanceFromContextAndCheckSecurityFlags(r.Context(), devLXDSecurityKey)
	if err != nil {
		return response.DevLXDErrorResponse(err, inst != nil && inst.Type() == instancetype.VM)
	}

	// Populate NIC hwaddr from volatile if not explicitly specified.
	// This is so cloud-init running inside the instance can identify the NIC when the interface name is
	// different than the LXD device name (such as when run inside a VM).
	localConfig := inst.LocalConfig()
	devices := inst.ExpandedDevices()
	for devName, devConfig := range devices {
		if devConfig["type"] == "nic" && devConfig["hwaddr"] == "" && localConfig["volatile."+devName+".hwaddr"] != "" {
			devices[devName]["hwaddr"] = localConfig["volatile."+devName+".hwaddr"]
		}
	}

	return response.DevLXDResponse(http.StatusOK, inst.ExpandedDevices(), "json", inst.Type() == instancetype.VM)
}

func devLXDInstanceDevicesPostHandler(d *Daemon, r *http.Request) response.Response {
	inst, err := getInstanceFromContextAndCheckSecurityFlags(r.Context(), devLXDSecurityKey)
	if err != nil {
		return response.DevLXDErrorResponse(err, inst != nil && inst.Type() == instancetype.VM)
	}

	s := d.State()

	projectName := inst.Project().Name
	targetInstName := mux.Vars(r)["instanceName"]

	var device map[string]string

	err = json.NewDecoder(r.Body).Decode(&device)
	if err != nil {
		return response.DevLXDErrorResponse(api.NewStatusError(http.StatusInternalServerError, "Failed to parse request: "+err.Error()), inst.Type() == instancetype.VM)
	}

	var volName string
	var poolName string
	var mountPath string

	for k, v := range device {
		switch k {
		case "volume":
			volName = v
		case "pool":
			poolName = v
		case "path":
			mountPath = v
		case "type":
			if v != "disk" {
				return response.DevLXDErrorResponse(api.NewStatusError(http.StatusBadRequest, fmt.Sprintf("Invalid device type %q", v)), inst.Type() == instancetype.VM)
			}
		case "propagation":
		default:
			return response.DevLXDErrorResponse(api.NewStatusError(http.StatusBadRequest, fmt.Sprintf("Invalid device property %q", k)), inst.Type() == instancetype.VM)
		}
	}

	// Quick check.
	if poolName == "" {
		return response.BadRequest(fmt.Errorf("Pool name in required"))
	}

	if volName == "" {
		return response.BadRequest(fmt.Errorf("Volume name in required"))
	}

	targetInst, err := instance.LoadByProjectAndName(d.State(), projectName, targetInstName)
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed to load instance: %w", err))
	}

	_, etag, err := targetInst.Render()
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed to render instance: %w", err))
	}

	_, ok := targetInst.ExpandedDevices()[volName]
	if ok {
		return response.Conflict(fmt.Errorf("Device %q already exists", volName))
	}

	targetInstDevices := targetInst.LocalDevices().Clone()
	if targetInstDevices == nil {
		targetInstDevices = make(config.Devices)
	}

	targetInstDevices[volName] = config.Device{
		"type":   "disk",
		"pool":   poolName,
		"source": volName,
		"path":   mountPath,
	}

	logger.Error("New devices:", logger.Ctx{"devices": fmt.Sprintf("%+v", targetInstDevices)})

	unlock, err := instanceOperationLock(s.ShutdownCtx, projectName, targetInstName)
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed to obtain the lock: %w", err))
	}

	defer unlock()

	// Validate the ETag
	err = util.EtagCheck(r, etag)
	if err != nil {
		return response.PreconditionFailed(fmt.Errorf("Failed to check ETag: %w", err))
	}

	args := db.InstanceArgs{
		Architecture: targetInst.Architecture(),
		Config:       targetInst.LocalConfig(),
		Description:  targetInst.Description(),
		Devices:      targetInstDevices,
		Ephemeral:    targetInst.IsEphemeral(),
		Profiles:     targetInst.Profiles(),
		Project:      projectName,
	}

	err = targetInst.Update(args, true)
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed to update instance: %w", err))
	}

	return response.DevLXDResponse(http.StatusOK, "", "raw", inst.Type() == instancetype.VM)
}

var devLXDInstanceDeviceEndpoint = devLXDAPIEndpoint{
	Path:   "instances/{instanceName}/devices/{deviceName}",
	Get:    devLXDAPIEndpointAction{Handler: devLXDInstanceDeviceGetHandler},
	Delete: devLXDAPIEndpointAction{Handler: devLXDInstanceDeviceDeleteHandler},
}

func devLXDInstanceDeviceGetHandler(d *Daemon, r *http.Request) response.Response {
	inst, err := getInstanceFromContextAndCheckSecurityFlags(r.Context(), devLXDSecurityKey)
	if err != nil {
		return response.DevLXDErrorResponse(err, inst != nil && inst.Type() == instancetype.VM)
	}

	// It is not allowed to anything outside the project where the current instance is running.
	projectName := inst.Project().Name

	targetInstName := mux.Vars(r)["instanceName"]
	devName := mux.Vars(r)["deviceName"]

	logger.Warn("devLXDDevicesHandler GET started", logger.Ctx{"name": targetInstName, "project": projectName})
	defer logger.Warn("devLXDDevicesHandler GET finished", logger.Ctx{"project": projectName, "name": targetInstName})

	targetInst, err := instance.LoadByProjectAndName(d.State(), projectName, targetInstName)
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed to load instance: %w", err))
	}

	dev, ok := targetInst.ExpandedDevices()[devName]
	if !ok {
		return response.DevLXDResponse(http.StatusNotFound, fmt.Sprintf("Device %q not found", devName), "raw", inst.Type() == instancetype.VM)
	}

	return response.DevLXDResponse(http.StatusOK, dev.Clone(), "json", inst.Type() == instancetype.VM)
}

func devLXDInstanceDeviceDeleteHandler(d *Daemon, r *http.Request) response.Response {
	inst, err := getInstanceFromContextAndCheckSecurityFlags(r.Context(), devLXDSecurityKey)
	if err != nil {
		return response.DevLXDErrorResponse(err, inst != nil && inst.Type() == instancetype.VM)
	}

	s := d.State()

	projectName := inst.Project().Name
	targetInstName := mux.Vars(r)["instanceName"]
	devName := mux.Vars(r)["deviceName"]

	logger.Warn("devLXDDevicesHandler DELETE started", logger.Ctx{"name": targetInstName, "project": projectName})
	defer logger.Warn("devLXDDevicesHandler DELETE finished", logger.Ctx{"project": projectName, "name": targetInstName})

	targetInst, err := instance.LoadByProjectAndName(d.State(), projectName, targetInstName)
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed to load instance: %w", err))
	}

	_, etag, err := targetInst.Render()
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed to render instance: %w", err))
	}

	// Search only local devices.
	dev, ok := targetInst.LocalDevices()[devName]
	if !ok {
		return response.DevLXDResponse(http.StatusNotFound, fmt.Sprintf("Device %q not found", devName), "raw", inst.Type() == instancetype.VM)
	}

	if dev["type"] != "disk" || dev["path"] == "/" {
		// DevLXD is not authorized to detach non-disk device or root disk device.
		return response.DevLXDErrorResponse(api.StatusErrorf(http.StatusForbidden, "Not authorized to detach device %q", devName), inst.Type() == instancetype.VM)
	}

	delete(targetInst.LocalDevices(), devName)

	unlock, err := instanceOperationLock(s.ShutdownCtx, projectName, targetInstName)
	if err != nil {
		return response.SmartError(err)
	}

	defer unlock()

	// Validate the ETag
	err = util.EtagCheck(r, etag)
	if err != nil {
		return response.PreconditionFailed(err)
	}

	args := db.InstanceArgs{
		Architecture: targetInst.Architecture(),
		Config:       targetInst.LocalConfig(),
		Description:  targetInst.Description(),
		Devices:      targetInst.LocalDevices(),
		Ephemeral:    targetInst.IsEphemeral(),
		Profiles:     targetInst.Profiles(),
		Project:      projectName,
	}

	err = targetInst.Update(args, true)
	if err != nil {
		return response.SmartError(err)
	}

	return response.DevLXDResponse(http.StatusOK, "", "raw", inst.Type() == instancetype.VM)
}
