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
	"github.com/canonical/lxd/shared/revert"
)

var devLXDInstanceDevicesEndpoint = devLXDAPIEndpoint{
	Path: "instances/{instanceName}/devices",
	Get:  devLXDAPIEndpointAction{Handler: devLXDInstanceDevicesGetHandler},
	Post: devLXDAPIEndpointAction{Handler: devLXDInstanceDevicesPostHandler},
}

func devLXDInstanceDevicesGetHandler(d *Daemon, inst instance.Instance, w http.ResponseWriter, r *http.Request) response.Response {
	resp := checkDevLXDSecurityFlags(inst, devLXDSecurityKey)
	if resp != nil {
		return resp
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

func devLXDInstanceDevicesPostHandler(d *Daemon, inst instance.Instance, w http.ResponseWriter, r *http.Request) response.Response {
	resp := checkDevLXDSecurityFlags(inst, devLXDSecurityKey)
	if resp != nil {
		return resp
	}

	if inst.Type() == instancetype.Container {
		return response.DevLXDErrorResponse(api.NewStatusError(http.StatusBadRequest, "Device attachment is only supported for virtual machines"), inst.Type() == instancetype.VM)
	}

	s := d.State()

	projectName := inst.Project().Name
	instName := mux.Vars(r)["instanceName"]

	logger.Warn("devLXDDevicesHandler POST started", logger.Ctx{"name": instName, "project": projectName})
	defer logger.Warn("devLXDDevicesHandler POST finished", logger.Ctx{"project": projectName, "name": instName})

	// type DeviceAttachment struct {
	// 	VolumeName  string `json:"volume"`
	// 	PoolName    string `json:"pool"`
	// 	Path        string `json:"path"` // Path within the instance
	// 	Propagation string `json:"propagation"`
	// }

	// var req DeviceAttachment
	// err := json.NewDecoder(r.Body).Decode(&req)
	// if err != nil {
	// 	return response.BadRequest(fmt.Errorf("Failed to decode request: %w", err))
	// }

	var device map[string]string

	err := json.NewDecoder(r.Body).Decode(&device)
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

	targetInst, err := instance.LoadByProjectAndName(d.State(), projectName, instName)
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed to load instance: %w", err))
	}

	_, etag, err := targetInst.Render()
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed to render instance: %w", err))
	}

	_, ok := targetInst.ExpandedDevices()[volName]
	if ok {
		// if dev["type"] == "disk" && dev["pool"] == poolName {
		// 	return response.DevLXDResponse(http.StatusOK, "Device is already attached", "raw", inst.Type() == instancetype.VM)
		// }

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

	unlock, err := instanceOperationLock(s.ShutdownCtx, projectName, instName)
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

func devLXDInstanceDeviceGetHandler(d *Daemon, c instance.Instance, w http.ResponseWriter, r *http.Request) response.Response {
	resp := checkDevLXDSecurityFlags(c, devLXDSecurityKey)
	if resp != nil {
		return resp
	}

	// It is not allowed to anything outside the project where the current instance is running.
	projectName := c.Project().Name

	instName := mux.Vars(r)["instanceName"]
	devName := mux.Vars(r)["deviceName"]

	logger.Warn("devLXDDevicesHandler GET started", logger.Ctx{"name": instName, "project": projectName})
	defer logger.Warn("devLXDDevicesHandler GET finished", logger.Ctx{"project": projectName, "name": instName})

	inst, err := instance.LoadByProjectAndName(d.State(), projectName, instName)
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed to load instance: %w", err))
	}

	dev, ok := inst.ExpandedDevices()[devName]
	if !ok {
		return response.DevLXDResponse(http.StatusNotFound, fmt.Sprintf("Device %q not found", devName), "raw", c.Type() == instancetype.VM)
	}

	return response.DevLXDResponse(http.StatusOK, dev.Clone(), "json", c.Type() == instancetype.VM)
}

func devLXDInstanceDeviceDeleteHandler(d *Daemon, c instance.Instance, w http.ResponseWriter, r *http.Request) response.Response {
	resp := checkDevLXDSecurityFlags(c, devLXDSecurityKey)
	if resp != nil {
		return resp
	}

	if c.Type() == instancetype.Container {
		return response.DevLXDErrorResponse(api.NewStatusError(http.StatusBadRequest, "Device attachment is only supported for virtual machines"), c.Type() == instancetype.VM)
	}

	s := d.State()

	// It is not allowed to anything outside the project where the current instance is running.
	projectName := c.Project().Name

	instName := mux.Vars(r)["instanceName"]
	devName := mux.Vars(r)["deviceName"]

	logger.Warn("devLXDDevicesHandler DELETE started", logger.Ctx{"name": instName, "project": projectName})
	defer logger.Warn("devLXDDevicesHandler DELETE finished", logger.Ctx{"project": projectName, "name": instName})

	inst, err := instance.LoadByProjectAndName(d.State(), projectName, instName)
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed to load instance: %w", err))
	}

	_, etag, err := inst.Render()
	if err != nil {
		return response.SmartError(fmt.Errorf("Failed to render instance: %w", err))
	}

	// Search only local devices.
	dev, ok := inst.LocalDevices()[devName]
	if !ok {
		return response.DevLXDResponse(http.StatusNotFound, fmt.Sprintf("Device %q not found", devName), "raw", c.Type() == instancetype.VM)
	}

	if dev["type"] != "disk" || dev["path"] == "/" {
		// DevLXD is not authorized to detach non-disk device or root disk device.
		return response.DevLXDErrorResponse(api.StatusErrorf(http.StatusForbidden, "Not authorized to detach device %q", devName), c.Type() == instancetype.VM)
	}

	delete(inst.LocalDevices(), devName)

	unlock, err := instanceOperationLock(s.ShutdownCtx, projectName, instName)
	if err != nil {
		return response.SmartError(err)
	}

	defer unlock()

	revert := revert.New()
	defer revert.Fail()

	revert.Add(func() {
		unlock()
	})

	// Validate the ETag
	err = util.EtagCheck(r, etag)
	if err != nil {
		return response.PreconditionFailed(err)
	}

	args := db.InstanceArgs{
		Architecture: inst.Architecture(),
		Config:       inst.LocalConfig(),
		Description:  inst.Description(),
		Devices:      inst.LocalDevices(),
		Ephemeral:    inst.IsEphemeral(),
		Profiles:     inst.Profiles(),
		Project:      projectName,
	}

	err = inst.Update(args, true)
	if err != nil {
		return response.SmartError(err)
	}

	return response.DevLXDResponse(http.StatusOK, "", "raw", c.Type() == instancetype.VM)
}
