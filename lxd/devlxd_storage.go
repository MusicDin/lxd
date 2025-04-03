package main

import (
	"net/http"

	"github.com/gorilla/mux"

	"github.com/canonical/lxd/lxd/instance"
	"github.com/canonical/lxd/lxd/instance/instancetype"
	"github.com/canonical/lxd/lxd/response"
	"github.com/canonical/lxd/shared/api"
)

var devLXDStoragePoolsEndpoint = DevLXDAPIEndpoint{
	Path: "storage-pools",
	Get:  DevLXDAPIEndpointAction{Handler: devLXDStoragePoolsGetHandler},
}

func devLXDStoragePoolsGetHandler(d *Daemon, inst instance.Instance, w http.ResponseWriter, r *http.Request) response.Response {
	resp := checkDevLXDSecurityFlags(inst, devLXDSecurityKey)
	if resp != nil {
		return resp
	}

	return storagePoolsGet(d, r)
}

var devLXDStoragePoolEndpoint = DevLXDAPIEndpoint{
	Path: "storage-pools/{poolName}",
	Get:  DevLXDAPIEndpointAction{Handler: devLXDStoragePoolGetHandler},
}

func devLXDStoragePoolGetHandler(d *Daemon, inst instance.Instance, w http.ResponseWriter, r *http.Request) response.Response {
	resp := checkDevLXDSecurityFlags(inst, devLXDSecurityKey)
	if resp != nil {
		return resp
	}

	return storagePoolGet(d, r)
}

var devLXDStoragePoolVolumesEndpoint = DevLXDAPIEndpoint{
	Path: "storage-pools/{poolName}/volumes",
	Get:  DevLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumesGetHandler},
	Post: DevLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumesPostHandler},
}

var devLXDStoragePoolVolumesTypeEndpoint = DevLXDAPIEndpoint{
	Path: "storage-pools/{poolName}/volumes/{type}",
	Get:  DevLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumesGetHandler},
	Post: DevLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumesPostHandler},
}

func devLXDStoragePoolVolumesGetHandler(d *Daemon, inst instance.Instance, w http.ResponseWriter, r *http.Request) response.Response {
	resp := checkDevLXDSecurityFlags(inst, devLXDSecurityKey)
	if resp != nil {
		return resp
	}

	return storagePoolVolumesGet(d, r)
}

func devLXDStoragePoolVolumesPostHandler(d *Daemon, inst instance.Instance, w http.ResponseWriter, r *http.Request) response.Response {
	resp := checkDevLXDSecurityFlags(inst, devLXDSecurityKey)
	if resp != nil {
		return resp
	}

	return storagePoolVolumesPost(d, r)
}

var devLXDStoragePoolVolumeEndpoint = DevLXDAPIEndpoint{
	Path:   "storage-pools/{poolName}/volumes/{type}/{volumeName}",
	Get:    DevLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumeGetHandler},
	Put:    DevLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumePutHandler},
	Delete: DevLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumeDeleteHandler},
	// Patch:  DevLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumePatchHandler},
}

func devLXDStoragePoolVolumeGetHandler(d *Daemon, inst instance.Instance, w http.ResponseWriter, r *http.Request) response.Response {
	resp := checkDevLXDSecurityFlags(inst, devLXDSecurityKey)
	if resp != nil {
		return resp
	}

	// Restrict access to custom volumes.
	volType := mux.Vars(r)["type"]
	if volType != "custom" {
		return response.DevLXDErrorResponse(api.StatusErrorf(http.StatusForbidden, "not authorized"), inst.Type() == instancetype.VM)
	}

	err := addStoragePoolVolumeDetailsToRequestContext(d.State(), r)
	if err != nil {
		return response.SmartError(err)
	}

	return storagePoolVolumeGet(d, r)
}

func devLXDStoragePoolVolumePutHandler(d *Daemon, inst instance.Instance, w http.ResponseWriter, r *http.Request) response.Response {
	resp := checkDevLXDSecurityFlags(inst, devLXDSecurityKey)
	if resp != nil {
		return resp
	}

	// Restrict access to custom volumes.
	volType := mux.Vars(r)["type"]
	if volType != "custom" {
		return response.DevLXDErrorResponse(api.StatusErrorf(http.StatusForbidden, "not authorized"), inst.Type() == instancetype.VM)
	}

	err := addStoragePoolVolumeDetailsToRequestContext(d.State(), r)
	if err != nil {
		return response.SmartError(err)
	}

	return storagePoolVolumePut(d, r)
}

func devLXDStoragePoolVolumeDeleteHandler(d *Daemon, inst instance.Instance, w http.ResponseWriter, r *http.Request) response.Response {
	resp := checkDevLXDSecurityFlags(inst, devLXDSecurityKey)
	if resp != nil {
		return resp
	}

	// Restrict access to custom volumes.
	volType := mux.Vars(r)["type"]
	if volType != "custom" {
		return response.DevLXDErrorResponse(api.StatusErrorf(http.StatusForbidden, "not authorized"), inst.Type() == instancetype.VM)
	}

	err := addStoragePoolVolumeDetailsToRequestContext(d.State(), r)
	if err != nil {
		return response.SmartError(err)
	}

	return storagePoolVolumeDelete(d, r)
}
