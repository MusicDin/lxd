package main

import (
	"net/http"

	"github.com/gorilla/mux"

	"github.com/canonical/lxd/lxd/instance"
	"github.com/canonical/lxd/lxd/instance/instancetype"
	"github.com/canonical/lxd/lxd/response"
	"github.com/canonical/lxd/shared/api"
)

var devLXDStoragePoolsEndpoint = devLXDAPIEndpoint{
	Path: "storage-pools",
	Get:  devLXDAPIEndpointAction{Handler: devLXDStoragePoolsGetHandler},
}

func devLXDStoragePoolsGetHandler(d *Daemon, inst instance.Instance, w http.ResponseWriter, r *http.Request) response.Response {
	resp := checkDevLXDSecurityFlags(inst, devLXDSecurityKey)
	if resp != nil {
		return resp
	}

	return storagePoolsGet(d, r)
}

var devLXDStoragePoolEndpoint = devLXDAPIEndpoint{
	Path: "storage-pools/{poolName}",
	Get:  devLXDAPIEndpointAction{Handler: devLXDStoragePoolGetHandler},
}

func devLXDStoragePoolGetHandler(d *Daemon, inst instance.Instance, w http.ResponseWriter, r *http.Request) response.Response {
	resp := checkDevLXDSecurityFlags(inst, devLXDSecurityKey)
	if resp != nil {
		return resp
	}

	return storagePoolGet(d, r)
}

var devLXDStoragePoolVolumesEndpoint = devLXDAPIEndpoint{
	Path: "storage-pools/{poolName}/volumes",
	Get:  devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumesGetHandler},
	Post: devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumesPostHandler},
}

var devLXDStoragePoolVolumesTypeEndpoint = devLXDAPIEndpoint{
	Path: "storage-pools/{poolName}/volumes/{type}",
	Get:  devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumesGetHandler},
	Post: devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumesPostHandler},
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

var devLXDStoragePoolVolumeEndpoint = devLXDAPIEndpoint{
	Path:   "storage-pools/{poolName}/volumes/{type}/{volumeName}",
	Get:    devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumeGetHandler},
	Put:    devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumePutHandler},
	Delete: devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumeDeleteHandler},
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
