package main

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/logger"
	"github.com/gorilla/mux"
)

var devlxdStoragePoolsEndpoint = DevLXDAPIEndpoint{
	Path: "storage-pools",
	Get:  DevLXDAPIEndpointAction{Handler: devlxdStoragePoolsGetHandler},
}

func devlxdStoragePoolsGetHandler(d *Daemon, w http.ResponseWriter, r *http.Request) *devLXDResponse {
	client, err := getVsockClient(d)
	if err != nil {
		return smartResponse(err)
	}

	pools, err := client.GetStoragePools()
	if err != nil {
		return smartResponse(fmt.Errorf("Failed to get storage pools: %w", err))
	}

	return okResponse(pools, "json")
}

var devlxdStoragePoolEndpoint = DevLXDAPIEndpoint{
	Path: "storage-pools/{pool}",
	Get:  DevLXDAPIEndpointAction{Handler: devlxdStoragePoolGetHandler},
}

func devlxdStoragePoolGetHandler(d *Daemon, w http.ResponseWriter, r *http.Request) *devLXDResponse {
	poolName, err := url.PathUnescape(mux.Vars(r)["pool"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	client, err := getVsockClient(d)
	if err != nil {
		return smartResponse(err)
	}

	pool, etag, err := client.GetStoragePool(poolName)
	if err != nil {
		return smartResponse(fmt.Errorf("Failed to get storage pool %q: %w", poolName, err))
	}

	w.Header().Set("Etag", etag)

	return okResponse(pool, "json")
}

var devlxdStoragePoolVolumesEndpoint = DevLXDAPIEndpoint{
	Path: "storage-pools/{pool}/volumes",
	Get:  DevLXDAPIEndpointAction{Handler: devlxdStoragePoolVolumesGetHandler},
	Post: DevLXDAPIEndpointAction{Handler: devlxdStoragePoolVolumesPostHandler},
}

var devlxdStoragePoolVolumesTypeEndpoint = DevLXDAPIEndpoint{
	Path: "storage-pools/{pool}/volumes/{type}",
	Get:  DevLXDAPIEndpointAction{Handler: devlxdStoragePoolVolumesGetHandler},
	Post: DevLXDAPIEndpointAction{Handler: devlxdStoragePoolVolumesPostHandler},
}

func devlxdStoragePoolVolumesGetHandler(d *Daemon, w http.ResponseWriter, r *http.Request) *devLXDResponse {
	poolName, err := url.PathUnescape(mux.Vars(r)["pool"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	client, err := getVsockClient(d)
	if err != nil {
		return smartResponse(err)
	}

	vols, err := client.GetStoragePoolVolumes(poolName)
	if err != nil {
		return smartResponse(fmt.Errorf("Failed to get volumes from storage pool %q: %w", poolName, err))
	}

	return okResponse(vols, "json")
}

func devlxdStoragePoolVolumesPostHandler(d *Daemon, w http.ResponseWriter, r *http.Request) *devLXDResponse {
	poolName, err := url.PathUnescape(mux.Vars(r)["pool"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	client, err := getVsockClient(d)
	if err != nil {
		return smartResponse(err)
	}

	var vol api.StorageVolumesPost
	err = parseRequestStruct(r, &vol)
	if err != nil {
		return smartResponse(fmt.Errorf("Failed to parse request: %w", err))
	}

	if vol.Type != "custom" {
		return errorResponse(http.StatusForbidden, fmt.Sprintf("Volume type %q not allowed", vol.Type))
	}

	logger.Warnf("Parsed create storage volume request: %+v", vol)

	err = client.CreateStoragePoolVolume(poolName, vol)
	logger.Errorf("Error creating storage volume: %v", err)
	if err != nil {
		return smartResponse(fmt.Errorf("Failed to get volumes from storage pool %q: %w", poolName, err))
	}

	return okResponse(nil, "raw")
}

var devlxdStoragePoolVolumeEndpoint = DevLXDAPIEndpoint{
	Path:   "storage-pools/{pool}/volumes/{type}/{volume}",
	Get:    DevLXDAPIEndpointAction{Handler: devlxdStoragePoolVolumeGetHandler},
	Put:    DevLXDAPIEndpointAction{Handler: devlxdStoragePoolVolumePutHandler},
	Delete: DevLXDAPIEndpointAction{Handler: devlxdStoragePoolVolumeDeleteHandler},
}

func devlxdStoragePoolVolumeGetHandler(d *Daemon, w http.ResponseWriter, r *http.Request) *devLXDResponse {
	poolName, err := url.PathUnescape(mux.Vars(r)["pool"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	volType, err := url.PathUnescape(mux.Vars(r)["type"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	volName, err := url.PathUnescape(mux.Vars(r)["volume"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	if volType != "custom" {
		return errorResponse(http.StatusForbidden, fmt.Sprintf("Volume type %q not allowed", volType))
	}

	client, err := getVsockClient(d)
	if err != nil {
		return smartResponse(err)
	}

	vol, etag, err := client.GetStoragePoolVolume(poolName, volType, volName)
	if err != nil {
		return smartResponse(fmt.Errorf("Failed to get volume %q from storage pool %q: %w", volName, poolName, err))
	}

	w.Header().Set("Etag", etag)
	return okResponse(vol, "json")
}

func devlxdStoragePoolVolumePutHandler(d *Daemon, w http.ResponseWriter, r *http.Request) *devLXDResponse {
	poolName, err := url.PathUnescape(mux.Vars(r)["pool"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	volType, err := url.PathUnescape(mux.Vars(r)["type"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	volName, err := url.PathUnescape(mux.Vars(r)["volume"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	if volType != "custom" {
		return errorResponse(http.StatusForbidden, fmt.Sprintf("Volume type %q not allowed", volType))
	}

	client, err := getVsockClient(d)
	if err != nil {
		return smartResponse(err)
	}

	var vol api.StorageVolumePut
	err = parseRequestStruct(r, &vol)
	if err != nil {
		return smartResponse(fmt.Errorf("Failed to parse request: %w", err))
	}

	etag := r.Header.Get("If-Match")

	err = client.UpdateStoragePoolVolume(poolName, volType, volName, vol, etag)
	if err != nil {
		return smartResponse(fmt.Errorf("Failed to update volume %q in storage pool %q: %w", volName, poolName, err))
	}

	return okResponse(nil, "raw")
}

func devlxdStoragePoolVolumeDeleteHandler(d *Daemon, w http.ResponseWriter, r *http.Request) *devLXDResponse {
	poolName, err := url.PathUnescape(mux.Vars(r)["pool"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	volType, err := url.PathUnescape(mux.Vars(r)["type"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	volName, err := url.PathUnescape(mux.Vars(r)["volume"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	if volType != "custom" {
		return errorResponse(http.StatusForbidden, fmt.Sprintf("Volume type %q not allowed", volType))
	}

	client, err := getVsockClient(d)
	if err != nil {
		return smartResponse(err)
	}

	err = client.DeleteStoragePoolVolume(poolName, volType, volName)
	if err != nil {
		return smartResponse(fmt.Errorf("Failed to delete volume %q from storage pool %q: %w", volName, poolName, err))
	}

	return okResponse(nil, "raw")
}
