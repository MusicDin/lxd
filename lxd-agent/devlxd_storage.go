package main

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/gorilla/mux"

	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/logger"
)

var devLXDStoragePoolsEndpoint = devLXDAPIEndpoint{
	Path: "storage-pools",
	Get:  devLXDAPIEndpointAction{Handler: devLXDStoragePoolsGetHandler},
}

func devLXDStoragePoolsGetHandler(d *Daemon, r *http.Request) *devLXDResponse {
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

var devLXDStoragePoolEndpoint = devLXDAPIEndpoint{
	Path: "storage-pools/{pool}",
	Get:  devLXDAPIEndpointAction{Handler: devLXDStoragePoolGetHandler},
}

func devLXDStoragePoolGetHandler(d *Daemon, r *http.Request) *devLXDResponse {
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

	return manualResponse(func(w http.ResponseWriter) error {
		// Set the ETag header.
		w.Header().Set("Etag", etag)

		// Write the response.
		return okResponse(pool, "json").Render(w, r)
	})
}

var devLXDStoragePoolVolumesEndpoint = devLXDAPIEndpoint{
	Path: "storage-pools/{pool}/volumes",
	Get:  devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumesGetHandler},
	Post: devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumesPostHandler},
}

var devLXDStoragePoolVolumesTypeEndpoint = devLXDAPIEndpoint{
	Path: "storage-pools/{pool}/volumes/{type}",
	Get:  devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumesGetHandler},
	Post: devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumesPostHandler},
}

func devLXDStoragePoolVolumesGetHandler(d *Daemon, r *http.Request) *devLXDResponse {
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

func devLXDStoragePoolVolumesPostHandler(d *Daemon, r *http.Request) *devLXDResponse {
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

var devLXDStoragePoolVolumeEndpoint = devLXDAPIEndpoint{
	Path:   "storage-pools/{pool}/volumes/{type}/{volume}",
	Get:    devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumeGetHandler},
	Put:    devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumePutHandler},
	Delete: devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumeDeleteHandler},
}

func devLXDStoragePoolVolumeGetHandler(d *Daemon, r *http.Request) *devLXDResponse {
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

	return manualResponse(func(w http.ResponseWriter) error {
		// Set the ETag header.
		w.Header().Set("Etag", etag)

		// Write the response.
		return okResponse(vol, "json").Render(w, r)
	})
}

func devLXDStoragePoolVolumePutHandler(d *Daemon, r *http.Request) *devLXDResponse {
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

func devLXDStoragePoolVolumeDeleteHandler(d *Daemon, r *http.Request) *devLXDResponse {
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
