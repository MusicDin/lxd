package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/gorilla/mux"

	"github.com/canonical/lxd/lxd/util"
	"github.com/canonical/lxd/shared/api"
)

var devLXDStoragePoolEndpoint = devLXDAPIEndpoint{
	Path: "storage-pools/{pool}",
	Get:  devLXDAPIEndpointAction{Handler: devLXDStoragePoolGetHandler},
}

func devLXDStoragePoolGetHandler(d *Daemon, r *http.Request) *devLXDResponse {
	poolName, err := url.PathUnescape(mux.Vars(r)["pool"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	client, err := getDevLXDVsockClient(d, r)
	if err != nil {
		return smartResponse(err)
	}

	defer client.Disconnect()

	pool, etag, err := client.GetStoragePool(poolName)
	if err != nil {
		return smartResponse(err)
	}

	return okResponseETag(pool, "json", etag)
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
	// Non-recursive requests are currently not supported.
	if !util.IsRecursionRequest(r) {
		return errorResponse(http.StatusNotImplemented, "Only recursive requests are currently supported")
	}

	poolName, err := url.PathUnescape(mux.Vars(r)["pool"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	client, err := getDevLXDVsockClient(d, r)
	if err != nil {
		return smartResponse(err)
	}

	client = client.UseTarget(r.URL.Query().Get("target"))
	defer client.Disconnect()

	vols, err := client.GetStoragePoolVolumes(poolName)
	if err != nil {
		return smartResponse(err)
	}

	return okResponse(vols, "json")
}

func devLXDStoragePoolVolumesPostHandler(d *Daemon, r *http.Request) *devLXDResponse {
	poolName, err := url.PathUnescape(mux.Vars(r)["pool"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	volType, err := url.PathUnescape(mux.Vars(r)["type"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	var vol api.DevLXDStorageVolumesPost
	err = json.NewDecoder(r.Body).Decode(&vol)
	if err != nil {
		return smartResponse(fmt.Errorf("Failed to parse request: %w", err))
	}

	if vol.Type == "" {
		vol.Type = volType
	}

	client, err := getDevLXDVsockClient(d, r)
	if err != nil {
		return smartResponse(err)
	}

	client = client.UseTarget(r.URL.Query().Get("target"))
	defer client.Disconnect()

	err = client.CreateStoragePoolVolume(poolName, vol)
	if err != nil {
		return smartResponse(err)
	}

	return okResponse("", "raw")
}

var devLXDStoragePoolVolumeTypeEndpoint = devLXDAPIEndpoint{
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

	client, err := getDevLXDVsockClient(d, r)
	if err != nil {
		return smartResponse(err)
	}

	client = client.UseTarget(r.URL.Query().Get("target"))
	defer client.Disconnect()

	vol, etag, err := client.GetStoragePoolVolume(poolName, volType, volName)
	if err != nil {
		return smartResponse(err)
	}

	return okResponseETag(vol, "json", etag)
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

	etag := r.Header.Get("If-Match")

	var vol api.DevLXDStorageVolumePut
	err = json.NewDecoder(r.Body).Decode(&vol)
	if err != nil {
		return smartResponse(fmt.Errorf("Failed to parse request: %w", err))
	}

	client, err := getDevLXDVsockClient(d, r)
	if err != nil {
		return smartResponse(err)
	}

	client = client.UseTarget(r.URL.Query().Get("target"))
	defer client.Disconnect()

	err = client.UpdateStoragePoolVolume(poolName, volType, volName, vol, etag)
	if err != nil {
		return smartResponse(err)
	}

	return okResponse("", "raw")
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

	client, err := getDevLXDVsockClient(d, r)
	if err != nil {
		return smartResponse(err)
	}

	client = client.UseTarget(r.URL.Query().Get("target"))
	defer client.Disconnect()

	err = client.DeleteStoragePoolVolume(poolName, volType, volName)
	if err != nil {
		return smartResponse(err)
	}

	return okResponse("", "raw")
}

var devLXDStoragePoolVolumeSnapshotsEndpoint = devLXDAPIEndpoint{
	Path: "storage-pools/{pool}/volumes/{type}/{volume}/snapshots",
	Get:  devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumeSnapshotsGetHandler},
	Post: devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumeSnapshotsPostHandler},
}

func devLXDStoragePoolVolumeSnapshotsGetHandler(d *Daemon, r *http.Request) *devLXDResponse {
	// Non-recursive requests are currently not supported.
	if !util.IsRecursionRequest(r) {
		return errorResponse(http.StatusNotImplemented, "Only recursive requests are currently supported")
	}

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

	client, err := getDevLXDVsockClient(d)
	if err != nil {
		return smartResponse(err)
	}

	snapshots, err := client.GetStoragePoolVolumeSnapshots(poolName, volType, volName)
	if err != nil {
		return smartResponse(err)
	}

	return okResponse(snapshots, "json")
}

func devLXDStoragePoolVolumeSnapshotsPostHandler(d *Daemon, r *http.Request) *devLXDResponse {
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

	var snapshot api.DevLXDStorageVolumeSnapshotsPost
	err = json.NewDecoder(r.Body).Decode(&snapshot)
	if err != nil {
		return smartResponse(fmt.Errorf("Failed to parse request: %w", err))
	}

	client, err := getDevLXDVsockClient(d)
	if err != nil {
		return smartResponse(err)
	}

	client = client.UseTarget(r.URL.Query().Get("target"))
	defer client.Disconnect()

	op, err := client.CreateStoragePoolVolumeSnapshot(poolName, volType, volName, snapshot)
	if err != nil {
		return smartResponse(err)
	}

	return okResponse(op.Get(), "json")
}

var devLXDStoragePoolVolumeSnapshotEndpoint = devLXDAPIEndpoint{
	Path: "storage-pools/{pool}/volumes/{type}/{volume}/snapshots/{snapshot}",
	Get:  devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumeSnapshotGetHandler},
}

func devLXDStoragePoolVolumeSnapshotGetHandler(d *Daemon, r *http.Request) *devLXDResponse {
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

	snapshotName, err := url.PathUnescape(mux.Vars(r)["snapshot"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	client, err := getDevLXDVsockClient(d)
	if err != nil {
		return smartResponse(err)
	}

	snapshot, etag, err := client.GetStoragePoolVolumeSnapshot(poolName, volType, volName, snapshotName)
	if err != nil {
		return smartResponse(err)
	}

	return okResponseETag(snapshot, "json", etag)
}
