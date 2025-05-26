package main

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/canonical/lxd/lxd/response"
	"github.com/canonical/lxd/lxd/util"
	"github.com/canonical/lxd/shared/api"
)

var devLXDStoragePoolEndpoint = devLXDAPIEndpoint{
	Path: "storage-pools/{poolName}",
	Get:  devLXDAPIEndpointAction{Handler: devLXDStoragePoolGetHandler, AllowUntrusted: true},
}

func devLXDStoragePoolGetHandler(d *Daemon, r *http.Request) response.Response {
	inst, err := getInstanceFromContextAndCheckSecurityFlags(r.Context(), devLXDSecurityKey, devLXDSecurityMgmtVolumesKey)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	// Get storage pool.
	poolName := mux.Vars(r)["poolName"]
	projectName := inst.Project().Name
	pool := api.StoragePool{}

	url := api.NewURL().Path("1.0", "storage-pools", poolName).Project(projectName)
	req, err := NewRequestWithContext(r.Context(), http.MethodGet, url.String(), nil, "")
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	resp := storagePoolGet(d, req)
	etag, err := RenderToStruct(req, resp, &pool)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	// Map to devLXD response.
	respPool := api.DevLXDStoragePool{
		Name:   pool.Name,
		Driver: pool.Driver,
		Status: pool.Status,
	}

	return response.DevLXDResponseETag(http.StatusOK, respPool, "json", etag)
}

var devLXDStoragePoolVolumesTypeEndpoint = devLXDAPIEndpoint{
	Path: "storage-pools/{poolName}/volumes/{type}",
	Get:  devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumesGetHandler, AllowUntrusted: true},
	Post: devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumesPostHandler, AllowUntrusted: true},
}

func devLXDStoragePoolVolumesGetHandler(d *Daemon, r *http.Request) response.Response {
	inst, err := getInstanceFromContextAndCheckSecurityFlags(r.Context(), devLXDSecurityKey, devLXDSecurityMgmtVolumesKey)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	poolName := mux.Vars(r)["poolName"]
	volType := mux.Vars(r)["type"]
	projectName := inst.Project().Name

	// Reject non-recursive requests.
	if !util.IsRecursionRequest(r) {
		return response.DevLXDErrorResponse(api.NewStatusError(http.StatusNotImplemented, "Only recursive requests are currently supported"))
	}

	// Reject non-custom volume types, if the type is specified.
	if volType != "custom" {
		return response.DevLXDErrorResponse(api.NewStatusError(http.StatusBadRequest, "Only custom storage volumes can be retrieved"))
	}

	// Get storage volumes.
	vols := []api.StorageVolume{}

	url := api.NewURL().Path("1.0", "storage-pools", poolName, "volumes", volType).Project(projectName).WithQuery("recursion", "1")
	target := r.URL.Query().Get("target")
	if target != "" {
		url = url.WithQuery("target", target)
	}

	// Ensure only custom volumes are returned, if the volume type is not provided.
	if volType == "" {
		url = url.WithQuery("filter", "type eq custom")
	}

	req, err := NewRequestWithContext(r.Context(), http.MethodGet, url.String(), nil, "")
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	resp := storagePoolVolumesGet(d, req)
	_, err = RenderToStruct(req, resp, &vols)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	respVols := make([]api.DevLXDStorageVolume, len(vols))
	for i, vol := range vols {
		respVols[i] = api.DevLXDStorageVolume{
			Name:        vol.Name,
			Description: vol.Description,
			Pool:        vol.Pool,
			Type:        vol.Type,
			Config:      vol.Config,
			Location:    vol.Location,
		}
	}

	return response.DevLXDResponse(http.StatusOK, respVols, "json")
}

func devLXDStoragePoolVolumesPostHandler(d *Daemon, r *http.Request) response.Response {
	inst, err := getInstanceFromContextAndCheckSecurityFlags(r.Context(), devLXDSecurityKey, devLXDSecurityMgmtVolumesKey)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	poolName := mux.Vars(r)["poolName"]
	volType := mux.Vars(r)["type"]
	projectName := inst.Project().Name

	// Decode the request body.
	vol := api.DevLXDStorageVolumesPost{}
	err = json.NewDecoder(r.Body).Decode(&vol)
	if err != nil {
		return response.DevLXDErrorResponse(api.StatusErrorf(http.StatusInternalServerError, "Failed decoding request body: %w", err))
	}

	// Reject non-custom volume type.
	if volType != "custom" {
		return response.DevLXDErrorResponse(api.NewStatusError(http.StatusBadRequest, "Only custom storage volumes can be created"))
	}

	if vol.Type != "" && vol.Type != volType {
		return response.DevLXDErrorResponse(api.NewStatusError(http.StatusBadRequest, "URL volume type does not match the volume type in body"))
	}

	// Create storage volume.
	reqBody := api.StorageVolumesPost{
		Name:        vol.Name,
		Type:        volType,
		Source:      vol.Source,
		ContentType: vol.ContentType,
		StorageVolumePut: api.StorageVolumePut{
			Config:      vol.Config,
			Description: vol.Description,
			Restore:     vol.Restore,
		},
	}

	url := api.NewURL().Path("1.0", "storage-pools", poolName, "volumes", volType).Project(projectName).WithQuery("recursion", "1")
	target := r.URL.Query().Get("target")
	if target != "" {
		url = url.WithQuery("target", target)
	}

	req, err := NewRequestWithContext(r.Context(), http.MethodPost, url.String(), reqBody, "")
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	resp := storagePoolVolumesPost(d, req)
	err = Render(req, resp)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	return response.DevLXDResponse(http.StatusOK, "", "raw")
}

var devLXDStoragePoolVolumeTypeEndpoint = devLXDAPIEndpoint{
	Path: "storage-pools/{poolName}/volumes/{type}/{volumeName}",
	Get:  devLXDAPIEndpointAction{Handler: devLXDStoragePoolVolumeGetHandler, AllowUntrusted: true},
}

func devLXDStoragePoolVolumeGetHandler(d *Daemon, r *http.Request) response.Response {
	inst, err := getInstanceFromContextAndCheckSecurityFlags(r.Context(), devLXDSecurityKey, devLXDSecurityMgmtVolumesKey)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	poolName := mux.Vars(r)["poolName"]
	volName := mux.Vars(r)["volumeName"]
	volType := mux.Vars(r)["type"]
	projectName := inst.Project().Name

	// Restrict access to custom volumes.
	if volType != "custom" {
		return response.DevLXDErrorResponse(api.NewStatusError(http.StatusBadRequest, "Only custom storage volumes can be retrieved"))
	}

	// Get storage volumes.
	vol := api.StorageVolume{}

	url := api.NewURL().Path("1.0", "storage-pools", poolName, "volumes", "custom", volName).Project(projectName)
	target := r.URL.Query().Get("target")
	if target != "" {
		url = url.WithQuery("target", target)
	}

	req, err := NewRequestWithContext(r.Context(), http.MethodGet, url.String(), nil, "")
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	err = addStoragePoolVolumeDetailsToRequestContext(d.State(), req)
	if err != nil {
		response.DevLXDErrorResponse(err)
	}

	resp := storagePoolVolumeGet(d, req)
	etag, err := RenderToStruct(req, resp, &vol)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	respVol := api.DevLXDStorageVolume{
		Name:        vol.Name,
		Description: vol.Description,
		Pool:        vol.Pool,
		Type:        vol.Type,
		Config:      vol.Config,
		Location:    vol.Location,
	}

	return response.DevLXDResponseETag(http.StatusOK, respVol, "json", etag)
}
