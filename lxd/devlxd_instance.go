package main

import (
	"net/http"

	"github.com/gorilla/mux"

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
