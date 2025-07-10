package main

import (
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/canonical/lxd/shared/api"
	"github.com/gorilla/mux"

	"github.com/canonical/lxd/shared/api"
)

var devLXDInstanceEndpoint = devLXDAPIEndpoint{
<<<<<<< HEAD
	Path:  "instances/{name}",
	Get:   devLXDAPIEndpointAction{Handler: devLXDInstanceGetHandler},
	Patch: devLXDAPIEndpointAction{Handler: devLXDInstancePatchHandler},
=======
	Path: "instances/{name}",
	Get:  devLXDAPIEndpointAction{Handler: devLXDInstanceGetHandler},
	Put:  devLXDAPIEndpointAction{Handler: devLXDInstancePutHandler},
>>>>>>> c1141e2b13 (lxd-agent/devlxd: Update instance endpoint)
}

func devLXDInstanceGetHandler(d *Daemon, r *http.Request) *devLXDResponse {
	instName, err := url.PathUnescape(mux.Vars(r)["name"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	client, err := getDevLXDVsockClient(d, r)
	if err != nil {
		return smartResponse(err)
	}

	defer client.Disconnect()

	inst, etag, err := client.GetInstance(instName)
	if err != nil {
		return smartResponse(err)
	}

	return okResponseETag(inst, "json", etag)
}

func devLXDInstancePatchHandler(d *Daemon, r *http.Request) *devLXDResponse {
	instName, err := url.PathUnescape(mux.Vars(r)["name"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	var inst api.DevLXDInstancePut
	err = json.NewDecoder(r.Body).Decode(&inst)
	if err != nil {
		return smartResponse(err)
	}

	client, err := getDevLXDVsockClient(d, r)
	if err != nil {
		return smartResponse(err)
	}

	defer client.Disconnect()

	etag := r.Header.Get("If-Match")
	err = client.UpdateInstance(instName, inst, etag)
	if err != nil {
		return smartResponse(err)
	}

	return okResponse("", "raw")
}
