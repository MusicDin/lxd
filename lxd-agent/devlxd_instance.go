package main

import (
	"net/http"
	"net/url"

	"github.com/gorilla/mux"
)

var devLXDInstanceDevicesEndpoint = devLXDAPIEndpoint{
	Path: "instances/{instanceName}/devices",
	Get:  devLXDAPIEndpointAction{Handler: devLXDInstanceDevicesGetHandler},
}

func devLXDInstanceDevicesGetHandler(d *Daemon, r *http.Request) *devLXDResponse {
	instName, err := url.PathUnescape(mux.Vars(r)["instanceName"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	client, err := getDevLXDVsockClient(d)
	if err != nil {
		return smartResponse(err)
	}

	defer client.Disconnect()

	devices, err := client.GetInstanceDevices(instName)
	if err != nil {
		return smartResponse(err)
	}

	return okResponse(devices, "json")
}
