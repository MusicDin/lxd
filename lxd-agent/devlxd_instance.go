package main

import (
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/gorilla/mux"
)

var devLXDInstanceDevicesEndpoint = devLXDAPIEndpoint{
	Path: "instances/{instanceName}/devices",
	Get:  devLXDAPIEndpointAction{Handler: devLXDInstanceDevicesGetHandler},
	Post: devLXDAPIEndpointAction{Handler: devLXDInstanceDevicesPostHandler},
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

func devLXDInstanceDevicesPostHandler(d *Daemon, r *http.Request) *devLXDResponse {
	instName, err := url.PathUnescape(mux.Vars(r)["instanceName"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	var device map[string]string
	err = json.NewDecoder(r.Body).Decode(&device)
	if err != nil {
		return smartResponse(err)
	}

	client, err := getDevLXDVsockClient(d)
	if err != nil {
		return smartResponse(err)
	}

	defer client.Disconnect()

	err = client.CreateInstanceDevice(instName, device)
	if err != nil {
		return smartResponse(err)
	}

	return okResponse("", "raw")
}

var devLXDInstanceDeviceEndpoint = devLXDAPIEndpoint{
	Path: "instances/{instanceName}/devices/{devName}",
	Get:  devLXDAPIEndpointAction{Handler: devLXDInstanceDeviceGetHandler},
}

func devLXDInstanceDeviceGetHandler(d *Daemon, r *http.Request) *devLXDResponse {
	instName, err := url.PathUnescape(mux.Vars(r)["instanceName"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	devName, err := url.PathUnescape(mux.Vars(r)["devName"])
	if err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	client, err := getDevLXDVsockClient(d)
	if err != nil {
		return smartResponse(err)
	}

	defer client.Disconnect()

	device, etag, err := client.GetInstanceDevice(instName, devName)
	if err != nil {
		return smartResponse(err)
	}

	return okResponseETag(device, "json", etag)
}
