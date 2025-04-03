package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/canonical/lxd/lxd/device/config"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/logger"
	"github.com/gorilla/mux"
)

var devLXDInstanceDevicesEndpoint = DevLXDAPIEndpoint{
	Path: "instances/{instanceName}/devices",
	Get:  DevLXDAPIEndpointAction{Handler: devLXDInstanceDevicesGetHandler},
	Post: DevLXDAPIEndpointAction{Handler: devLXDInstanceDevicesPostHandler},
}

func devLXDInstanceDevicesGetHandler(d *Daemon, w http.ResponseWriter, r *http.Request) *devLXDResponse {
	client, err := getVsockClient(d)
	if err != nil {
		return smartResponse(err)
	}

	defer client.Disconnect()

	instName := mux.Vars(r)["instanceName"]

	url := api.NewURL().Path("1.0", "instances", instName, "devices")
	resp, _, err := client.RawQuery(http.MethodGet, url.String(), nil, "")
	if err != nil {
		return smartResponse(err)
	}

	var devices map[string]map[string]string

	err = resp.MetadataAsStruct(&devices)
	if err != nil {
		return smartResponse(fmt.Errorf("Failed parsing response from LXD: %w", err))
	}

	return okResponse(devices, "json")
}

func devLXDInstanceDevicesPostHandler(d *Daemon, w http.ResponseWriter, r *http.Request) *devLXDResponse {
	client, err := getVsockClient(d)
	if err != nil {
		return smartResponse(err)
	}

	defer client.Disconnect()

	instName := mux.Vars(r)["instanceName"]

	var device map[string]string

	err = json.NewDecoder(r.Body).Decode(&device)
	if err != nil {
		return smartResponse(err)
	}

	logger.Warn("DeviceAttachement decoded request", logger.Ctx{"request": device})

	url := api.NewURL().Path("1.0", "instances", instName, "devices")
	_, _, err = client.RawQuery(http.MethodPost, url.String(), device, "")
	if err != nil {
		return smartResponse(fmt.Errorf("Failed to attach device: %w", err))
	}

	return okResponse(nil, "raw")
}

var devLXDInstanceDeviceEndpoint = DevLXDAPIEndpoint{
	Path:   "instances/{instanceName}/devices/{devName}",
	Get:    DevLXDAPIEndpointAction{Handler: devLXDInstanceDeviceGetHandler},
	Delete: DevLXDAPIEndpointAction{Handler: devLXDInstanceDeviceDeleteHandler},
}

func devLXDInstanceDeviceGetHandler(d *Daemon, w http.ResponseWriter, r *http.Request) *devLXDResponse {
	client, err := getVsockClient(d)
	if err != nil {
		return smartResponse(err)
	}

	defer client.Disconnect()

	instName := mux.Vars(r)["instanceName"]
	devName := mux.Vars(r)["devName"]

	url := api.NewURL().Path("1.0", "instances", instName, "devices", devName)
	resp, _, err := client.RawQuery(http.MethodGet, url.String(), nil, "")
	if err != nil {
		return smartResponse(err)
	}

	// var device map[string]string
	var device config.Device

	err = resp.MetadataAsStruct(&device)
	if err != nil {
		return smartResponse(fmt.Errorf("Failed parsing response from LXD: %w", err))
	}

	return okResponse(device, "json")
}

func devLXDInstanceDeviceDeleteHandler(d *Daemon, w http.ResponseWriter, r *http.Request) *devLXDResponse {
	client, err := getVsockClient(d)
	if err != nil {
		return smartResponse(err)
	}

	defer client.Disconnect()

	instName := mux.Vars(r)["instanceName"]
	devName := mux.Vars(r)["devName"]

	url := api.NewURL().Path("1.0", "instances", instName, "devices", devName)
	_, _, err = client.RawQuery(http.MethodDelete, url.String(), nil, "")
	if err != nil {
		return smartResponse(err)
	}

	return okResponse(nil, "raw")
}
