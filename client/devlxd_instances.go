package lxd

import (
	"net/http"

	"github.com/canonical/lxd/lxd/device/config"
	"github.com/canonical/lxd/shared/api"
)

// GetInstanceDevices retrieves a map of instance devices.
func (r *ProtocolDevLXD) GetInstanceDevices(instName string) (devices map[string]config.Device, err error) {
	devices = make(map[string]config.Device)

	url := api.NewURL().Path("instances", instName, "devices").URL
	_, err = r.queryStruct(http.MethodGet, url.String(), nil, "", &devices)
	if err != nil {
		return nil, err
	}

	return devices, nil
}

// AttachInstanceDevice attaches a new device to the instance.
func (r *ProtocolDevLXD) GetInstanceDevice(instName string, deviceName string) (config.Device, error) {
	device := config.Device{}

	url := api.NewURL().Path("instances", instName, "devices", deviceName).URL
	_, err := r.queryStruct(http.MethodGet, url.String(), nil, "", device)
	if err != nil {
		return nil, err
	}

	return device, nil
}

// AttachInstanceDevice attaches a new device to the instance.
func (r *ProtocolDevLXD) CreateInstanceDevice(instName string, device config.Device) error {
	url := api.NewURL().Path("instances", instName, "devices").URL
	_, _, err := r.query(http.MethodPost, url.String(), device, "")
	if err != nil {
		return err
	}

	return nil
}

// AttachInstanceDevice attaches a new device to the instance.
func (r *ProtocolDevLXD) DeleteInstanceDevice(instName string, deviceName string) error {
	url := api.NewURL().Path("instances", instName, "devices", deviceName).URL
	_, _, err := r.query(http.MethodDelete, url.String(), nil, "")
	if err != nil {
		return err
	}

	return nil
}
