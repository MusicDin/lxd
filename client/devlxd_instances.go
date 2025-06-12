package lxd

import (
	"net/http"

	"github.com/canonical/lxd/shared/api"
)

// GetInstanceDevices retrieves a map of instance devices.
func (r *ProtocolDevLXD) GetInstanceDevices(instName string) (devices map[string]map[string]string, err error) {
	devices = make(map[string]map[string]string)

	url := api.NewURL().Path("instances", instName, "devices").URL
	_, err = r.queryStruct(http.MethodGet, url.String(), nil, "", &devices)
	if err != nil {
		return nil, err
	}

	return devices, nil
}

// GetInstanceDevice retrieves a specific instance device.
// It also returns the ETag of an instance, which is used when updating the instance devices.
func (r *ProtocolDevLXD) GetInstanceDevice(instName string, deviceName string) (device map[string]string, etag string, err error) {
	device = make(map[string]string)

	url := api.NewURL().Path("instances", instName, "devices", deviceName).URL
	etag, err = r.queryStruct(http.MethodGet, url.String(), nil, "", &device)
	if err != nil {
		return nil, "", err
	}

	return device, etag, nil
}

// CreateInstanceDevice attaches a new device to the instance.
func (r *ProtocolDevLXD) CreateInstanceDevice(instName string, device map[string]string) error {
	url := api.NewURL().Path("instances", instName, "devices").URL
	_, _, err := r.query(http.MethodPost, url.String(), device, "")
	if err != nil {
		return err
	}

	return nil
}
