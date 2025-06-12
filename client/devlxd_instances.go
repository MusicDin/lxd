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
