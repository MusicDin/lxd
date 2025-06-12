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
