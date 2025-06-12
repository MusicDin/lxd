package lxd

import (
	"net/http"

	"github.com/canonical/lxd/shared/api"
)

// GetInstance retrieves the instance with the given name.
func (r *ProtocolDevLXD) GetInstance(instName string) (instance *api.DevLXDInstance, etag string, err error) {
	var inst api.DevLXDInstance

	url := api.NewURL().Path("instances", instName)
	etag, err = r.queryStruct(http.MethodGet, url.String(), nil, "", &inst)
	if err != nil {
		return nil, "", err
	}

	return &inst, etag, nil
}

// UpdateInstance updates an existing instance with the given name.
func (r *ProtocolDevLXD) UpdateInstance(instName string, inst api.DevLXDInstance, ETag string) (Operation, error) {
	url := api.NewURL().Path("instances", instName)
	op, _, err := r.queryOperation(http.MethodPut, url.String(), inst, ETag)
	if err != nil {
		return nil, err
	}

	return op, nil
}
