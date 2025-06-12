package lxd

import (
	"net/http"

	"github.com/canonical/lxd/shared/api"
)

// GetStoragePools retrieves the list of storage pools.
func (r *ProtocolDevLXD) GetStoragePools() ([]api.DevLXDStoragePool, error) {
	var pools []api.DevLXDStoragePool

	url := api.NewURL().Path("storage-pools").WithQuery("recursion", "1").URL
	_, err := r.queryStruct(http.MethodGet, url.String(), nil, "", &pools)
	if err != nil {
		return nil, err
	}

	return pools, nil
}
