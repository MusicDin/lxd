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

// GetStoragePool retrieves the storage pool with a given name.
func (r *ProtocolDevLXD) GetStoragePool(poolName string) (*api.DevLXDStoragePool, string, error) {
	var pool api.DevLXDStoragePool

	url := api.NewURL().Path("storage-pools", poolName).URL
	etag, err := r.queryStruct(http.MethodGet, url.String(), nil, "", &pool)
	if err != nil {
		return nil, "", err
	}

	return &pool, etag, nil
}
