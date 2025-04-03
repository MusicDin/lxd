package lxd

import (
	"fmt"
	"net/http"

	"github.com/canonical/lxd/shared/api"
)

// GetStoragePools retrieves the list of storage pools.
func (r *ProtocolDevLXD) GetStoragePools() ([]api.StoragePool, error) {
	var pools []api.StoragePool

	url := api.NewURL().Path("storage-pools").URL
	_, err := r.queryStruct(http.MethodGet, url.String(), nil, "", &pools)
	if err != nil {
		return nil, err
	}

	return pools, nil
}

// GetStoragePool retrieves the storage pool with a given name.
func (r *ProtocolDevLXD) GetStoragePool(poolName string) (*api.StoragePool, string, error) {
	var pool api.StoragePool

	url := api.NewURL().Path("storage-pools", poolName).URL
	etag, err := r.queryStruct(http.MethodGet, url.String(), nil, "", &pool)
	if err != nil {
		return nil, "", err
	}

	return &pool, etag, nil
}

// GetStoragePoolVolumes retrieves the storage volumes for a given storage pool name.
func (r *ProtocolDevLXD) GetStoragePoolVolumes(poolName string) ([]api.StorageVolume, error) {
	var vols []api.StorageVolume

	url := api.NewURL().Path("storage-pools", poolName, "volumes").URL
	_, err := r.queryStruct(http.MethodGet, url.String(), nil, "", &vols)
	if err != nil {
		return nil, err
	}

	return vols, nil
}

// GetStoragePoolVolume retrieves the storage volume with a given name.
func (r *ProtocolDevLXD) GetStoragePoolVolume(poolName string, volType string, volName string) (*api.StorageVolume, string, error) {
	var vol api.StorageVolume

	url := api.NewURL().Path("storage-pools", poolName, "volumes", volType, volName).URL
	etag, err := r.queryStruct(http.MethodGet, url.String(), nil, "", &vol)
	if err != nil {
		return nil, "", err
	}

	return &vol, etag, nil
}

// CreateStoragePoolVolume creates a new storage volume in a given storage pool.
func (r *ProtocolDevLXD) CreateStoragePoolVolume(poolName string, vol api.StorageVolumesPost) error {
	fmt.Printf("CreateStoragePoolVolume: %+v\n", vol)
	url := api.NewURL().Path("storage-pools", poolName, "volumes", vol.Type).URL
	_, _, err := r.query(http.MethodPost, url.String(), vol, "")
	if err != nil {
		return err
	}

	return nil
}

// DeleteStoragePoolVolume deletes a storage volume from a given storage pool.
func (r *ProtocolDevLXD) DeleteStoragePoolVolume(poolName string, volType string, volName string) error {
	url := api.NewURL().Path("storage-pools", poolName, "volumes", volType, volName).URL
	_, _, err := r.query(http.MethodDelete, url.String(), nil, "")
	if err != nil {
		return err
	}

	return nil
}
