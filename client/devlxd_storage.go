package lxd

import (
	"fmt"
	"net/http"

	"github.com/canonical/lxd/shared/api"
)

func (r *ProtocolDevLXD) GetStoragePools() ([]api.StoragePool, error) {
	var pools []api.StoragePool

	url := api.NewURL().Path("storage-pools").URL
	_, err := r.queryStruct(http.MethodGet, url.String(), nil, "", &pools)
	if err != nil {
		return nil, err
	}

	return pools, nil
}

func (r *ProtocolDevLXD) GetStoragePool(poolName string) (*api.StoragePool, string, error) {
	var pool api.StoragePool

	url := api.NewURL().Path("storage-pools", poolName).URL
	etag, err := r.queryStruct(http.MethodGet, url.String(), nil, "", &pool)
	if err != nil {
		return nil, "", err
	}

	return &pool, etag, nil
}

func (r *ProtocolDevLXD) GetStoragePoolVolumes(poolName string) ([]api.StorageVolume, error) {
	var vols []api.StorageVolume

	url := api.NewURL().Path("storage-pools", poolName, "volumes").URL
	_, err := r.queryStruct(http.MethodGet, url.String(), nil, "", &vols)
	if err != nil {
		return nil, err
	}

	return vols, nil
}

func (r *ProtocolDevLXD) GetStoragePoolVolume(poolName string, volType string, volName string) (*api.StorageVolume, string, error) {
	var vol api.StorageVolume

	url := api.NewURL().Path("storage-pools", poolName, "volumes", volType, volName).URL
	etag, err := r.queryStruct(http.MethodGet, url.String(), nil, "", &vol)
	if err != nil {
		return nil, "", err
	}

	return &vol, etag, nil
}

// GetMetadata retrieves the instance's meta-data.
func (r *ProtocolDevLXD) CreateStoragePoolVolume(poolName string, vol api.StorageVolumesPost) error {
	fmt.Printf("CreateStoragePoolVolume: %+v\n", vol)
	url := api.NewURL().Path("storage-pools", poolName, "volumes", vol.Type).URL
	_, _, err := r.query(http.MethodPost, url.String(), vol, "")
	if err != nil {
		return err
	}

	return nil
}

// GetMetadata retrieves the instance's meta-data.
func (r *ProtocolDevLXD) DeleteStoragePoolVolume(poolName string, volType string, volName string) error {
	url := api.NewURL().Path("storage-pools", poolName, "volumes", volType, volName).URL
	_, _, err := r.query(http.MethodDelete, url.String(), nil, "")
	if err != nil {
		return err
	}

	return nil
}
