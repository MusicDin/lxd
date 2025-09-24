package storage

import (
	"fmt"

	backupConfig "github.com/canonical/lxd/lxd/backup/config"
	"github.com/canonical/lxd/lxd/operations"
	"github.com/canonical/lxd/lxd/state"
)

// storageCache is used to cache pools and volumes.
type storageCache struct {
	pools map[string]Pool
	// The volume cache is using the pool as its first dimension.
	// By default all projects use features.storage.volumes=true which uses the volumes from the individual project.
	// In this case the top level dimension only has an entry for the pool(s) which causes the cache to stay small.
	// Having the project as the top level dimension would cause a duplicated pool entry under every project from which volumes are consumed:
	// {
	//  "pool1": {
	//   "default": {<vols>},
	//   "project1": {<vols>},
	//   ...
	//  },
	//  ...
	// }
	volumes map[string]map[string]map[string]*backupConfig.Volume
	state   *state.State
}

// NewStorageCache returns a new instance of the storage cache.
func NewStorageCache(backend *lxdBackend) *storageCache {
	return &storageCache{
		pools: map[string]Pool{
			// Initialize the cache with the already existing backend's pool.
			backend.name: backend,
		},
		volumes: map[string]map[string]map[string]*backupConfig.Volume{},
		state:   backend.state,
	}
}

// getPool returns the pool either by loading it from the DB or from the cache (preferred).
func (s *storageCache) getPool(name string) (Pool, error) {
	// Load the pool if it cannot be found.
	_, ok := s.pools[name]
	if !ok {
		var err error

		// Custom volume's pool is not yet in the cache, load it.
		pool, err := LoadByName(s.state, name)
		if err != nil {
			return nil, err
		}

		// Cache the pool.
		s.pools[name] = pool
	}

	return s.pools[name], nil
}

// getVolume returns the volume's backup config either by loading it from the DB or from the cache (preferred).
// If snapshots is true the volume's snapshots are included in the returned backup config.
func (s *storageCache) getVolume(projectName string, poolName string, volName string, snapshots bool, op *operations.Operation) (*backupConfig.Volume, error) {
	// Create pool cache.
	_, ok := s.volumes[poolName]
	if !ok {
		s.volumes[poolName] = map[string]map[string]*backupConfig.Volume{}
	}

	// Create project cache.
	_, ok = s.volumes[poolName][projectName]
	if !ok {
		s.volumes[poolName][projectName] = map[string]*backupConfig.Volume{}
	}

	_, ok = s.volumes[poolName][projectName][volName]
	if !ok {
		pool, err := s.getPool(poolName)
		if err != nil {
			return nil, fmt.Errorf("Failed to retrieve pool of volume %q in pool %q: %w", volName, poolName, err)
		}

		volConfig, err := pool.GenerateCustomVolumeBackupConfig(projectName, volName, snapshots, op)
		if err != nil {
			return nil, fmt.Errorf("Failed generating backup config of volume %q in pool %q and project %q: %w", volName, poolName, projectName, err)
		}

		vol, err := volConfig.CustomVolume()
		if err != nil {
			return nil, fmt.Errorf("Failed getting the custom volume: %w", err)
		}

		// Cache the volume.
		s.volumes[poolName][projectName][volName] = vol
	}

	return s.volumes[poolName][projectName][volName], nil
}
