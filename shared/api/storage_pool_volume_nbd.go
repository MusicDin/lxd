package api

// StorageVolumeNBDPost represents the fields available for an NBD export of a volume
//
// swagger:model
//
// API extension: storage_volume_block_tracking.
type StorageVolumeNBDPost struct {
	// Whether the volume is exported read-write, for a restore, instead of read-only
	// Example: true
	Writable bool `json:"writable" yaml:"writable"`
}
