package drivers

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"

	"github.com/canonical/lxd/lxd/operations"
	"github.com/canonical/lxd/lxd/storage/block"
	"github.com/canonical/lxd/lxd/storage/filesystem"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/logger"
	"github.com/canonical/lxd/shared/revert"
	"github.com/canonical/lxd/shared/units"
	"github.com/canonical/lxd/shared/validate"
)

const (
	powerStoreVolPrefixSep       = "_" // volume name prefix separator
	powerStoreContainerVolPrefix = "c" // volume name prefix indicating container volume
	powerStoreVMVolPrefix        = "v" // volume name prefix indicating virtual machine volume
	powerStoreImageVolPrefix     = "i" // volume name prefix indicating image volume
	powerStoreCustomVolPrefix    = "u" // volume name prefix indicating custom volume
)

// powerStoreVolTypePrefixes maps volume type to storage volume name prefix.
var powerStoreVolTypePrefixes = map[VolumeType]string{
	VolumeTypeContainer: powerStoreContainerVolPrefix,
	VolumeTypeVM:        powerStoreVMVolPrefix,
	VolumeTypeImage:     powerStoreImageVolPrefix,
	VolumeTypeCustom:    powerStoreCustomVolPrefix,
}

// powerStoreVolTypePrefixesRev maps storage volume name prefix to volume type.
var powerStoreVolTypePrefixesRev = map[string]VolumeType{
	powerStoreContainerVolPrefix: VolumeTypeContainer,
	powerStoreVMVolPrefix:        VolumeTypeVM,
	powerStoreImageVolPrefix:     VolumeTypeImage,
	powerStoreCustomVolPrefix:    VolumeTypeCustom,
}

const (
	powerStoreVolSuffixSep   = "." // volume name suffix separator
	powerStoreBlockVolSuffix = "b" // volume name suffix used for block content type volumes
	powerStoreISOVolSuffix   = "i" // volume name suffix used for iso content type volumes
)

// powerStoreVolContentTypeSuffixes maps volume content type to storage volume name suffix.
var powerStoreVolContentTypeSuffixes = map[ContentType]string{
	ContentTypeBlock: powerStoreBlockVolSuffix,
	ContentTypeISO:   powerStoreISOVolSuffix,
}

// powerStoreVolContentTypeSuffixesRev maps storage volume name suffix to volume content type.
var powerStoreVolContentTypeSuffixesRev = map[string]ContentType{
	powerStoreBlockVolSuffix: ContentTypeBlock,
	powerStoreISOVolSuffix:   ContentTypeISO,
}

// powerStoreResourceNamePrefix common prefix for all resource names in PowerStore.
const powerStoreResourceNamePrefix = "lxd:"

// powerStorePoolAndVolSep separates pool name and volume data in encoded volume names.
const powerStorePoolAndVolSep = "-"

// volumeResourceNamePrefix returns the prefix used by all volume resource
// names in PowerStore associated with the current storage pool.
func (d *powerstore) volumeResourceNamePrefix() string {
	poolHash := sha256.Sum256([]byte(d.Name()))
	poolName := base64.StdEncoding.EncodeToString(poolHash[:])
	return powerStoreResourceNamePrefix + poolName + powerStorePoolAndVolSep
}

// volumeResourceName derives the name of a volume resource in PowerStore from
// the provided volume.
func (d *powerstore) volumeResourceName(vol Volume) (string, error) {
	volUUID, err := uuid.Parse(vol.config["volatile.uuid"])
	if err != nil {
		return "", fmt.Errorf(`Failed parsing "volatile.uuid" from volume %q: %w`, vol.name, err)
	}

	volName := base64.StdEncoding.EncodeToString(volUUID[:])

	// Search for the volume type prefix, and if found, prepend it to the volume
	// name.
	prefix := powerStoreVolTypePrefixes[vol.volType]
	if prefix != "" {
		volName = prefix + powerStoreVolPrefixSep + volName
	}

	// Search for the content type suffix, and if found, append it to the volume
	// name.
	suffix := powerStoreVolContentTypeSuffixes[vol.contentType]
	if suffix != "" {
		volName = volName + powerStoreVolSuffixSep + suffix
	}

	return d.volumeResourceNamePrefix() + volName, nil
}

// extractDataFromVolumeResourceName decodes the PowerStore volume resource
// name and extracts the stored data.
func (d *powerstore) extractDataFromVolumeResourceName(name string) (poolHash string, volType VolumeType, volUUID uuid.UUID, volContentType ContentType, err error) {
	prefixLess, hasPrefix := strings.CutPrefix(name, powerStoreResourceNamePrefix)
	if !hasPrefix {
		return "", "", uuid.Nil, "", fmt.Errorf("Cannot decode volume name %q: invalid name format", name)
	}

	poolHash, volName, ok := strings.Cut(prefixLess, powerStorePoolAndVolSep)
	if !ok || poolHash == "" || volName == "" {
		return "", "", uuid.Nil, "", fmt.Errorf("Cannot decode volume name %q: invalid name format", name)
	}

	prefix, volNameWithoutPrefix, ok := strings.Cut(volName, powerStoreVolPrefixSep)
	if ok {
		volName = volNameWithoutPrefix
		volType = powerStoreVolTypePrefixesRev[prefix]
	}

	volNameWithoutSuffix, suffix, ok := strings.Cut(volName, powerStoreVolSuffixSep)
	if ok {
		volName = volNameWithoutSuffix
		volContentType = powerStoreVolContentTypeSuffixesRev[suffix]
	}

	binUUID, err := base64.StdEncoding.DecodeString(volName)
	if err != nil {
		return poolHash, volType, volUUID, volContentType, fmt.Errorf("Cannot decode volume name %q: %w", name, err)
	}

	volUUID, err = uuid.FromBytes(binUUID)
	if err != nil {
		return poolHash, volType, volUUID, volContentType, fmt.Errorf("Failed parsing UUID from decoded volume name: %w", err)
	}

	return poolHash, volType, volUUID, volContentType, nil
}

// volumeWWN derives the world wide name of a volume resource in PowerStore
// from the provided volume resource.
func (d *powerstore) volumeWWN(volResource *powerStoreVolumeResource) string {
	_, wwn, _ := strings.Cut(volResource.WWN, ".")
	return wwn
}

// roundVolumeBlockSizeBytes rounds the given size (in bytes) up to the next
// multiple of 1 MiB, which is the minimum volume size on PowerStore.
func (d *powerstore) roundVolumeBlockSizeBytes(_ Volume, sizeBytes int64) int64 {
	return roundAbove(powerStoreMinVolumeSizeBytes, sizeBytes)
}

// commonVolumeRules returns validation rules which are common for pool and
// volume.
func (d *powerstore) commonVolumeRules() map[string]func(value string) error {
	return map[string]func(value string) error{
		// lxdmeta:generate(entities=storage-powerstore; group=volume-conf; key=block.filesystem)
		// Valid options are: `btrfs`, `ext4`, `xfs`
		// If not set, `ext4` is assumed.
		// ---
		//  type: string
		//  condition: block-based volume with content type `filesystem`
		//  defaultdesc: same as `volume.block.filesystem`
		//  shortdesc: File system of the storage volume
		//  scope: global
		"block.filesystem": validate.Optional(validate.IsOneOf(blockBackedAllowedFilesystems...)),
		// lxdmeta:generate(entities=storage-powerstore; group=volume-conf; key=block.mount_options)
		//
		// ---
		//  type: string
		//  condition: block-based volume with content type `filesystem`
		//  defaultdesc: same as `volume.block.mount_options`
		//  shortdesc: Mount options for block-backed file system volumes
		//  scope: global
		"block.mount_options": validate.IsAny,
		// lxdmeta:generate(entities=storage-powerstore; group=volume-conf; key=size)
		// The size must be in multiples of 1 MiB. The minimum size is 1 MiB and maximum is 256 TiB.
		// ---
		//  type: string
		//  defaultdesc: same as `volume.size`
		//  shortdesc: Size/quota of the storage volume
		//  scope: global
		"size": validate.Optional(
			validate.IsNoLessThanUnit(powerStoreMinVolumeSizeUnit),
			validate.IsNoGreaterThanUnit(powerStoreMaxVolumeSizeUnit),
			validate.IsMultipleOfUnit(powerStoreMinVolumeSizeAlignmentUnit),
		),
	}
}

// FillVolumeConfig populate volume with default config.
func (d *powerstore) FillVolumeConfig(vol Volume) error {
	// Copy volume.* configuration options from pool.
	// Exclude 'block.filesystem' and 'block.mount_options'
	// as these ones are handled below in this function and depend on the volume's type.
	err := d.fillVolumeConfig(&vol, "block.filesystem", "block.mount_options")
	if err != nil {
		return err
	}

	// Only validate filesystem config keys for filesystem volumes or VM block
	// volumes (which have an associated filesystem volume).
	if vol.ContentType() == ContentTypeFS || vol.IsVMBlock() {
		// VM volumes will always use the default filesystem.
		if vol.IsVMBlock() {
			vol.config["block.filesystem"] = DefaultFilesystem
		} else {
			// Inherit filesystem from pool if not set.
			if vol.config["block.filesystem"] == "" {
				vol.config["block.filesystem"] = d.config["volume.block.filesystem"]
			}

			// Default filesystem if neither volume nor pool specify an override.
			if vol.config["block.filesystem"] == "" {
				// Unchangeable volume property: Set unconditionally.
				vol.config["block.filesystem"] = DefaultFilesystem
			}
		}

		// Inherit filesystem mount options from pool if not set.
		if vol.config["block.mount_options"] == "" {
			vol.config["block.mount_options"] = d.config["volume.block.mount_options"]
		}

		// Default filesystem mount options if neither volume nor pool specify an override.
		if vol.config["block.mount_options"] == "" {
			// Unchangeable volume property: Set unconditionally.
			vol.config["block.mount_options"] = "discard"
		}
	}

	return nil
}

// ValidateVolume validates the supplied volume config.
func (d *powerstore) ValidateVolume(vol Volume, removeUnknownKeys bool) error {
	if vol.ContentType() == ContentTypeISO {
		sizeBytes, err := units.ParseByteSizeString(vol.ConfigSize())
		if err != nil {
			return err
		}

		sizeBytes = d.roundVolumeBlockSizeBytes(vol, sizeBytes)
		vol.SetConfigSize(strconv.FormatInt(sizeBytes, 10))
	}

	commonRules := d.commonVolumeRules()

	// Disallow block.* settings for regular custom block volumes. These settings only make sense
	// when using custom filesystem volumes. LXD will create the filesystem
	// for these volumes, and use the mount options. When attaching a regular block volume to a VM,
	// these are not mounted by LXD and therefore don't need these config keys.
	if vol.volType == VolumeTypeCustom && vol.contentType == ContentTypeBlock {
		delete(commonRules, "block.filesystem")
		delete(commonRules, "block.mount_options")
	}

	return d.validateVolume(vol, commonRules, removeUnknownKeys)
}

// GetVolumeUsage returns the disk space used by the volume.
func (d *powerstore) GetVolumeUsage(vol Volume) (int64, error) {
	// If mounted, use the filesystem stats for pretty accurate usage information.
	if vol.contentType == ContentTypeFS && filesystem.IsMountPoint(vol.MountPath()) {
		var stat unix.Statfs_t
		err := unix.Statfs(vol.MountPath(), &stat)
		if err != nil {
			return -1, err
		}

		return int64(stat.Blocks-stat.Bfree) * int64(stat.Bsize), nil
	}

	volResource, err := d.getExistingVolumeResourceByVolume(vol)
	if err != nil {
		return -1, err
	}

	return volResource.LogicalUsed, nil
}

// SetVolumeQuota applies a size limit on volume.
func (d *powerstore) SetVolumeQuota(vol Volume, size string, allowUnsafeResize bool, op *operations.Operation) error {
	// Convert to bytes.
	sizeBytes, err := units.ParseByteSizeString(size)
	if err != nil {
		return err
	}

	// Do nothing if size isn't specified.
	if sizeBytes <= 0 {
		return nil
	}

	volResource, err := d.getExistingVolumeResourceByVolume(vol)
	if err != nil {
		return err
	}

	// Do nothing if volume is already specified size (+/- 512 bytes).
	if volResource.Size+512 > sizeBytes && volResource.Size-512 < sizeBytes {
		return nil
	}

	// PowerStore supports increasing of size only.
	if sizeBytes < volResource.Size {
		return errors.New("Volume capacity can only be increased")
	}

	// Validate the minimum size.
	err = validate.IsNoLessThanUnit(powerStoreMinVolumeSizeUnit)(size)
	if err != nil {
		return err
	}

	// Validate the maximum size.
	err = validate.IsNoGreaterThanUnit(powerStoreMaxVolumeSizeUnit)(size)
	if err != nil {
		return err
	}

	// Validate the alignment.
	err = validate.IsMultipleOfUnit(powerStoreMinVolumeSizeAlignmentUnit)(size)
	if err != nil {
		return err
	}

	// Resize filesystem if needed.
	if vol.contentType == ContentTypeFS {
		fsType := vol.ConfigBlockFilesystem()
		devPath, cleanup, err := d.getMappedDevicePath(vol, true)
		if err != nil {
			return err
		}

		// Resize block device.
		err = d.client().ResizeVolumeByID(d.state.ShutdownCtx, volResource.ID, sizeBytes)
		if err != nil {
			return err
		}

		defer cleanup()

		// Always wait for the disk to reflect the new size. In case SetVolumeQuota
		// is called on an already mapped volume, it might take some time until
		// the actual size of the device is reflected on the host. This is for
		// example the case when creating a volume and the filler performs a resize
		// in case the image exceeds the volume's size.
		err = block.WaitDiskDeviceResize(d.state.ShutdownCtx, devPath, sizeBytes)
		if err != nil {
			return fmt.Errorf("Failed waiting for volume %q to change its size: %w", vol.name, err)
		}

		// Grow the filesystem to fill block device.
		err = growFileSystem(fsType, devPath, vol)
		if err != nil {
			return err
		}

		return nil
	}

	// Only perform pre-resize checks if we are not in "unsafe" mode. In unsafe
	// mode we expect the caller to know what they are doing and understand
	// the risks.
	if !allowUnsafeResize && vol.MountInUse() {
		// We don't allow online resizing of block volumes.
		return ErrInUse
	}

	// Resize block device.
	err = d.client().ResizeVolumeByID(d.state.ShutdownCtx, volResource.ID, sizeBytes)
	if err != nil {
		return err
	}

	devPath, cleanup, err := d.getMappedDevicePath(vol, true)
	if err != nil {
		return err
	}

	defer cleanup()

	err = block.WaitDiskDeviceResize(d.state.ShutdownCtx, devPath, sizeBytes)
	if err != nil {
		return fmt.Errorf("Failed waiting for volume %q to change its size: %w", vol.name, err)
	}

	// Move the VM GPT alt header to end of disk if needed (not needed in unsafe
	// resize mode as it is expected the caller will do all necessary post resize
	// actions themselves).
	if vol.IsVMBlock() && !allowUnsafeResize {
		err = d.moveGPTAltHeader(devPath)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetVolumeDiskPath returns the location of a root disk block device.
func (d *powerstore) GetVolumeDiskPath(vol Volume) (string, error) {
	if vol.IsVMBlock() || (vol.volType == VolumeTypeCustom && IsContentBlock(vol.contentType)) {
		devPath, _, err := d.getMappedDevicePath(vol, false)
		return devPath, err
	}

	return "", ErrNotSupported
}

// CreateVolume creates an empty volume and can optionally fill it by executing
// the supplied filler function.
func (d *powerstore) CreateVolume(vol Volume, filler *VolumeFiller, op *operations.Operation) error {
	revert := revert.New()
	defer revert.Fail()

	volResource, err := d.createVolumeResource(vol)
	if err != nil {
		return err
	}

	revert.Add(func() { _ = d.deleteVolumeResource(volResource) })

	if vol.contentType == ContentTypeFS {
		cleanup, err := d.mapVolumeByVolumeResource(volResource)
		if err != nil {
			return err
		}

		revert.Add(cleanup)

		devPath, err := d.getMappedDevicePathByVolumeWWN(d.volumeWWN(volResource), true)
		if err != nil {
			return err
		}

		volumeFilesystem := vol.ConfigBlockFilesystem()
		_, err = makeFSType(devPath, volumeFilesystem, nil)
		if err != nil {
			return err
		}
	}

	// For VMs, also create the filesystem volume.
	if vol.IsVMBlock() {
		fsVol := vol.NewVMBlockFilesystemVolume()
		err := d.CreateVolume(fsVol, nil, op)
		if err != nil {
			return err
		}

		revert.Add(func() { _ = d.DeleteVolume(fsVol, op) })
	}

	mountTask := func(mountPath string, op *operations.Operation) error {
		// Run the volume filler function if supplied.
		if filler != nil && filler.Fill != nil {
			var err error
			var devPath string

			if IsContentBlock(vol.contentType) {
				// Get the device path.
				devPath, err = d.GetVolumeDiskPath(vol)
				if err != nil {
					return err
				}
			}

			// Run the filler.
			err = d.runFiller(vol, devPath, filler, true)
			if err != nil {
				return err
			}

			// Move the GPT alt header to end of disk if needed.
			if vol.IsVMBlock() {
				err = d.moveGPTAltHeader(devPath)
				if err != nil {
					return err
				}
			}
		}

		if vol.contentType == ContentTypeFS {
			// Run EnsureMountPath again after mounting and filling to ensure the mount
			// directory has the correct permissions set.
			err = vol.EnsureMountPath()
			if err != nil {
				return err
			}
		}

		return nil
	}

	err = vol.MountTask(mountTask, op)
	if err != nil {
		return err
	}

	revert.Success()
	return nil
}

// HasVolume indicates whether a specific volume exists on the storage pool.
func (d *powerstore) HasVolume(vol Volume) (bool, error) {
	volResource, err := d.getVolumeResourceByVolume(vol)
	if err != nil {
		return false, err
	}

	return volResource != nil, nil
}

// ListVolumes returns a list of LXD volumes in storage pool.
// It returns all volumes and sets the volume's volatile.uuid extracted from
// the name.
func (d *powerstore) ListVolumes() ([]Volume, error) {
	volResources, err := d.client().GetVolumes(d.state.ShutdownCtx)
	if err != nil {
		return nil, err
	}

	vols := make([]Volume, 0, len(volResources))
	for _, volResource := range volResources {
		_, volType, volUUID, volContentType, err := d.extractDataFromVolumeResourceName(volResource.Name)
		if err != nil {
			d.logger.Debug("Ignoring unrecognized volume", logger.Ctx{"name": volResource.Name, "err": err.Error()})
			continue
		}

		volConfig := map[string]string{
			"volatile.uuid": volUUID.String(),
		}

		vol := NewVolume(d, d.name, volType, volContentType, "", volConfig, d.config)
		if volContentType == ContentTypeFS {
			vol.SetMountFilesystemProbe(true)
		}

		vols = append(vols, vol)
	}

	return vols, nil
}

// UpdateVolume applies config changes to the volume.
func (d *powerstore) UpdateVolume(vol Volume, changedConfig map[string]string) error {
	newSize, sizeChanged := changedConfig["size"]
	if sizeChanged {
		err := d.SetVolumeQuota(vol, newSize, false, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

// DeleteVolume deletes a volume of the storage device.
func (d *powerstore) DeleteVolume(vol Volume, op *operations.Operation) error {
	volResource, err := d.getVolumeResourceByVolume(vol)
	if err != nil {
		return err
	}

	if volResource != nil {
		err = d.deleteVolumeResource(volResource)
		if err != nil {
			return err
		}
	}

	hostResource, _, err := d.getHostWithInitiatorResource()
	if err != nil {
		return err
	}

	if hostResource != nil {
		err = d.deleteHostAndInitiatorResource(hostResource)
		if err != nil {
			return err
		}
	}

	if vol.IsVMBlock() {
		fsVol := vol.NewVMBlockFilesystemVolume()
		err := d.DeleteVolume(fsVol, op)
		if err != nil {
			return err
		}
	}

	mountPath := vol.MountPath()
	if vol.contentType == ContentTypeFS && shared.PathExists(mountPath) {
		err := wipeDirectory(mountPath)
		if err != nil {
			return err
		}

		err = os.RemoveAll(mountPath)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("Removing %q directory: %w", mountPath, err)
		}
	}

	return nil
}

// MountVolume mounts a volume and increments ref counter. Please call
// UnmountVolume() when done with the volume.
func (d *powerstore) MountVolume(vol Volume, op *operations.Operation) error {
	return mountVolume(d, vol, d.getMappedDevicePath, op)
}

// UnmountVolume simulates unmounting a volume.
//
// keepBlockDev indicates if the backing block device should not be unmapped if
// the volume is unmounted.
func (d *powerstore) UnmountVolume(vol Volume, keepBlockDev bool, op *operations.Operation) (bool, error) {
	return unmountVolume(d, vol, keepBlockDev, d.getMappedDevicePath, d.unmapVolume, op)
}

// RenameVolume renames a volume and its snapshots.
func (d *powerstore) RenameVolume(vol Volume, newVolName string, op *operations.Operation) error {
	// Renaming a volume in PowerStore will not change the name of the associated volume resource.
	return nil
}

// getMappedDevicePath returns the local device path for the given volume.
//
// Indicate with mapVolume if the volume should get mapped to the system if it
// is not present.
func (d *powerstore) getMappedDevicePath(vol Volume, mapVolume bool) (string, revert.Hook, error) {
	revert := revert.New()
	defer revert.Fail()

	volResource, err := d.getExistingVolumeResourceByVolume(vol)
	if err != nil {
		return "", nil, err
	}

	if mapVolume {
		cleanup, err := d.mapVolumeByVolumeResource(volResource)
		if err != nil {
			return "", nil, err
		}

		revert.Add(cleanup)
	}

	devicePath, err := d.getMappedDevicePathByVolumeWWN(d.volumeWWN(volResource), mapVolume)
	if err != nil {
		return "", nil, err
	}

	cleanup := revert.Clone().Fail
	revert.Success()
	return devicePath, cleanup, nil
}

// unmapVolume unmaps the given volume from this host.
func (d *powerstore) unmapVolume(vol Volume) error {
	volResource, err := d.getExistingVolumeResourceByVolume(vol)
	if err != nil {
		return err
	}

	return d.unmapVolumeByVolumeResource(volResource)
}
