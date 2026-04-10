package drivers

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"

	"github.com/canonical/lxd/lxd/operations"
	"github.com/canonical/lxd/lxd/storage/block"
	"github.com/canonical/lxd/lxd/storage/filesystem"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/logger"
	"github.com/canonical/lxd/shared/revert"
	"github.com/canonical/lxd/shared/units"
	"github.com/canonical/lxd/shared/validate"
)

// powerStoreResourceNamePrefix common prefix for all resource names in PowerStore.
const powerStoreResourceNamePrefix = "lxd:"

// powerStorePoolAndVolSep separates pool name and volume data in encoded volume names.
const powerStorePoolAndVolSep = "-"

const (
	powerStoreVolPrefixSep = "_" // volume name prefix separator
	powerStoreVolSuffixSep = "." // volume name suffix separator

	powerStoreContainerVolPrefix = "c" // volume name prefix indicating container volume
	powerStoreVMVolPrefix        = "v" // volume name prefix indicating virtual machine volume
	powerStoreImageVolPrefix     = "i" // volume name prefix indicating image volume
	powerStoreCustomVolPrefix    = "u" // volume name prefix indicating custom volume

	powerStoreBlockVolSuffix = "b"     // volume name suffix used for block content type volumes
	powerStoreISOVolSuffix   = "i"     // volume name suffix used for iso content type volumes
	powerStoreVolSnapSuffix  = "-snap" // volume snapshot name suffix
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

// // volumeWWN derives the world wide name of a volume resource in PowerStore
// // from the provided volume resource.
// func (d *powerstore) volumeWWN(volResource *powerStoreVolumeResource) string {
// 	_, wwn, _ := strings.Cut(volResource.WWN, ".")
// 	return wwn
// }

// // roundVolumeBlockSizeBytes rounds the given size (in bytes) up to the next
// // multiple of 1 MiB, which is the minimum volume size on PowerStore.
// func (d *powerstore) roundVolumeBlockSizeBytes(_ Volume, sizeBytes int64) int64 {
// 	return roundAbove(powerStoreMinVolumeSizeBytes, sizeBytes)
// }

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

	volName, err := d.getVolumeName(vol)
	if err != nil {
		return err
	}

	psVol, err := d.client().GetVolumeByName(d.state.ShutdownCtx, volName)
	if err != nil {
		return err
	}

	// Do nothing if volume is already specified size (+/- 512 bytes).
	if psVol.Size+512 > sizeBytes && psVol.Size-512 < sizeBytes {
		return nil
	}

	// PowerStore supports increasing of size only.
	if sizeBytes < psVol.Size {
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
		err = d.client().ResizeVolume(d.state.ShutdownCtx, psVol.ID, sizeBytes)
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
	err = d.client().ResizeVolume(d.state.ShutdownCtx, psVol.ID, sizeBytes)
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

// HasVolume indicates whether a specific volume exists on the storage pool.
func (d *powerstore) HasVolume(vol Volume) (bool, error) {
	volName, err := d.getVolumeName(vol)
	if err != nil {
		return false, err
	}

	_, err = d.client().GetVolumeByName(d.state.ShutdownCtx, volName)
	if err != nil {
		return false, err
	}

	return true, nil
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

// CreateVolume creates an empty volume and can optionally fill it by executing
// the supplied filler function.
func (d *powerstore) CreateVolume(vol Volume, filler *VolumeFiller, op *operations.Operation) error {
	client := d.client()

	revert := revert.New()
	defer revert.Fail()

	volName, err := d.getVolumeName(vol)
	if err != nil {
		return err
	}

	sizeBytes, err := units.ParseByteSizeString(vol.ConfigSize())
	if err != nil {
		return err
	}

	sizeBytes = d.roundVolumeBlockSizeBytes(vol, sizeBytes)

	volID, err := client.CreateVolume(d.state.ShutdownCtx, volName, sizeBytes)
	if err != nil {
		return err
	}

	revert.Add(func() { _ = client.DeleteVolume(d.state.ShutdownCtx, volID) })

	if vol.contentType == ContentTypeFS {
		devPath, cleanup, err := d.getMappedDevicePath(vol, true)
		if err != nil {
			return err
		}

		revert.Add(cleanup)

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

// DeleteVolume deletes a volume of the storage device.
func (d *powerstore) DeleteVolume(vol Volume, op *operations.Operation) error {
	client := d.client()
	ctx := d.state.ShutdownCtx

	connector, err := d.connector()
	if err != nil {
		return err
	}

	qn, err := connector.QualifiedName()
	if err != nil {
		return err
	}

	volName, err := d.getVolumeName(vol)
	if err != nil {
		return err
	}

	volID, err := client.GetVolumeID(ctx, volName)
	if err != nil {
		return err
	}

	err = client.DeleteVolume(ctx, volID)
	if err != nil {
		return err
	}

	psHost, err := client.GetCurrentHost(ctx, connector.Type(), qn)
	if err != nil {
		// If the host doesn't exist, continue with the deletion of
		// the volume and do not try to delete the volume mapping as
		// it cannot exist.
		if !api.StatusErrorCheck(err, http.StatusNotFound) {
			return err
		}
	} else {
		// If the host exists, attempt to delete the volume mapping for the deleted volume.
		// If the mapping doesn't exist, continue with the deletion as the volume is already deleted.
		err = client.DetachHostFromVolume(ctx, psHost.ID, volID)
	}

	if psHost != nil {

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

// VolumeSnapshots returns a list of volume snapshot names for the given volume.
func (d *powerstore) VolumeSnapshots(vol Volume, op *operations.Operation) ([]string, error) {
	client := d.client()

	volName, err := d.getVolumeName(vol)
	if err != nil {
		return nil, err
	}

	volID, err := client.GetVolumeID(d.state.ShutdownCtx, volName)
	if err != nil {
		return nil, err
	}

	snapshots, err := client.GetVolumeSnapshots(d.state.ShutdownCtx, volID)
	if err != nil {
		return nil, err
	}

	snapshotNames := make([]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		snapshotNames = append(snapshotNames, snapshot.Name)
	}

	return snapshotNames, nil
}

// CreateVolumeSnapshot creates a snapshot of a volume.
func (d *powerstore) CreateVolumeSnapshot(snapVol Volume, op *operations.Operation) error {
	client := d.client()

	revert := revert.New()
	defer revert.Fail()

	parentName, _, _ := api.GetParentAndSnapshotName(snapVol.name)
	sourcePath := GetVolumeMountPath(d.name, snapVol.volType, parentName)

	if filesystem.IsMountPoint(sourcePath) {
		// Attempt to sync and freeze filesystem, but do not error if not able to freeze (as filesystem
		// could still be busy), as we do not guarantee the consistency of a snapshot. This is costly but
		// try to ensure that all cached data has been committed to disk. If we don't then the snapshot
		// of the underlying filesystem can be inconsistent or, in the worst case, empty.
		unfreezeFS, err := d.filesystemFreeze(sourcePath)
		if err == nil {
			defer func() { _ = unfreezeFS() }()
		}
	}

	// Create the parent directory.
	err := createParentSnapshotDirIfMissing(d.name, snapVol.volType, parentName)
	if err != nil {
		return err
	}

	err = snapVol.EnsureMountPath()
	if err != nil {
		return err
	}

	volName, err := d.getVolumeName(snapVol.GetParent())
	if err != nil {
		return err
	}

	snapVolName, err := d.getVolumeName(snapVol)
	if err != nil {
		return err
	}

	volID, err := client.GetVolumeID(d.state.ShutdownCtx, volName)
	if err != nil {
		return err
	}

	_, err = d.client().CreateVolumeSnapshot(d.state.ShutdownCtx, volID, snapVol.name)
	if err != nil {
		return err
	}

	revert.Add(func() { _ = d.DeleteVolumeSnapshot(snapVol, op) })

	// For VMs, create a snapshot of the filesystem volume too.
	if snapVol.IsVMBlock() {
		fsVol := snapVol.NewVMBlockFilesystemVolume()

		// Set the parent volume's UUID.
		fsVol.SetParentUUID(snapVol.parentUUID)

		err := d.CreateVolumeSnapshot(fsVol, op)
		if err != nil {
			return err
		}

		revert.Add(func() { _ = d.DeleteVolumeSnapshot(fsVol, op) })
	}

	revert.Success()
	return nil
}

// RenameVolumeSnapshot renames a volume snapshot.
func (d *powerstore) RenameVolumeSnapshot(snapVol Volume, newSnapshotName string, op *operations.Operation) error {
	return nil
}

// DeleteVolumeSnapshot removes a snapshot from the storage device.
func (d *powerstore) DeleteVolumeSnapshot(snapVol Volume, op *operations.Operation) error {
	volSnapResource, err := d.getVolumeResourceSnapshotByVolumeSnapshot(snapVol)
	if err != nil {
		return err
	}

	if volSnapResource != nil {
		err = d.deleteVolumeResourceSnapshot(volSnapResource)
		if err != nil {
			return err
		}
	}

	// Delete temporary volume, if any.
	_, err = d.UnmountVolumeSnapshot(snapVol, op)
	if err != nil {
		return err
	}

	// For VMs, delete a snapshot of the filesystem volume too.
	if snapVol.IsVMBlock() {
		err := d.DeleteVolumeSnapshot(snapVol.NewVMBlockFilesystemVolume(), op)
		if err != nil {
			return err
		}
	}

	return nil
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

// MountVolumeSnapshot mounts a storage volume snapshot.
func (d *powerstore) MountVolumeSnapshot(snapVol Volume, op *operations.Operation) error {
	revert := revert.New()
	defer revert.Fail()

	volSnapResource, err := d.getExistingVolumeResourceSnapshotByVolumeSnapshot(snapVol)
	if err != nil {
		return err
	}

	volResource, err := d.copyVolumeResourceSnapshotToVolume(volSnapResource, snapVol)
	if err != nil {
		return err
	}

	revert.Add(func() { _ = d.deleteVolumeResource(volResource) })

	// For VMs, also create the temporary filesystem volume snapshot.
	if snapVol.IsVMBlock() {
		snapFsVol := snapVol.NewVMBlockFilesystemVolume()

		volFsSnapResource, err := d.getExistingVolumeResourceSnapshotByVolumeSnapshot(snapFsVol)
		if err != nil {
			return err
		}

		volFsResource, err := d.copyVolumeResourceSnapshotToVolume(volFsSnapResource, snapFsVol)
		if err != nil {
			return err
		}

		revert.Add(func() { _ = d.deleteVolumeResource(volFsResource) })
	}

	err = d.MountVolume(snapVol, op)
	if err != nil {
		return err
	}

	revert.Success()
	return nil
}

// UnmountVolume unmounts a storage volume snapshot, returns true if unmounted,
// false if was not mounted.
func (d *powerstore) UnmountVolumeSnapshot(snapVol Volume, op *operations.Operation) (bool, error) {
	wasUnmounted, err := d.UnmountVolume(snapVol, false, op)
	if err != nil {
		return false, err
	}

	if !wasUnmounted {
		return false, nil
	}

	// Cleanup temporary snapshot volume.

	volResource, err := d.getVolumeResourceByVolume(snapVol)
	if err != nil {
		return true, err
	}

	if volResource != nil {
		err := d.deleteVolumeResource(volResource)
		if err != nil {
			return true, err
		}
	}
	// For VMs, also cleanup the temporary volume for a filesystem snapshot.
	if snapVol.IsVMBlock() {
		snapFsVol := snapVol.NewVMBlockFilesystemVolume()

		volFsResource, err := d.getVolumeResourceByVolume(snapFsVol)
		if err != nil {
			return true, err
		}

		if volFsResource != nil {
			err := d.deleteVolumeResource(volFsResource)
			if err != nil {
				return true, err
			}
		}
	}

	return true, nil
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

	connector, err := d.connector()
	if err != nil {
		return err
	}

	// Resize filesystem if needed.
	if vol.contentType == ContentTypeFS {
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

		// Always wait for the disk to reflect the new size. In case SetVolumeQuota
		// is called on an already mapped volume, it might take some time until
		// the actual size of the device is reflected on the host. This is for
		// example the case when creating a volume and the filler performs a resize
		// in case the image exceeds the volume's size.
		err = connector.WaitDiskDeviceResize(d.state.ShutdownCtx, devPath, sizeBytes)
		if err != nil {
			return fmt.Errorf("Failed waiting for volume %q to change its size: %w", vol.name, err)
		}

		// Grow the filesystem to fill block device.
		fsType := vol.ConfigBlockFilesystem()
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

	// Wait for the block device to be resized before moving GPT alt header.
	// This ensures that the GPT alt header is not moved before the actual
	// size is reflected on a local host. Otherwise, the GPT alt header
	// would be moved to the same location.
	err = connector.WaitDiskDeviceResize(d.state.ShutdownCtx, devPath, sizeBytes)
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

// volumeResourceName derives the name of a volume resource in PowerStore from
// the provided volume.
func (d *powerstore) getVolumeName(vol Volume) (string, error) {
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

	return d.globalVolumeNamePrefix() + volName, nil
}

// ensureHost returns a name of the host that is configured with a given IQN. If such host
// does not exist, a new one is created, where host's name equals to the server name with a
// mode included.
func (d *powerstore) ensureHost() (hostName string, cleanup revert.Hook, err error) {
	var hostname string

	revert := revert.New()
	defer revert.Fail()

	connector, err := d.connector()
	if err != nil {
		return "", nil, err
	}

	// Get the qualified name of the host.
	qn, err := connector.QualifiedName()
	if err != nil {
		return "", nil, err
	}

	// Fetch an existing host entry on a storage array.
	host, err := d.client().GetCurrentHost(connector.Type(), qn)
	if err != nil {
		if !api.StatusErrorCheck(err, http.StatusNotFound) {
			return "", nil, err
		}

		// The storage host entry with a qualified name of the current LXD host does not exist.
		// Therefore, create a new one and name it after the server name.
		serverName, err := ResolveServerName(d.state.ServerName)
		if err != nil {
			return "", nil, err
		}

		// Append the mode to the server name because storage array does not allow mixing
		// NQNs, IQNs, and WWNs for a single host.
		hostname = serverName + "-" + connector.Type()

		err = d.client().CreateHost(connector.Type(), hostname, []string{qn})
		if err != nil {
			return "", nil, fmt.Errorf("Failed creating host %q: %w", hostname, err)
		}

		revert.Add(func() {
			err := d.client().DeleteHost(hostname)
			if err != nil {
				d.logger.Warn("DeleteHost API call failed on error path", logger.Ctx{"err": err, "hostname": hostname})
			}
		})
	} else {
		// Hostname already exists with the given IQN.
		hostname = host.Name
	}

	cleanup = revert.Clone().Fail
	revert.Success()
	return hostname, cleanup, nil
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

	connector, err := d.connector()
	if err != nil {
		return err
	}

	// Resize filesystem if needed.
	if vol.contentType == ContentTypeFS {
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

		// Always wait for the disk to reflect the new size. In case SetVolumeQuota
		// is called on an already mapped volume, it might take some time until
		// the actual size of the device is reflected on the host. This is for
		// example the case when creating a volume and the filler performs a resize
		// in case the image exceeds the volume's size.
		err = connector.WaitDiskDeviceResize(d.state.ShutdownCtx, devPath, sizeBytes)
		if err != nil {
			return fmt.Errorf("Failed waiting for volume %q to change its size: %w", vol.name, err)
		}

		// Grow the filesystem to fill block device.
		fsType := vol.ConfigBlockFilesystem()
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

	// Wait for the block device to be resized before moving GPT alt header.
	// This ensures that the GPT alt header is not moved before the actual
	// size is reflected on a local host. Otherwise, the GPT alt header
	// would be moved to the same location.
	err = connector.WaitDiskDeviceResize(d.state.ShutdownCtx, devPath, sizeBytes)
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

// MountVolume mounts a volume and increments ref counter. Please call
// UnmountVolume() when done with the volume.
func (d *powerstore) MountVolume(vol Volume, op *operations.Operation) error {
	return mountVolume(d, vol, d.getMappedDevicePath, op)
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
		cleanup, err := d.mapVolume(vol)
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

// UnmountVolume simulates unmounting a volume.
//
// keepBlockDev indicates if the backing block device should not be unmapped if
// the volume is unmounted.
func (d *powerstore) UnmountVolume(vol Volume, keepBlockDev bool, op *operations.Operation) (bool, error) {
	unmapVolume := func(vol Volume) error {
		volResource, err := d.getExistingVolumeResourceByVolume(vol)
		if err != nil {
			return err
		}

		return d.unmapVolumeByVolumeResource(volResource)
	}

	return unmountVolume(d, vol, keepBlockDev, d.getMappedDevicePath, unmapVolume, op)
}

// UnmountVolume unmounts a storage volume snapshot, returns true if unmounted,
// false if was not mounted.
func (d *powerstore) UnmountVolumeSnapshot(snapVol Volume, op *operations.Operation) (bool, error) {
	wasUnmounted, err := d.UnmountVolume(snapVol, false, op)
	if err != nil {
		return false, err
	}

	if !wasUnmounted {
		return false, nil
	}

	// Cleanup temporary snapshot volume.

	volResource, err := d.getVolumeResourceByVolume(snapVol)
	if err != nil {
		return true, err
	}

	if volResource != nil {
		err := d.deleteVolumeResource(volResource)
		if err != nil {
			return true, err
		}
	}
	// For VMs, also cleanup the temporary volume for a filesystem snapshot.
	if snapVol.IsVMBlock() {
		snapFsVol := snapVol.NewVMBlockFilesystemVolume()

		volFsResource, err := d.getVolumeResourceByVolume(snapFsVol)
		if err != nil {
			return true, err
		}

		if volFsResource != nil {
			err := d.deleteVolumeResource(volFsResource)
			if err != nil {
				return true, err
			}
		}
	}

	return true, nil
}

// mapVolume maps the volume onto this host.
func (d *powerstore) mapVolume(vol Volume) (revert.Hook, error) {
	connector, err := d.connector()
	if err != nil {
		return nil, err
	}

	unlock, err := remoteVolumeMapLock(connector.Type(), d.Info().Name)
	if err != nil {
		return nil, err
	}

	defer unlock()

	reverter := revert.New()
	defer reverter.Fail()

	hostResource, err := d.getOrCreateHostWithInitiatorResource()
	if err != nil {
		return nil, err
	}

	reverter.Add(func() { _ = d.deleteHostAndInitiatorResource(hostResource) })

	volResource, err := d.getExistingVolumeResourceByVolume(vol)
	if err != nil {
		return nil, err
	}

	mapped := slices.ContainsFunc(volResource.MappedVolumes, func(mappingResource *clients.PowerStoreHostVolumeMapping) bool {
		return mappingResource.HostID == hostResource.ID
	})
	if !mapped {
		err := d.client().AttachHostToVolume(d.state.ShutdownCtx, hostResource.ID, volResource.ID)
		if err != nil {
			return nil, err
		}

		reverter.Add(func() { _ = d.client().DetachHostFromVolume(d.state.ShutdownCtx, hostResource.ID, volResource.ID) })
	}

	// Reverting mapping or connection outside mapVolume function could conflict with other
	// ongoing operations as lock will already be released. Therefore, use unmapVolume instead
	// because it ensures the lock is acquired and accounts for an existing session before
	// unmapping a volume.
	outerReverter := revert.New()
	if !mapped {
		outerReverter.Add(func() { _ = d.unmapVolume(vol) })
	}

	targets, err := d.targets()
	if err != nil {
		return nil, err
	}

	for qualifiedName, addresses := range powerStoreGroupTargetsAddressesByQualifiedName(targets...) {
		cleanup, err := connector.Connect(d.state.ShutdownCtx, qualifiedName, addresses...)
		if err != nil {
			return nil, err
		}

		reverter.Add(cleanup)
		outerReverter.Add(cleanup)
	}

	reverter.Success()
	return outerReverter.Fail, nil
}

// CreateVolumeSnapshot creates a snapshot of a volume.
func (d *powerstore) CreateVolumeSnapshot(snapVol Volume, op *operations.Operation) error {
	revert := revert.New()
	defer revert.Fail()

	parentName, _, _ := api.GetParentAndSnapshotName(snapVol.name)
	sourcePath := GetVolumeMountPath(d.name, snapVol.volType, parentName)

	if filesystem.IsMountPoint(sourcePath) {
		// Attempt to sync and freeze filesystem, but do not error if not able to freeze (as filesystem
		// could still be busy), as we do not guarantee the consistency of a snapshot. This is costly but
		// try to ensure that all cached data has been committed to disk. If we don't then the snapshot
		// of the underlying filesystem can be inconsistent or, in the worst case, empty.
		unfreezeFS, err := d.filesystemFreeze(sourcePath)
		if err == nil {
			defer func() { _ = unfreezeFS() }()
		}
	}

	// Create the parent directory.
	err := createParentSnapshotDirIfMissing(d.name, snapVol.volType, parentName)
	if err != nil {
		return err
	}

	err = snapVol.EnsureMountPath()
	if err != nil {
		return err
	}

	_, err = d.createVolumeResourceSnapshot(snapVol)
	if err != nil {
		return err
	}

	revert.Add(func() { _ = d.DeleteVolumeSnapshot(snapVol, op) })

	// For VMs, create a snapshot of the filesystem volume too.
	if snapVol.IsVMBlock() {
		fsVol := snapVol.NewVMBlockFilesystemVolume()

		// Set the parent volume's UUID.
		fsVol.SetParentUUID(snapVol.parentUUID)

		err := d.CreateVolumeSnapshot(fsVol, op)
		if err != nil {
			return err
		}
	}

	if volumePath != "" {
		// Wait until the volume has disappeared.
		ctx, cancel := context.WithTimeout(d.state.ShutdownCtx, 30*time.Second)
		defer cancel()

		if !block.WaitDiskDeviceGone(ctx, volumePath) {
			return fmt.Errorf("Timeout exceeded waiting for disk device %q related to PowerStore volume resource with ID %q to disappear", volumePath, volResource.ID)
		}
	}

	// Disconnect connector if:
	// - there is no associated PowerStore host resource,
	// - there are no other volumes mapped.
	if hostResource == nil || len(hostResource.MappedHosts) == 0 {
		targets, err := d.targets()
		if err != nil {
			return err
		}

		revert.Add(func() { _ = d.DeleteVolumeSnapshot(fsVol, op) })
	}

	revert.Success()
	return nil
}

// DeleteVolumeSnapshot removes a snapshot from the storage device.
func (d *powerstore) DeleteVolumeSnapshot(snapVol Volume, op *operations.Operation) error {
	volSnapResource, err := d.getVolumeResourceSnapshotByVolumeSnapshot(snapVol)
	if err != nil {
		return err
	}

	if volSnapResource != nil {
		err = d.deleteVolumeResourceSnapshot(volSnapResource)
		if err != nil {
			return err
		}
	}

	// Delete temporary volume, if any.
	_, err = d.UnmountVolumeSnapshot(snapVol, op)
	if err != nil {
		return err
	}

	// For VMs, delete a snapshot of the filesystem volume too.
	if snapVol.IsVMBlock() {
		err := d.DeleteVolumeSnapshot(snapVol.NewVMBlockFilesystemVolume(), op)
		for qualifiedName := range powerStoreGroupTargetsAddressesByQualifiedName(targets...) {
			err = connector.Disconnect(qualifiedName)
			if err != nil {
				return err
			}
		}
	}

	if hostResource != nil {
		err = d.deleteHostAndInitiatorResource(hostResource)
		if err != nil {
			return err
		}
	}

	return nil
}

// RenameVolumeSnapshot renames a volume snapshot.
func (d *powerstore) RenameVolumeSnapshot(snapVol Volume, newSnapshotName string, op *operations.Operation) error {
	return nil
}

// VolumeSnapshots returns a list of volume snapshot names for the given volume.
func (d *powerstore) VolumeSnapshots(vol Volume, op *operations.Operation) ([]string, error) {
	volResource, err := d.getExistingVolumeResourceByVolume(vol)
	if err != nil {
		return nil, err
	}

	volSnapResources, err := d.client().GetVolumeSnapshots(d.state.ShutdownCtx, volResource.ID)
	if err != nil {
		return nil, err
	}

	snapshotNames := make([]string, 0, len(volSnapResources))
	for _, volSnapResource := range volSnapResources {
		snapshotNames = append(snapshotNames, volSnapResource.Name)
	}

	return snapshotNames, nil
}
