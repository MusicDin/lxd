package drivers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"

	"github.com/canonical/lxd/lxd/backup"
	"github.com/canonical/lxd/lxd/migration"
	"github.com/canonical/lxd/lxd/operations"
	"github.com/canonical/lxd/lxd/storage/block"
	"github.com/canonical/lxd/lxd/storage/connectors"
	"github.com/canonical/lxd/lxd/storage/filesystem"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/logger"
	"github.com/canonical/lxd/shared/revert"
	"github.com/canonical/lxd/shared/units"
	"github.com/canonical/lxd/shared/validate"
)

// powerStoreResourcePrefix common prefix for all resource names in PowerStore.
const powerStoreResourcePrefix = "lxd-"

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
// Use smallest possible prefixes since PowerStore volume names are limited to 63 characters.
var powerStoreVolTypePrefixes = map[VolumeType]string{
	VolumeTypeContainer: "c",
	VolumeTypeVM:        "v",
	VolumeTypeImage:     "i",
	VolumeTypeCustom:    "u",
}

// powerStoreVolContentTypeSuffixes maps volume's content type to storage volume name suffix.
var powerStoreVolContentTypeSuffixes = map[ContentType]string{
	// Suffix used for block content type volumes.
	ContentTypeBlock: "b",

	// Suffix used for ISO content type volumes.
	ContentTypeISO: "i",
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

// GetVolumeDiskPath returns the location of a root disk block device.
func (d *powerstore) GetVolumeDiskPath(vol Volume) (string, error) {
	d.logger.Warn("Getting volume disk path", logger.Ctx{"vol": vol.name})
	if vol.IsVMBlock() || (vol.volType == VolumeTypeCustom && IsContentBlock(vol.contentType)) {
		devPath, _, err := d.getMappedDevicePath(vol, false)
		return devPath, err
	}

	return "", ErrNotSupported
}

// GetVolumeUsage returns the disk space used by the volume.
func (d *powerstore) GetVolumeUsage(vol Volume) (int64, error) {
	d.logger.Warn("Getting volume usage", logger.Ctx{"vol": vol.name})
	// If mounted, use the filesystem stats for pretty accurate usage information.
	if vol.contentType == ContentTypeFS && filesystem.IsMountPoint(vol.MountPath()) {
		var stat unix.Statfs_t
		err := unix.Statfs(vol.MountPath(), &stat)
		if err != nil {
			return -1, err
		}

		return int64(stat.Blocks-stat.Bfree) * int64(stat.Bsize), nil
	}

	volName, err := d.encodeVolumeName(vol)
	if err != nil {
		return -1, err
	}

	psVol, err := d.client().GetVolume(volName)
	if err != nil {
		return -1, err
	}

	return psVol.LogicalUsed, nil
}

// HasVolume indicates whether a specific volume exists on the storage pool.
func (d *powerstore) HasVolume(vol Volume) (bool, error) {
	d.logger.Warn("Checking if volume exists", logger.Ctx{"vol": vol.name})

	client := d.client()

	volName, err := d.encodeVolumeName(vol)
	if err != nil {
		return false, err
	}

	// If volume represents a snapshot, also retrieve (encoded) volume name of the parent,
	// and check if the snapshot exists.
	if vol.IsSnapshot() {
		parentVol := vol.GetParent()
		parentVolName, err := d.encodeVolumeName(parentVol)
		if err != nil {
			return false, err
		}

		parentVolID, err := client.GetVolumeID(parentVolName)
		if err != nil {
			return false, err
		}

		_, err = client.GetVolumeSnapshot(parentVolID, volName)
		if err != nil {
			if api.StatusErrorCheck(err, http.StatusNotFound) {
				return false, nil
			}

			return false, err
		}

		return true, nil
	}

	// Otherwise, check if the volume exists.
	_, err = client.GetVolume(volName)
	if err != nil {
		if api.StatusErrorCheck(err, http.StatusNotFound) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// ListVolumes returns a list of LXD volumes in storage pool.
// It returns all volumes and sets the volume's volatile.uuid extracted from
// the name.
func (d *powerstore) ListVolumes() ([]Volume, error) {
	d.logger.Warn("Listing volumes")
	psVols, err := d.client().GetVolumes()
	if err != nil {
		return nil, err
	}

	vols := make([]Volume, 0, len(psVols))
	for _, volResource := range psVols {
		volType, volUUID, volContentType, err := d.decodeVolumeName(volResource.Name)
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
	d.logger.Warn("Creating volume", logger.Ctx{"vol": vol.name, "content_type": vol.contentType, "vol_type": vol.volType, "size": vol.ConfigSize(), "block_filesystem": vol.ConfigBlockFilesystem()})
	defer d.logger.Warn("Volume created", logger.Ctx{"vol": vol.name, "content_type": vol.contentType, "vol_type": vol.volType, "size": vol.ConfigSize(), "block_filesystem": vol.ConfigBlockFilesystem()})

	client := d.client()

	revert := revert.New()
	defer revert.Fail()

	volName, err := d.encodeVolumeName(vol)
	if err != nil {
		return err
	}

	sizeBytes, err := units.ParseByteSizeString(vol.ConfigSize())
	if err != nil {
		return err
	}

	sizeBytes = d.roundVolumeBlockSizeBytes(vol, sizeBytes)

	volID, err := client.CreateVolume(volName, sizeBytes)
	if err != nil {
		return fmt.Errorf("Failed to create volume %q: %w", vol.name, err)
	}

	revert.Add(func() { _ = client.DeleteVolume(volID) })

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

			allowUnsafeResize := false
			if vol.volType == VolumeTypeImage {
				// Allow filler to resize initial image volume as needed.
				// Some storage drivers don't normally allow image volumes to be resized due to
				// them having read-only snapshots that cannot be resized. However when creating
				// the initial image volume and filling it before the snapshot is taken resizing
				// can be allowed and is required in order to support unpacking images larger than
				// the default volume size. The filler function is still expected to obey any
				// volume size restrictions configured on the pool.
				// Unsafe resize is also needed to disable filesystem resize safety checks.
				// This is safe because if for some reason an error occurs the volume will be
				// discarded rather than leaving a corrupt filesystem.
				allowUnsafeResize = true
			}

			// Run the filler.
			err = d.runFiller(vol, devPath, filler, allowUnsafeResize)
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

// CreateVolumeFromBackup re-creates a volume from its exported state.
func (d *powerstore) CreateVolumeFromBackup(vol VolumeCopy, srcBackup backup.Info, srcData io.ReadSeeker, op *operations.Operation) (VolumePostHook, revert.Hook, error) {
	return genericVFSBackupUnpack(d, d.state, vol, srcBackup.Snapshots, srcData, op)
}

// CreateVolumeFromImage creates a new volume from an image, unpacking it directly.
func (d *powerstore) CreateVolumeFromImage(vol Volume, imgVol *Volume, filler *VolumeFiller, op *operations.Operation) error {
	return d.CreateVolume(vol, filler, op)
}

// TODO:
// 1. Create empty volume (probably with same size as source?).
// 2. Restore volume from snapshots
// 3. Copy volume from source to destination.
// CreateVolumeFromCopy provides same-pool volume copying functionality.
func (d *powerstore) CreateVolumeFromCopy(vol VolumeCopy, srcVol VolumeCopy, allowInconsistent bool, op *operations.Operation) error {
	client := d.client()

	revert := revert.New()
	defer revert.Fail()

	// Function to run once the volume is created, which will ensure appropriate permissions
	// on the mount path inside the volume, and resize the volume to specified size.
	postCreateTasks := func(v Volume) error {
		if vol.contentType == ContentTypeFS {
			// Mount the volume and ensure the permissions are set correctly inside the mounted volume.
			err := v.MountTask(func(_ string, _ *operations.Operation) error {
				return v.EnsureMountPath()
			}, op)
			if err != nil {
				return err
			}
		}

		// Resize volume to the size specified.
		err := d.SetVolumeQuota(v, vol.config["size"], false, op)
		if err != nil {
			return err
		}

		return nil
	}

	// For VMs, also copy the filesystem volume.
	if vol.IsVMBlock() {
		// Ensure that the volume's snapshots are also replaced with their filesystem counterpart.
		fsVolSnapshots := make([]Volume, 0, len(vol.Snapshots))
		for _, snapshot := range vol.Snapshots {
			fsVolSnapshots = append(fsVolSnapshots, snapshot.NewVMBlockFilesystemVolume())
		}

		srcFsVolSnapshots := make([]Volume, 0, len(srcVol.Snapshots))
		for _, snapshot := range srcVol.Snapshots {
			srcFsVolSnapshots = append(srcFsVolSnapshots, snapshot.NewVMBlockFilesystemVolume())
		}

		fsVol := NewVolumeCopy(vol.NewVMBlockFilesystemVolume(), fsVolSnapshots...)
		srcFSVol := NewVolumeCopy(srcVol.NewVMBlockFilesystemVolume(), srcFsVolSnapshots...)

		// Ensure parent UUID is retained for the filesystem volumes.
		fsVol.SetParentUUID(vol.parentUUID)
		srcFSVol.SetParentUUID(srcVol.parentUUID)

		err := d.CreateVolumeFromCopy(fsVol, srcFSVol, false, op)
		if err != nil {
			return err
		}

		revert.Add(func() { _ = d.DeleteVolume(fsVol.Volume, op) })
	}

	volID := ""
	volName, err := d.encodeVolumeName(vol.Volume)
	if err != nil {
		return err
	}

	srcVolName, err := d.encodeVolumeName(srcVol.Volume)
	if err != nil {
		return err
	}

	srcVolID, err := client.GetVolumeID(srcVolName)
	if err != nil {
		return err
	}

	// Since snapshots are first copied into destination volume from which a new snapshot is created,
	// we need to also remove the destination volume if an error occurs during copying of snapshots.
	deleteVolCopy := true

	// First clone the volume.
	// If source volume has snapshots, this volume will be overwriten, and we will need to
	// clone it again. The reason behind this is because PowerStore only allows refreshing
	// volumes from "related" volumes or snapshots. To sequentially copy snapshots (using
	// refresh and snapshot approach), we need to create this "relationship" by first cloning
	// the source volume.
	// if srcVol.IsSnapshot() {
	// 	// Copy the source snapshot into destination volume.
	// 	err = d.client().CloneVolume(srcVolName, volName)
	// 	if err != nil {
	// 		return err
	// 	}
	// } else {
	// 	err = d.client().copyVolume(srcPoolName, srcVolName, poolName, volName, true)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	// Copy volume snapshots.
	// PowerStore does copy snapshots along with the volume. Therefore, we copy the snapshots
	// sequentially once the volume was copied. Each snapshot is first copied into destination
	// volume from which a new snapshot is created. The process is repeted until all snapshots
	// are copied.
	if !srcVol.IsSnapshot() {
		for _, snapshot := range vol.Snapshots {
			_, snapshotShortName, _ := api.GetParentAndSnapshotName(snapshot.name)

			// Find the corresponding source snapshot.
			var srcSnapshot *Volume
			for _, srcSnap := range srcVol.Snapshots {
				_, srcSnapshotShortName, _ := api.GetParentAndSnapshotName(srcSnap.name)
				if snapshotShortName == srcSnapshotShortName {
					srcSnapshot = &srcSnap
					break
				}
			}

			if srcSnapshot == nil {
				return fmt.Errorf("Failed copying snapshot %q: Source snapshot does not exist", snapshotShortName)
			}

			srcSnapshotName, err := d.encodeVolumeName(*srcSnapshot)
			if err != nil {
				return err
			}

			srcSnapshotID, err := client.GetVolumeSnapshotID(srcVolID, srcSnapshotName)
			if err != nil {
				return err
			}

			// Copy the snapshot.
			if volID == "" {
				// If this is a first snapshot, we need to clone it as the
				// destination volume does not exist yet.
				volID, err = client.CloneVolume(srcSnapshotID, volName)

				// Volume is created, make sure to remove it in case the operation
				// fails.
				revert.Add(func() { _ = d.DeleteVolume(vol.Volume, op) })
				deleteVolCopy = false
			} else {
				// Otherwise, overwrite the destination volume.
				err = client.RefreshVolume(srcSnapshotID, volID)
			}

			if err != nil {
				return fmt.Errorf("Failed copying snapshot %q into volume %q: %w", snapshot.name, vol.name, err)
			}

			// Set snapshot's parent UUID and retain source snapshot UUID.
			snapshot.SetParentUUID(vol.config["volatile.uuid"])

			// Create snapshot from a new volume (that was created from the source snapshot).
			// However, do not create VM's filesystem volume snapshot, as filesystem volume is
			// copied before block volume.
			err = d.createVolumeSnapshot(snapshot, false, op)
			if err != nil {
				return err
			}
		}
	}

	// Finally, copy the source volume (or snapshot) into destination volume snapshots.
	if srcVol.IsSnapshot() {
		// Copy the source snapshot into destination volume.
		_, err = client.CloneVolume(srcVolID, volName)
		if err != nil {
			return err
		}
	} else {
		if volID == "" {
			_, err = client.CloneVolume(srcVolName, volName)
			if err != nil {
				return err
			}
		} else {
			err = client.RefreshVolume(srcVolName, volName)
			if err != nil {
				return err
			}
		}
	}

	// Add reverted to delete destination volume, if not already added.
	if deleteVolCopy {
		revert.Add(func() { _ = d.DeleteVolume(vol.Volume, op) })
	}

	err = postCreateTasks(vol.Volume)
	if err != nil {
		return err
	}

	revert.Success()
	return nil
}

// CreateVolumeFromMigration creates a volume being sent via a migration.
func (d *powerstore) CreateVolumeFromMigration(vol VolumeCopy, conn io.ReadWriteCloser, volTargetArgs migration.VolumeTargetArgs, preFiller *VolumeFiller, op *operations.Operation) error {
	// When performing a cluster member move prepare the volumes on the target side.
	if volTargetArgs.ClusterMoveSourceName != "" {
		err := vol.EnsureMountPath()
		if err != nil {
			return err
		}

		if vol.IsVMBlock() {
			fsVol := NewVolumeCopy(vol.NewVMBlockFilesystemVolume())
			err := d.CreateVolumeFromMigration(fsVol, conn, volTargetArgs, preFiller, op)
			if err != nil {
				return err
			}
		}

		return nil
	}

	_, err := genericVFSCreateVolumeFromMigration(d, nil, vol, conn, volTargetArgs, preFiller, op)
	return err
}

// UpdateVolume applies config changes to the volume.
func (d *powerstore) UpdateVolume(vol Volume, changedConfig map[string]string) error {
	d.logger.Warn("Updating volume", logger.Ctx{"vol": vol.name})
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
	d.logger.Warn("Deleting volume", logger.Ctx{"vol": vol.name})
	defer d.logger.Warn("Volume deleted", logger.Ctx{"vol": vol.name})

	client := d.client()

	connector, err := d.connector()
	if err != nil {
		return err
	}

	qn, err := connector.QualifiedName()
	if err != nil {
		return err
	}

	volName, err := d.encodeVolumeName(vol)
	if err != nil {
		return err
	}

	volID, err := client.GetVolumeID(volName)
	if err != nil {
		return fmt.Errorf("Failed to retrieve volume %q: %w", vol.name, err)
	}

	err = client.DeleteVolume(volID)
	if err != nil {
		return fmt.Errorf("Failed to delete volume %q: %w", vol.name, err)
	}

	psHost, err := client.GetCurrentHost(connector.Type(), qn)
	if err != nil {
		// If the host doesn't exist, continue with the deletion of
		// the volume and do not try to delete the volume mapping as
		// it cannot exist.
		if !api.StatusErrorCheck(err, http.StatusNotFound) {
			return fmt.Errorf("Failed to retrieve current host for volume %q: %w", vol.name, err)
		}
	} else {
		// If the host exists, attempt to delete the volume mapping for the deleted volume.
		// If the mapping doesn't exist, continue with the deletion as the volume is already deleted.
		err = client.DetachVolumeFromHost(volID, psHost.Name)
		if err != nil {
			return fmt.Errorf("Failed to detach volume %q from host %q: %w", vol.name, psHost.Name, err)
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

// SetVolumeQuota applies a size limit on volume.
func (d *powerstore) SetVolumeQuota(vol Volume, size string, allowUnsafeResize bool, op *operations.Operation) error {
	d.logger.Warn("Setting volume quota", logger.Ctx{"vol": vol.name, "size": size})

	client := d.client()

	// Convert to bytes.
	sizeBytes, err := units.ParseByteSizeString(size)
	if err != nil {
		return err
	}

	// Do nothing if size isn't specified.
	if sizeBytes <= 0 {
		return nil
	}

	volName, err := d.encodeVolumeName(vol)
	if err != nil {
		return err
	}

	psVol, err := client.GetVolume(volName)
	if err != nil {
		return fmt.Errorf("Failed to retrieve volume %q: %w", vol.name, err)
	}

	// Do nothing if volume is already specified size (+/- 512 bytes).
	if psVol.Size+512 > sizeBytes && psVol.Size-512 < sizeBytes {
		return nil
	}

	// Only volume expansion is supported.
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

	connector, err := d.connector()
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
		err = client.ResizeVolume(psVol.ID, sizeBytes)
		if err != nil {
			return fmt.Errorf("Failed to resize volume %q: %w", vol.name, err)
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
	err = client.ResizeVolume(psVol.ID, sizeBytes)
	if err != nil {
		return fmt.Errorf("Failed to resize volume %q: %w", vol.name, err)
	}

	devPath, cleanup, err := d.getMappedDevicePath(vol, true)
	if err != nil {
		return err
	}

	defer cleanup()

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

// MountVolume mounts a volume and increments ref counter. Please call
// UnmountVolume() when done with the volume.
func (d *powerstore) MountVolume(vol Volume, op *operations.Operation) error {
	d.logger.Warn("Mounting volume", logger.Ctx{"vol": vol.name})
	return mountVolume(d, vol, d.getMappedDevicePath, op)
}

// UnmountVolume simulates unmounting a volume.
//
// keepBlockDev indicates if the backing block device should not be unmapped if
// the volume is unmounted.
func (d *powerstore) UnmountVolume(vol Volume, keepBlockDev bool, op *operations.Operation) (bool, error) {
	d.logger.Warn("Unmounting volume", logger.Ctx{"vol": vol.name, "keep_block_dev": keepBlockDev})
	return unmountVolume(d, vol, keepBlockDev, d.getMappedDevicePath, d.unmapVolume, op)
}

// RenameVolume renames a volume and its snapshots.
func (d *powerstore) RenameVolume(vol Volume, newVolName string, op *operations.Operation) error {
	d.logger.Warn("Renaming volume", logger.Ctx{"vol": vol.name, "new_name": newVolName})
	// Renaming a volume in PowerStore will not change the name of the associated volume resource.
	return nil
}

// encodeVolumeName derives the name of a volume resource in PowerStore from the provided volume.
func (d *powerstore) encodeVolumeName(vol Volume) (string, error) {
	volUUID, err := uuid.Parse(vol.config["volatile.uuid"])
	if err != nil {
		return "", fmt.Errorf(`Failed parsing "volatile.uuid" from volume %q: %w`, vol.name, err)
	}

	volName := volUUID.String()

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

	return d.encodeStoragePoolName() + volName, nil
}

// decodeVolumeName decodes the PowerStore volume resource name and extracts the stored data.
func (d *powerstore) decodeVolumeName(name string) (volType VolumeType, volUUID uuid.UUID, volContentType ContentType, err error) {
	poolAndVolName, ok := strings.CutPrefix(name, powerStoreResourcePrefix)
	if !ok {
		return "", uuid.Nil, "", fmt.Errorf("Failed decoding volume name %q: Missing LXD prefix", name)
	}

	// Remove poolName prefix.
	poolName, volName, ok := strings.Cut(poolAndVolName, powerStorePoolAndVolSep)
	if !ok || poolName == "" || volName == "" {
		return "", uuid.Nil, "", fmt.Errorf("Failed decoding volume name %q: Invalid name format", name)
	}

	// Volume prefix represents volume type.
	volPrefix, volNameWithoutPrefix, ok := strings.Cut(volName, powerStoreVolPrefixSep)
	if ok {
		volName = volNameWithoutPrefix
		for k, v := range powerStoreVolTypePrefixes {
			if v == volPrefix {
				volType = k
				break
			}
		}
	}

	volName, volSuffix, ok := strings.Cut(volName, powerStoreVolSuffixSep)
	if ok {
		for k, v := range powerStoreVolContentTypeSuffixes {
			if v == volSuffix {
				volContentType = k
				break
			}
		}
	}

	volUUID, err = uuid.Parse(volName)
	if err != nil {
		return "", uuid.Nil, "", fmt.Errorf("Failed decoding volume name %q: %w", name, err)
	}

	return volType, volUUID, volContentType, nil
}

// ensureHost returns a name of the host that is configured with a given IQN. If such host
// does not exist, a new one is created, where host's name equals to the server name with a
// mode included.
func (d *powerstore) ensureHost() (hostName string, cleanup revert.Hook, err error) {
	d.logger.Warn("Ensuring host")

	client := d.client()

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
	host, err := client.GetCurrentHost(connector.Type(), qn)
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
		hostName = serverName + "-" + connector.Type()

		err = client.CreateHost(hostName, connector.Type(), qn)
		if err != nil {
			return "", nil, fmt.Errorf("Failed creating host %q: %w", hostName, err)
		}

		revert.Add(func() {
			err := client.DeleteHost(hostName)
			if err != nil {
				d.logger.Warn("Failed to cleanup created PowerStore host", logger.Ctx{"err": err, "hostname": hostName})
			}
		})
	} else {
		// Hostname already exists with the given qualified name.
		hostName = host.Name
	}

	cleanup = revert.Clone().Fail
	revert.Success()
	return hostName, cleanup, nil
}

// getMappedDevicePath returns the local device path for the given volume.
// Indicate with mapVolume if the volume should get mapped to the system if it isn't present.
func (d *powerstore) getMappedDevicePath(vol Volume, mapVolume bool) (string, revert.Hook, error) {
	d.logger.Warn("Getting mapped device path", logger.Ctx{"vol": vol.name, "map_volume": mapVolume})

	revert := revert.New()
	defer revert.Fail()

	connector, err := d.connector()
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

	volName, err := d.encodeVolumeName(vol)
	if err != nil {
		return "", nil, err
	}

	psVol, err := d.client().GetVolume(volName)
	if err != nil {
		return "", nil, err
	}

	_, wwn, ok := strings.Cut(psVol.WWN, ".")
	if !ok {
		return "", nil, fmt.Errorf("Failed parsing WWN for volume %q: Invalid format %q", vol.name, psVol.WWN)
	}

	// Filters devices by matching the device path with the WWN.
	devicePathFilter := func(path string) bool {
		return strings.Contains(path, wwn)
	}

	var devicePath string
	if mapVolume {
		// Wait until the disk device is mapped to the host.
		devicePath, err = connector.WaitDiskDevicePath(d.state.ShutdownCtx, devicePathFilter)
	} else {
		// Expect device to be already mapped.
		devicePath, err = connector.GetDiskDevicePath(devicePathFilter)
	}

	if err != nil {
		return "", nil, fmt.Errorf("Failed locating device for volume %q: %w", vol.name, err)
	}

	cleanup := revert.Clone().Fail
	revert.Success()
	return devicePath, cleanup, nil
}

// mapVolume maps the given volume onto this host.
func (d *powerstore) mapVolume(vol Volume) (cleanup revert.Hook, err error) {
	logger.Warn("Mapping volume", logger.Ctx{"vol": vol.name})
	defer logger.Warn("Volume mapped", logger.Ctx{"vol": vol.name})

	client := d.client()

	reverter := revert.New()
	defer reverter.Fail()

	connector, err := d.connector()
	if err != nil {
		return nil, err
	}

	volName, err := d.encodeVolumeName(vol)
	if err != nil {
		return nil, err
	}

	volID, err := client.GetVolumeID(volName)
	if err != nil {
		return nil, fmt.Errorf("Failed to retrieve volume %q: %w", vol.name, err)
	}

	unlock, err := remoteVolumeMapLock(connector.Type(), d.Info().Name)
	if err != nil {
		return nil, err
	}

	defer unlock()

	// Ensure the host exists and is configured with the correct QN.
	hostName, cleanup, err := d.ensureHost()
	if err != nil {
		return nil, err
	}

	reverter.Add(cleanup)

	// Ensure the volume is connected to the host.
	connCreated, err := client.AttachVolumeToHost(volID, hostName)
	if err != nil {
		return nil, fmt.Errorf("Failed to attach volume %q to host %q: %w", vol.name, hostName, err)
	}

	if connCreated {
		reverter.Add(func() { _ = client.DetachVolumeFromHost(volID, hostName) })
	}

	// Find the array's qualified name for the configured mode.
	targets, err := d.getTargets()
	if err != nil {
		return nil, err
	}

	outerReverter := revert.New()
	hasUnmapReverter := false

	// Connect to the array.
	for _, target := range targets {
		logger.Warn("Connecting to target", logger.Ctx{"vol": vol.name, "target": target.QualifiedName, "address": target.Address})
		connReverter, err := connector.Connect(d.state.ShutdownCtx, target.QualifiedName, target.Address)
		if err != nil {
			return nil, err
		}

		// If connect succeeded it means we have at least one established connection.
		// However, it's reverter does not cleanup the establised connections or a newly
		// created session. Therefore, if we created a mapping, add unmapVolume to the
		// returned (outer) reverter. Unmap ensures the target is disconnected only when
		// no other device is using it.
		if connCreated && !hasUnmapReverter {
			outerReverter.Add(func() { _ = d.unmapVolume(vol) })
			hasUnmapReverter = true
		}

		// Add connReverter to the outer reverter, as it will immediately stop
		// any ongoing connection attempts. Note that it must be added after
		// unmapVolume to ensure it is called first.
		outerReverter.Add(connReverter)
		reverter.Add(connReverter)
	}

	reverter.Success()
	return outerReverter.Fail, nil
}

// unmapVolume unmaps the given volume from this host.
func (d *powerstore) unmapVolume(vol Volume) error {
	logger.Warn("Unmapping volume", logger.Ctx{"vol": vol.name})
	defer logger.Warn("Volume unmapped", logger.Ctx{"vol": vol.name})

	client := d.client()

	connector, err := d.connector()
	if err != nil {
		return err
	}

	qn, err := connector.QualifiedName()
	if err != nil {
		return err
	}

	volName, err := d.encodeVolumeName(vol)
	if err != nil {
		return err
	}

	psVol, err := client.GetVolume(volName)
	if err != nil {
		return fmt.Errorf("Failed to retrieve volume %q: %w", vol.name, err)
	}

	unlock, err := remoteVolumeMapLock(connector.Type(), d.Info().Name)
	if err != nil {
		return err
	}

	defer unlock()

	host, err := client.GetCurrentHost(connector.Type(), qn)
	if err != nil {
		return err
	}

	// Get a path of a block device we want to unmap.
	volumePath, _, _ := d.getMappedDevicePath(vol, false)

	// Remove disk device.
	err = connector.RemoveDiskDevice(d.state.ShutdownCtx, volumePath)
	if err != nil {
		return fmt.Errorf("Failed unmapping PowerStore volume %q: %w", vol.name, err)
	}

	// Disconnect the volume from the host and ignore error if connection does not exist.
	err = client.DetachVolumeFromHost(psVol.ID, host.Name)
	if err != nil && !api.StatusErrorCheck(err, http.StatusNotFound) {
		return fmt.Errorf("Failed to detach volume %q from host %q: %w", vol.name, host.Name, err)
	}

	// Wait until the volume has disappeared.
	ctx, cancel := context.WithTimeout(d.state.ShutdownCtx, 30*time.Second)
	defer cancel()

	if volumePath != "" && !block.WaitDiskDeviceGone(ctx, volumePath) {
		return fmt.Errorf("Timeout exceeded waiting for PowerStore volume %q to disappear on path %q", vol.name, volumePath)
	}

	// If this was the last volume being unmapped from this system, disconnect the active session
	// and remove the host from PowerStore.
	if len(host.MappedVolumes) <= 1 {
		targets, err := d.getTargets()
		if err != nil {
			return err
		}

		targetsString := make([]string, len(targets))
		for i, target := range targets {
			targetsString[i] = target.QualifiedName + "[" + target.Address + "]"
		}

		logger.Warn("Disconnect targets", logger.Ctx{"vol": vol.name, "found_targets": strings.Join(targetsString, ","), "host": fmt.Sprintf("%#v", host)})

		var disconnectErr error
		for _, target := range targets {
			// Disconnect from the target.
			err = connector.Disconnect(target.QualifiedName)
			if err != nil {
				disconnectErr = err
			}
		}

		if disconnectErr != nil {
			return fmt.Errorf("Failed disconnecting from targets after unmapping the last volume %q: %w", vol.name, disconnectErr)
		}

		// Remove the host from PowerStore.
		err = d.client().DeleteHost(host.Name)
		if err != nil {
			return err
		}
	}

	return nil
}

// targets return discovered PowerStore targets (their addresses and associated
// qualified names).
func (d *powerstore) getTargets() ([]powerStoreTarget, error) {
	d.logger.Warn("Getting targets")

	if len(d.discoveredTargets) > 0 {
		return d.discoveredTargets, nil
	}

	connector, err := d.connector()
	if err != nil {
		return nil, err
	}

	discoveryAddresses := shared.SplitNTrimSpace(d.config["powerstore.discovery"], ",", -1, true)

	var discoveryLogRecords []any
	for _, addr := range discoveryAddresses {
		discovered, err := connector.Discover(d.state.ShutdownCtx, addr)
		if err != nil {
			// Underlying connector should log a waring.
			continue
		}

		discoveryLogRecords = append(discoveryLogRecords, discovered...)
	}

	if len(discoveryLogRecords) == 0 {
		return nil, errors.New("Failed fetching discovery log records for all PowerStore target addresses")
	}

	userForcedTargetAddresses := shared.SplitNTrimSpace(d.config["powerstore.target"], ",", -1, true)
	parser, err := d.discoveryLogRecordParser(userForcedTargetAddresses)
	if err != nil {
		return nil, err
	}

	discoveredTargets := []powerStoreTarget{}
	for _, record := range discoveryLogRecords {
		target, includeTarget, err := parser(record)
		if err != nil {
			return nil, err
		}

		if !includeTarget {
			continue
		}

		discoveredTargets = append(discoveredTargets, *target)
	}

	discoveredTargets = shared.Unique(discoveredTargets)

	if len(discoveredTargets) == 0 {
		return nil, errors.New("Failed fetching a discovery log record from any of the discovery addresses")
	}

	d.discoveredTargets = discoveredTargets
	return d.discoveredTargets, nil
}

// discoveryLogRecordParser returns a parsing function that converts single
// discovery log entry to target.
func (d *powerstore) discoveryLogRecordParser(filterTargetAddresses []string) (func(any) (*powerStoreTarget, bool, error), error) {
	transport := d.config["powerstore.transport"]
	if transport != powerStoreTransportTCP {
		return nil, fmt.Errorf("Unsupported transport %q in PowerStore configuration", transport)
	}

	mode := d.config["powerstore.mode"]
	switch mode {
	case powerStoreModeISCSI:
		filterTargetAddresses = slices.Clone(filterTargetAddresses)
		for i := range filterTargetAddresses {
			filterTargetAddresses[i] = shared.EnsurePort(filterTargetAddresses[i], connectors.ISCSIDefaultPort)
		}

		return func(record any) (*powerStoreTarget, bool, error) {
			r, ok := record.(connectors.ISCSIDiscoveryLogRecord)
			if !ok {
				return nil, false, fmt.Errorf("Invalid discovery log record entry type %T", record)
			}

			target := powerStoreTarget{
				Address:       r.Address,
				QualifiedName: r.IQN,
			}

			if len(filterTargetAddresses) > 0 && !slices.Contains(filterTargetAddresses, target.Address) {
				return nil, false, nil
			}

			return &target, true, nil
		}, nil

	case powerStoreModeNVME:
		filterTargetAddresses = slices.Clone(filterTargetAddresses)
		for i := range filterTargetAddresses {
			filterTargetAddresses[i] = shared.EnsurePort(filterTargetAddresses[i], connectors.NVMeDefaultTransportPort)
		}

		return func(record any) (*powerStoreTarget, bool, error) {
			r, ok := record.(connectors.NVMeDiscoveryLogRecord)
			if !ok {
				return nil, false, fmt.Errorf("Invalid discovery log record entry type %T", record)
			}

			target := powerStoreTarget{
				Address:       net.JoinHostPort(r.TransportAddress, r.TransportServiceIdentifier),
				QualifiedName: r.SubNQN,
			}

			if len(filterTargetAddresses) > 0 && !slices.Contains(filterTargetAddresses, target.Address) {
				return nil, false, nil
			}

			return &target, true, nil
		}, nil
	default:
		return nil, fmt.Errorf("Unsupported PowerStore mode %q", mode)
	}
}

// CreateVolumeSnapshot creates a snapshot of a volume.
func (d *powerstore) CreateVolumeSnapshot(snapVol Volume, op *operations.Operation) error {
	return d.createVolumeSnapshot(snapVol, true, op)
}

// createVolumeSnapshot creates a snapshot of a volume. If snapshotVMfilesystem is false, a VM's filesystem volume
// is not copied.
func (d *powerstore) createVolumeSnapshot(snapVol Volume, snapshotVMfilesystem bool, op *operations.Operation) error {
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

	volName, err := d.encodeVolumeName(snapVol.GetParent())
	if err != nil {
		return err
	}

	volID, err := d.client().GetVolumeID(volName)
	if err != nil {
		return fmt.Errorf("Failed to retrieve volume %q: %w", snapVol.GetParent().name, err)
	}

	snapVolName, err := d.encodeVolumeName(snapVol)
	if err != nil {
		return err
	}

	err = d.client().CreateVolumeSnapshot(volID, snapVolName)
	if err != nil {
		return fmt.Errorf("Failed to create snapshot %q for volume %q: %w", snapVol.name, snapVol.GetParent().name, err)
	}

	revert.Add(func() { _ = d.DeleteVolumeSnapshot(snapVol, op) })

	// For VMs, create a snapshot of the filesystem volume too.
	// Skip if snapshotVMfilesystem is false to prevent overwriting separately copied volumes.
	if snapVol.IsVMBlock() && snapshotVMfilesystem {
		fsVol := snapVol.NewVMBlockFilesystemVolume()

		// Set the parent volume's UUID.
		fsVol.SetParentUUID(snapVol.parentUUID)

		err := d.CreateVolumeSnapshot(fsVol, op)
		if err != nil {
			return fmt.Errorf("Failed to create snapshot %q for volume %q: %w", fsVol.name, fsVol.GetParent().name, err)
		}

		revert.Add(func() { _ = d.DeleteVolumeSnapshot(fsVol, op) })
	}

	revert.Success()
	return nil
}

// DeleteVolumeSnapshot removes a snapshot from the storage device.
func (d *powerstore) DeleteVolumeSnapshot(snapVol Volume, op *operations.Operation) error {
	client := d.client()

	parentVol := snapVol.GetParent()
	parentVolName, err := d.encodeVolumeName(parentVol)
	if err != nil {
		return err
	}

	parentVolID, err := client.GetVolumeID(parentVolName)
	if err != nil {
		return err
	}

	snapVolName, err := d.encodeVolumeName(snapVol)
	if err != nil {
		return err
	}

	snapVolID, err := client.GetVolumeSnapshotID(parentVolID, snapVolName)
	if err != nil {
		return err
	}

	err = client.DeleteVolumeSnapshot(snapVolID)
	if err != nil {
		return err
	}

	if snapVol.contentType == ContentTypeFS {
		mountPath := snapVol.MountPath()

		err = wipeDirectory(mountPath)
		if err != nil {
			return err
		}

		err = os.Remove(mountPath)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("Failed removing %q: %w", mountPath, err)
		}
	}

	// For VM images, delete the filesystem volume too.
	if snapVol.IsVMBlock() {
		fsVol := snapVol.NewVMBlockFilesystemVolume()
		fsVol.SetParentUUID(snapVol.parentUUID)

		err := d.DeleteVolumeSnapshot(fsVol, op)
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
	client := d.client()

	volName, err := d.encodeVolumeName(vol)
	if err != nil {
		return nil, err
	}

	// volID, err := client.GetVolumeID(volName)
	// if err != nil {
	// 	return nil, err
	// }

	snapshots, err := client.GetVolumeSnapshots(volName)
	if err != nil {
		return nil, err
	}

	snapshotNames := make([]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		snapshotNames = append(snapshotNames, snapshot.Name)
	}

	return snapshotNames, nil
}

// MountVolumeSnapshot mounts a storage volume snapshot.
func (d *powerstore) MountVolumeSnapshot(snapVol Volume, op *operations.Operation) error {
	return d.MountVolume(snapVol, op)
}

// UnmountVolumeSnapshot unmounts a storage volume snapshot, returns true if unmounted,
// false if was not mounted.
func (d *powerstore) UnmountVolumeSnapshot(snapVol Volume, op *operations.Operation) (bool, error) {
	return d.UnmountVolume(snapVol, false, op)
}
