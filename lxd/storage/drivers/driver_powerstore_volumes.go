package drivers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
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
	"github.com/canonical/lxd/lxd/instancewriter"
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

const (
	powerStoreResourcePrefix = "lxd-" // common prefix for all resource names in PowerStore.
	powerStoreVolNameSep     = "-"    // separates pool name and volume data in encoded volume names.
	powerStoreVolPrefixSep   = "_"    // volume name prefix separator
	powerStoreVolSuffixSep   = "."    // volume name suffix separator
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

	if vol.IsSnapshot() {
		// Snapshots cannot be attached directly. The [MountVolumeSnapshot] maps a
		// temporary clone, therefore, search for the device path of a snapshot clone.
		cloneVol, err := d.newMountableSnapshotVolume(vol)
		if err != nil {
			return "", err
		}

		vol = cloneVol
	}

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

	volID, err := d.getVolumeID(vol)
	if err != nil {
		return -1, err
	}

	psVol, err := d.client().GetVolume(volID)
	if err != nil {
		return -1, err
	}

	return psVol.LogicalUsed, nil
}

// HasVolume indicates whether a specific volume exists on the storage pool.
func (d *powerstore) HasVolume(vol Volume) (bool, error) {
	d.logger.Warn("Checking if volume exists", logger.Ctx{"vol": vol.name})

	// Try to retrieve ID of the remote volume. If it succeeds, the volume exists.
	_, err := d.getVolumeID(vol)
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

	sizeBytes, err := units.ParseByteSizeString(vol.ConfigSize())
	if err != nil {
		return err
	}

	sizeBytes = d.roundVolumeBlockSizeBytes(vol, sizeBytes)

	volName, err := d.encodeVolumeName(vol)
	if err != nil {
		return err
	}

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

	srcVolID, err := d.getVolumeID(srcVol.Volume)
	if err != nil {
		return err
	}

	// Since snapshots are first copied into destination volume from which a new snapshot is created,
	// we need to also remove the destination volume if an error occurs during copying of snapshots.
	deleteVolCopy := true

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

			srcSnapshotID, err := d.getVolumeID(*srcSnapshot)
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
	if srcVol.IsSnapshot() || volID == "" {
		// Copy the source volume/snapshot into destination volume.
		_, err = client.CloneVolume(srcVolID, volName)
		if err != nil {
			return err
		}
	} else {
		// Destination volume already exists, so refresh it.
		err = client.RefreshVolume(srcVolID, volID)
		if err != nil {
			return err
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

	volID, err := d.getVolumeID(vol)
	if err != nil {
		return err
	}

	host, err := client.GetCurrentHost(connector.Type(), qn)
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
		err = client.DetachVolumeFromHost(volID, host.ID)
		if err != nil && !api.StatusErrorCheck(err, http.StatusNotFound) {
			return fmt.Errorf("Failed to detach volume %q from host %q: %w", vol.name, host.Name, err)
		}
	}

	err = client.DeleteVolume(volID)
	if err != nil {
		return fmt.Errorf("Failed to delete volume %q: %w", vol.name, err)
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

// RenameVolume renames a volume and its snapshots.
func (d *powerstore) RenameVolume(vol Volume, newVolName string, op *operations.Operation) error {
	d.logger.Warn("Renaming volume", logger.Ctx{"vol": vol.name, "new_name": newVolName})
	// Renaming a volume in PowerStore will not change the name of the associated volume resource.
	return nil
}

// BackupVolume creates an exported version of a volume.
func (d *powerstore) BackupVolume(vol VolumeCopy, projectName string, tarWriter *instancewriter.InstanceTarWriter, optimized bool, snapshots []string, op *operations.Operation) error {
	return genericVFSBackupVolume(d, vol, tarWriter, snapshots, op)
}

// MigrateVolume sends a volume for migration.
func (d *powerstore) MigrateVolume(vol VolumeCopy, conn io.ReadWriteCloser, volSrcArgs *migration.VolumeSourceArgs, op *operations.Operation) error {
	// When performing a cluster member move don't do anything on the source member.
	if volSrcArgs.ClusterMove {
		return nil
	}

	return genericVFSMigrateVolume(d, d.state, vol, conn, volSrcArgs, op)
}

// RestoreVolume restores a volume from a snapshot.
func (d *powerstore) RestoreVolume(vol Volume, snapVol Volume, op *operations.Operation) error {
	client := d.client()

	ourUnmount, err := d.UnmountVolume(vol, false, op)
	if err != nil {
		return err
	}

	if ourUnmount {
		defer func() { _ = d.MountVolume(vol, op) }()
	}

	volID, err := d.getVolumeID(vol)
	if err != nil {
		return err
	}

	snapVolID, err := d.getVolumeID(snapVol)
	if err != nil {
		return err
	}

	// Overwrite existing volume by copying the given snapshot content into it.
	err = client.RestoreVolume(volID, snapVolID)
	if err != nil {
		return err
	}

	// For VMs, also restore the filesystem volume.
	if vol.IsVMBlock() {
		fsVol := vol.NewVMBlockFilesystemVolume()

		snapFSVol := snapVol.NewVMBlockFilesystemVolume()
		snapFSVol.SetParentUUID(snapVol.parentUUID)

		err := d.RestoreVolume(fsVol, snapFSVol, op)
		if err != nil {
			return err
		}
	}

	return nil
}

// RefreshVolume updates an existing volume to match the state of another.
func (d *powerstore) RefreshVolume(vol VolumeCopy, srcVol VolumeCopy, refreshSnapshots []string, allowInconsistent bool, op *operations.Operation) error {
	revert := revert.New()
	defer revert.Fail()

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

		cleanup, err := d.refreshVolume(fsVol, srcFSVol, refreshSnapshots, allowInconsistent, op)
		if err != nil {
			return err
		}

		revert.Add(cleanup)
	}

	cleanup, err := d.refreshVolume(vol, srcVol, refreshSnapshots, allowInconsistent, op)
	if err != nil {
		return err
	}

	revert.Add(cleanup)

	revert.Success()
	return nil
}

// refreshVolume updates an existing volume to match the state of another. For VMs, this function
// refreshes either block or filesystem volume, depending on the volume type. Therefore, the caller
// needs to ensure it is called twice - once for each volume type.
func (d *powerstore) refreshVolume(vol VolumeCopy, srcVol VolumeCopy, refreshSnapshots []string, allowInconsistent bool, op *operations.Operation) (revert.Hook, error) {
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
		err := d.SetVolumeQuota(vol.Volume, vol.ConfigSize(), false, op)
		if err != nil {
			return err
		}

		return nil
	}

	srcVolID, err := d.getVolumeID(srcVol.Volume)
	if err != nil {
		return nil, err
	}

	volID, err := d.getVolumeID(vol.Volume)
	if err != nil {
		return nil, err
	}

	// Create new reverter snapshot, which is used to revert the original volume in case of
	// an error. Snapshots are also required to be first copied into destination volume,
	// from which a new snapshot is created to effectively copy a snapshot. If any error
	// occurs, the destination volume has been already modified and needs reverting.
	reverterSnapshotName := "lxd-reverter-snapshot"

	// Remove existing reverter snapshot.
	reverterSnapshotID, err := client.GetVolumeSnapshotID(volID, reverterSnapshotName)
	if err != nil && !api.StatusErrorCheck(err, http.StatusNotFound) {
		return nil, err
	}

	if reverterSnapshotID != "" {
		err = client.DeleteVolumeSnapshot(reverterSnapshotID)
		if err != nil && !api.StatusErrorCheck(err, http.StatusNotFound) {
			return nil, err
		}
	}

	// Create new reverter snapshot.
	reverterSnapshotID, err = client.CreateVolumeSnapshot(volID, reverterSnapshotName)
	if err != nil {
		return nil, err
	}

	revert.Add(func() {
		// Restore destination volume from reverter snapshot and remove the snapshot afterwards.
		_ = client.RestoreVolume(volID, reverterSnapshotID)
		_ = client.DeleteVolumeSnapshot(reverterSnapshotID)
	})

	if !srcVol.IsSnapshot() && len(refreshSnapshots) > 0 {
		var refreshedSnapshots []string

		// Refresh volume snapshots.
		// Pure Storage does not allow copying snapshots along with the volume. Therefore,
		// we copy the missing snapshots sequentially. Each snapshot is first copied into
		// destination volume from which a new snapshot is created. The process is repeted
		// until all of the missing snapshots are copied.
		for _, snapshot := range vol.Snapshots {
			// Remove volume name prefix from the snapshot name, and check whether it
			// has to be refreshed.
			_, snapshotShortName, _ := api.GetParentAndSnapshotName(snapshot.name)
			if !slices.Contains(refreshSnapshots, snapshotShortName) {
				// Skip snapshot if it doesn't have to be refreshed.
				continue
			}

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
				return nil, fmt.Errorf("Failed refreshing snapshot %q: Source snapshot does not exist", snapshotShortName)
			}

			srcSnapshotID, err := d.getVolumeID(*srcSnapshot)
			if err != nil {
				return nil, err
			}

			// Overwrite existing destination volume with snapshot.
			err = client.RefreshVolume(srcSnapshotID, volID)
			if err != nil {
				return nil, err
			}

			// Set snapshot's parent UUID.
			snapshot.SetParentUUID(vol.config["volatile.uuid"])

			// Create snapshot of a new volume. Do not copy VM's filesystem volume snapshot,
			// as FS volumes are already copied by this point.
			err = d.createVolumeSnapshot(snapshot, false, op)
			if err != nil {
				return nil, err
			}

			revert.Add(func() { _ = d.DeleteVolumeSnapshot(snapshot, op) })

			// Append snapshot to the list of successfully refreshed snapshots.
			refreshedSnapshots = append(refreshedSnapshots, snapshotShortName)
		}

		// Ensure all snapshots were successfully refreshed.
		missing := shared.RemoveElementsFromSlice(refreshSnapshots, refreshedSnapshots...)
		if len(missing) > 0 {
			return nil, fmt.Errorf("Failed refreshing snapshots %v", missing)
		}
	}

	// Finally, copy the source volume (or snapshot) into destination volume snapshots.
	err = client.RefreshVolume(srcVolID, volID)
	if err != nil {
		return nil, err
	}

	err = postCreateTasks(vol.Volume)
	if err != nil {
		return nil, err
	}

	cleanup := revert.Clone().Fail
	revert.Success()

	// Remove temporary reverter snapshot.
	_ = client.DeleteVolumeSnapshot(reverterSnapshotID)

	return cleanup, err
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

	volID, err := d.getVolumeID(vol)
	if err != nil {
		return err
	}

	psVol, err := client.GetVolume(volID)
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

// CreateVolumeSnapshot creates a snapshot of a volume.
func (d *powerstore) CreateVolumeSnapshot(snapVol Volume, op *operations.Operation) error {
	return d.createVolumeSnapshot(snapVol, true, op)
}

// createVolumeSnapshot creates a snapshot of a volume. If snapshotVMfilesystem is false, a VM's filesystem volume
// is not copied.
func (d *powerstore) createVolumeSnapshot(snapVol Volume, snapshotVMfilesystem bool, op *operations.Operation) error {
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

	volID, err := d.getVolumeID(snapVol.GetParent())
	if err != nil {
		return err
	}

	snapVolName, err := d.encodeVolumeName(snapVol)
	if err != nil {
		return err
	}

	_, err = client.CreateVolumeSnapshot(volID, snapVolName)
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

	snapVolID, err := d.getVolumeID(snapVol)
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

	volID, err := d.getVolumeID(vol)
	if err != nil {
		return nil, err
	}

	snapshots, err := client.GetVolumeSnapshots(volID)
	if err != nil {
		return nil, err
	}

	snapshotNames := make([]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		snapshotNames = append(snapshotNames, snapshot.Name)
	}

	return snapshotNames, nil
}

// newMountableSnapshotVolume returns a non-snapshot Volume representing the temporary clone
// of snapVol which is used when snapshot needs to be mounted (mounting snapshot directly is not
// supported by PowerStore). The cloned volume UUID is derived deterministically from the snapshot
// UUID so that UnmountVolumeSnapshot can reconstruct it without extra state.
func (d *powerstore) newMountableSnapshotVolume(snapVol Volume) (Volume, error) {
	snapUUID, err := uuid.Parse(snapVol.config["volatile.uuid"])
	if err != nil {
		return Volume{}, fmt.Errorf(`Failed parsing "volatile.uuid" from snapshot %q: %w`, snapVol.name, err)
	}

	// UUID v5: deterministic, derived from the snapshot UUID as namespace.
	cloneUUID := uuid.NewSHA1(snapUUID, []byte("snapshot-mount-clone"))

	cloneConfig := make(map[string]string, len(snapVol.config))
	maps.Copy(cloneConfig, snapVol.config)
	cloneConfig["volatile.uuid"] = cloneUUID.String()

	// Use the clone UUID as the name to avoid colliding with the parent volume's mount
	// ref-count. Name encoding uses volatile.uuid anyway, so the name only affects local
	// state.
	return NewVolume(d, d.name, snapVol.volType, snapVol.contentType, cloneUUID.String(), cloneConfig, d.config), nil
}

// MountVolumeSnapshot creates a temporary clone of the snapshot to allow mounting it.
// PowerStore snapshots cannot be attached to hosts directly.
func (d *powerstore) MountVolumeSnapshot(snapVol Volume, op *operations.Operation) error {
	revert := revert.New()
	defer revert.Fail()

	client := d.client()

	snapVolID, err := d.getVolumeID(snapVol)
	if err != nil {
		return err
	}

	cloneVol, err := d.newMountableSnapshotVolume(snapVol)
	if err != nil {
		return err
	}

	// Reuse an existing clone in case a previous unmount failed to clean up.
	_, err = d.getVolumeID(cloneVol)
	if api.StatusErrorCheck(err, http.StatusNotFound) {
		cloneVolName, err := d.encodeVolumeName(cloneVol)
		if err != nil {
			return err
		}

		_, err = client.CloneVolume(snapVolID, cloneVolName)
		if err != nil {
			return fmt.Errorf("Failed creating temporary clone for snapshot %q: %w", snapVol.name, err)
		}

		revert.Add(func() {
			cloneID, err := d.getVolumeID(cloneVol)
			if err == nil {
				_ = client.DeleteVolume(cloneID)
			}
		})
	} else if err != nil {
		return fmt.Errorf("Failed checking for existing clone of snapshot %q: %w", snapVol.name, err)
	}

	// For VMs, also clone the filesystem snapshot. cloneVol.NewVMBlockFilesystemVolume()
	// propagates cloneUUID automatically, so no manual UUID derivation is needed.
	if snapVol.IsVMBlock() {
		snapFsVol := snapVol.NewVMBlockFilesystemVolume()
		snapFsVol.SetParentUUID(snapVol.parentUUID)

		cloneFsVol := cloneVol.NewVMBlockFilesystemVolume()
		cloneFsVolName, err := d.encodeVolumeName(cloneFsVol)
		if err != nil {
			return err
		}

		snapFsID, err := d.getVolumeID(snapFsVol)
		if err != nil {
			return err
		}

		_, err = d.getVolumeID(cloneFsVol)
		if api.StatusErrorCheck(err, http.StatusNotFound) {
			_, err = client.CloneVolume(snapFsID, cloneFsVolName)
			if err != nil {
				return fmt.Errorf("Failed creating temporary clone for filesystem snapshot %q: %w", snapFsVol.name, err)
			}

			revert.Add(func() {
				cloneID, err := d.getVolumeID(cloneFsVol)
				if err == nil {
					_ = client.DeleteVolume(cloneID)
				}
			})
		} else if err != nil {
			return fmt.Errorf("Failed checking for existing clone of filesystem snapshot %q: %w", snapFsVol.name, err)
		}
	}

	err = d.MountVolume(cloneVol, op)
	if err != nil {
		return err
	}

	revert.Success()
	return nil
}

// UnmountVolumeSnapshot unmounts the temporary clone that was created by MountVolumeSnapshot
// and deletes it from PowerStore.
func (d *powerstore) UnmountVolumeSnapshot(snapVol Volume, op *operations.Operation) (bool, error) {
	client := d.client()

	cloneVol, err := d.newMountableSnapshotVolume(snapVol)
	if err != nil {
		return false, err
	}

	ourUnmount, err := d.UnmountVolume(cloneVol, false, op)
	if err != nil {
		return false, err
	}

	if !ourUnmount {
		return false, nil
	}

	cloneID, err := d.getVolumeID(cloneVol)
	if err != nil && !api.StatusErrorCheck(err, http.StatusNotFound) {
		return true, fmt.Errorf("Failed finding temporary clone for snapshot %q: %w", snapVol.name, err)
	}

	if err == nil {
		err = client.DeleteVolume(cloneID)
		if err != nil && !api.StatusErrorCheck(err, http.StatusNotFound) {
			return true, fmt.Errorf("Failed deleting temporary clone for snapshot %q: %w", snapVol.name, err)
		}
	}

	// For VMs, also delete the filesystem clone.
	if snapVol.IsVMBlock() {
		cloneFsVol := cloneVol.NewVMBlockFilesystemVolume()

		fsCloneID, err := d.getVolumeID(cloneFsVol)
		if err != nil && !api.StatusErrorCheck(err, http.StatusNotFound) {
			return true, fmt.Errorf("Failed finding temporary clone for filesystem snapshot %q: %w", cloneFsVol.name, err)
		}

		if err == nil {
			err = client.DeleteVolume(fsCloneID)
			if err != nil && !api.StatusErrorCheck(err, http.StatusNotFound) {
				return true, fmt.Errorf("Failed deleting temporary clone for filesystem snapshot %q: %w", cloneFsVol.name, err)
			}
		}
	}

	return ourUnmount, nil
}

// roundVolumeBlockSizeBytes rounds the given size (in bytes) up to the next
// multiple of 1 MiB, which is the minimum volume size on PowerStore.
func (d *powerstore) roundVolumeBlockSizeBytes(_ Volume, sizeBytes int64) int64 {
	return roundAbove(powerStoreMinVolumeSizeBytes, sizeBytes)
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
	poolName, volName, ok := strings.Cut(poolAndVolName, powerStoreVolNameSep)
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

// getVolumeID returns the PowerStore ID for the given LXD volume or snapshot.
// For snapshots, it resolves the parent volume first and then fetches the snapshot by name.
func (d *powerstore) getVolumeID(vol Volume) (string, error) {
	client := d.client()

	volName, err := d.encodeVolumeName(vol)
	if err != nil {
		return "", err
	}

	if vol.IsSnapshot() {
		parentVol := vol.GetParent()
		parentVolName, err := d.encodeVolumeName(parentVol)
		if err != nil {
			return "", err
		}

		parentVolID, err := client.GetVolumeID(parentVolName)
		if err != nil {
			return "", fmt.Errorf("Failed to retrieve remote storage ID for snapshot parent volume %q: %w", parentVol.name, err)
		}

		snapshotID, err := client.GetVolumeSnapshotID(parentVolID, volName)
		if err != nil {
			return "", fmt.Errorf("Failed to retrieve remote storage ID for snapshot %q: %w", vol.name, err)
		}

		return snapshotID, nil
	}

	volID, err := client.GetVolumeID(volName)
	if err != nil {
		return "", fmt.Errorf("Failed to retrieve remote storage ID for volume %q: %w", vol.name, err)
	}

	return volID, nil
}

// ensureHost returns a name of the host that is configured with a given IQN. If such host
// does not exist, a new one is created, where host's name equals to the server name with a
// mode included.
func (d *powerstore) ensureHost() (hostID string, cleanup revert.Hook, err error) {
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
		hostname := serverName + "-" + connector.Type()

		hostID, err = client.CreateHost(hostname, connector.Type(), qn)
		if err != nil {
			return "", nil, fmt.Errorf("Failed creating host %q: %w", hostname, err)
		}

		revert.Add(func() { _ = client.DeleteHost(hostID) })
	} else {
		// Hostname already exists with the given qualified name.
		hostID = host.ID
	}

	cleanup = revert.Clone().Fail
	revert.Success()
	return hostID, cleanup, nil
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

	volID, err := d.getVolumeID(vol)
	if err != nil {
		return "", nil, err
	}

	psVol, err := d.client().GetVolume(volID)
	if err != nil {
		return "", nil, fmt.Errorf("Failed to retrieve volume %q: %w", vol.name, err)
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

	volID, err := d.getVolumeID(vol)
	if err != nil {
		return nil, err
	}

	unlock, err := remoteVolumeMapLock(connector.Type(), d.Info().Name)
	if err != nil {
		return nil, err
	}

	defer unlock()

	// Ensure the host exists and is configured with the correct QN.
	hostID, cleanup, err := d.ensureHost()
	if err != nil {
		return nil, err
	}

	reverter.Add(cleanup)

	// Ensure the volume is connected to the host.
	connCreated, err := client.AttachVolumeToHost(volID, hostID)
	if err != nil {
		return nil, fmt.Errorf("Failed to attach volume %q to host: %w", vol.name, err)
	}

	if connCreated {
		reverter.Add(func() { _ = client.DetachVolumeFromHost(volID, hostID) })
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

	volID, err := d.getVolumeID(vol)
	if err != nil {
		return err
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
	err = client.DetachVolumeFromHost(volID, host.ID)
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
		err = d.client().DeleteHost(host.ID)
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

	targetAddresses, err := d.client().GetTargetAddresses(connector.Type())
	if err != nil {
		return nil, err
	}

	var discoveryLogRecords []any
	for _, addr := range targetAddresses {
		discovered, err := connector.Discover(d.state.ShutdownCtx, addr)
		if err != nil {
			// Underlying connector already logs a warning.
			continue
		}

		discoveryLogRecords = append(discoveryLogRecords, discovered...)
	}

	if len(discoveryLogRecords) == 0 {
		return nil, errors.New("Failed fetching discovery log records for all PowerStore target addresses")
	}

	// Parse user-specified list of addresses to connect to.
	// If not specified, all discovered addresses will be used.
	filterAddresses := shared.SplitNTrimSpace(d.config["powerstore.target"], ",", -1, true)
	if len(filterAddresses) > 0 {
		// Make sure target addresses have port configured.
		var defaultPort string
		mode := connector.Type()
		switch mode {
		case powerStoreModeISCSI:
			defaultPort = connectors.ISCSIDefaultPort
		case powerStoreModeNVME:
			defaultPort = connectors.NVMeDefaultDiscoveryPort
		default:
			return nil, fmt.Errorf("Unsupported PowerStore mode %q", mode)
		}

		for i := range filterAddresses {
			filterAddresses[i] = shared.EnsurePort(filterAddresses[i], defaultPort)
		}
	}

	// Helper function to extract address and qualified name from a discovery log record,
	// which differs based on the connector type.
	parseLogEntry := func(record any) (address string, qn string, err error) {
		switch r := record.(type) {
		case connectors.ISCSIDiscoveryLogRecord:
			address = r.Address
			qn = r.IQN
		case connectors.NVMeDiscoveryLogRecord:
			address = net.JoinHostPort(r.TransportAddress, r.TransportServiceIdentifier)
			qn = r.SubNQN
		default:
			return "", "", fmt.Errorf("Unknown discovery log record entry type %T", record)
		}

		return address, qn, nil
	}

	// Parse discovered targets and filter them by user-specified addresses if needed.
	discoveredTargets := []powerStoreTarget{}
	for _, record := range discoveryLogRecords {
		address, qn, err := parseLogEntry(record)
		if err != nil {
			return nil, err
		}

		if len(filterAddresses) > 0 && !slices.Contains(filterAddresses, address) {
			continue
		}

		discoveredTargets = append(discoveredTargets, powerStoreTarget{
			Address:       address,
			QualifiedName: qn,
		})
	}

	if len(discoveredTargets) == 0 {
		return nil, errors.New("Failed fetching a discovery log record from any of the discovery addresses")
	}

	// Cache discovered targets for future use and return them.
	d.discoveredTargets = shared.Unique(discoveredTargets)
	return d.discoveredTargets, nil
}
