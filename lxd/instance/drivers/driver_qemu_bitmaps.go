package drivers

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"

	"github.com/canonical/lxd/lxd/backup"
	"github.com/canonical/lxd/lxd/db"
	dbCluster "github.com/canonical/lxd/lxd/db/cluster"
	"github.com/canonical/lxd/lxd/db/warningtype"
	deviceConfig "github.com/canonical/lxd/lxd/device/config"
	"github.com/canonical/lxd/lxd/device/filters"
	"github.com/canonical/lxd/lxd/instance"
	"github.com/canonical/lxd/lxd/instance/drivers/qmp"
	"github.com/canonical/lxd/lxd/project"
	storagePools "github.com/canonical/lxd/lxd/storage"
	storageDrivers "github.com/canonical/lxd/lxd/storage/drivers"
	"github.com/canonical/lxd/lxd/warnings"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/entity"
	"github.com/canonical/lxd/shared/features"
	"github.com/canonical/lxd/shared/logger"
	"github.com/canonical/lxd/shared/revert"
)

// qemuDataNodePrefix used as part of the name given the QEMU block node of the block volume of a disk that is opened
// through its volume metadata image. The disk node keeps the name of the device, so that the guest device and the
// throttling, statistics and detach code address the same node whether or not the disk has an image. Device names
// may contain underscores, so a suffix on qemuDeviceNamePrefix would collide with a device named <name>_data. No
// block node of a device starts with this prefix.
const qemuDataNodePrefix = "lxddata_"

// qemuOverlayNodePrefix used as part of the name given the QEMU block nodes of overlays. Like qemuDataNodePrefix it
// cannot be a suffix on qemuDeviceNamePrefix.
const qemuOverlayNodePrefix = "lxdoverlay_"

// qemuMetadataImageNodePrefix used as part of the name given the QEMU block nodes of snapshot metadata images.
const qemuMetadataImageNodePrefix = "lxdmeta_"

// qemuImageFDSetPrefix used as part of the name of the fd set that passes the volume metadata image of a disk to
// QEMU. The fd set of the block volume of the disk is named after the disk node.
const qemuImageFDSetPrefix = "lxdimage_"

// blockNodeName returns the QEMU block node name of a disk device, which the guest device is attached to.
func blockNodeName(deviceName string) string {
	return qemuDeviceNameOrID(qemuDeviceNamePrefix, deviceName, "", qemuDeviceNameMaxLength)
}

// dataNodeName returns the QEMU block node name of the block volume of a disk device that is opened through its
// volume metadata image.
func dataNodeName(deviceName string) string {
	return qemuDeviceNameOrID(qemuDataNodePrefix, deviceName, "", qemuDeviceNameMaxLength)
}

// overlayNodeName returns the QEMU block node name of the overlay of a disk device.
func overlayNodeName(deviceName string) string {
	return qemuDeviceNameOrID(qemuOverlayNodePrefix, deviceName, "", qemuDeviceNameMaxLength)
}

// metadataImageNodeName returns the QEMU block node name of the snapshot metadata image of a disk device.
func metadataImageNodeName(deviceName string) string {
	return qemuDeviceNameOrID(qemuMetadataImageNodePrefix, deviceName, "", qemuDeviceNameMaxLength)
}

// imageFDSetName returns the name of the fd set that passes the volume metadata image of a disk device to QEMU.
func imageFDSetName(deviceName string) string {
	return qemuDeviceNameOrID(qemuImageFDSetPrefix, deviceName, "", qemuDeviceNameMaxLength)
}

// qemuMetadataImagesDir is the directory on the config volume that stores the metadata images, the qcow2 images
// that store the bitmaps of the block volumes of the instance. The volume metadata image of a volume is named after
// the UUID of the volume, and the snapshot metadata image of a volume snapshot after the UUIDs of the volume and of
// the snapshot. A snapshot metadata image is created on the config volume before the storage snapshot, so that the
// config volume snapshot includes it, and removed from the config volume afterwards.
const qemuMetadataImagesDir = "metadata_images"

// qemuMetadataImageSuffix is the file name suffix of the metadata images.
const qemuMetadataImageSuffix = ".qcow2"

// qemuOverlaySuffix is the file name suffix of the overlay of a volume, which is stored next to its metadata image.
const qemuOverlaySuffix = ".overlay.qcow2"

// metadataImagesDir returns the directory on the config volume that stores the metadata images.
func (d *qemu) metadataImagesDir() string {
	return filepath.Join(d.Path(), qemuMetadataImagesDir)
}

// volumeMetadataImagePath returns the volume metadata image of a volume, which QEMU opens the volume through and
// which stores the bitmaps of the volume.
func (d *qemu) volumeMetadataImagePath(volumeUUID string) string {
	return filepath.Join(d.metadataImagesDir(), volumeUUID+qemuMetadataImageSuffix)
}

// overlayPath returns the overlay that the guest's writes to a volume go to while a snapshot with a bitmap is
// created. It stays on the config volume until it is committed into the volume.
func (d *qemu) overlayPath(volumeUUID string) string {
	return filepath.Join(d.metadataImagesDir(), volumeUUID+qemuOverlaySuffix)
}

// snapshotMetadataImagePath returns the snapshot metadata image that stores the bitmaps of a volume as they were
// when the volume snapshot of the given UUID was created.
func (d *qemu) snapshotMetadataImagePath(volumeUUID string, snapshotUUID string) string {
	return filepath.Join(d.metadataImagesDir(), volumeUUID+"."+snapshotUUID+qemuMetadataImageSuffix)
}

// parseSnapshotMetadataImageName returns the UUIDs of the volume and of the volume snapshot that a snapshot
// metadata image is named after. It returns false for any other file of the metadata images directory.
func parseSnapshotMetadataImageName(fileName string) (volumeUUID string, snapshotUUID string, ok bool) {
	name, found := strings.CutSuffix(fileName, qemuMetadataImageSuffix)
	if !found {
		return "", "", false
	}

	volumeUUID, snapshotUUID, found = strings.Cut(name, ".")
	if !found || uuid.Validate(volumeUUID) != nil || uuid.Validate(snapshotUUID) != nil {
		return "", "", false
	}

	return volumeUUID, snapshotUUID, true
}

// metadataImagesEnabled reports whether the block disks of the instance are opened through their volume metadata
// images. A live migration target starts without them, because the images on its config volume are the ones of the
// source, which still has them open, and it removes them once the migration is complete.
func (d *qemu) metadataImagesEnabled() bool {
	return features.IsEnabled(features.ChangedBlockTracking) && d.migrationReceiveStateful == nil
}

// withInstanceMounted runs task with the volumes of the instance, or of the instance snapshot, mounted. The config
// volume stores the metadata images and is otherwise mounted only while the instance runs. The mount info gives the
// block device of the root volume.
func (d *qemu) withInstanceMounted(task func(mountInfo *storagePools.MountInfo) error) error {
	pool, err := d.getStoragePool()
	if err != nil {
		return err
	}

	if d.IsSnapshot() {
		mountInfo, err := pool.MountInstanceSnapshot(d, nil)
		if err != nil {
			return err
		}

		defer func() {
			err := pool.UnmountInstanceSnapshot(d, nil)
			if err != nil {
				d.logger.Warn("Failed unmounting config volume of snapshot", logger.Ctx{"err": err})
			}
		}()

		return task(mountInfo)
	}

	mountInfo, err := pool.MountInstance(d, nil)
	if err != nil {
		return err
	}

	defer func() {
		err := pool.UnmountInstance(d, nil)
		if err != nil && !errors.Is(err, storageDrivers.ErrInUse) {
			d.logger.Warn("Failed unmounting config volume", logger.Ctx{"err": err})
		}
	}()

	return task(mountInfo)
}

// withConfigVolume runs task with the config volume of the instance, or of the instance snapshot, mounted.
func (d *qemu) withConfigVolume(task func() error) error {
	return d.withInstanceMounted(func(*storagePools.MountInfo) error { return task() })
}

// qcow2BlockDev opens the qcow2 image at path, passes it to the running QEMU process by file descriptor under the
// given fd set name and returns the options of a qcow2 block node over it. The caller adds the node and adds any
// further options first, and the returned function removes the fd set once the node is gone.
func (d *qemu) qcow2BlockDev(monitor *qmp.Monitor, nodeName string, fdSetName string, path string, readOnly bool) (map[string]any, func(), error) {
	flags := unix.O_RDWR
	if readOnly {
		flags = unix.O_RDONLY
	}

	file, err := os.OpenFile(path, flags, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed opening image %q: %w", path, err)
	}

	// Closed as soon as QEMU has its own descriptor, so that it does not prevent a clean unmount on stop.
	defer func() { _ = file.Close() }()

	info, err := monitor.SendFileWithFDSet(fdSetName, file, readOnly)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed sending file descriptor of %q for block node %q: %w", path, nodeName, err)
	}

	blockDev := map[string]any{
		"driver":    "qcow2",
		"node-name": nodeName,
		"read-only": readOnly,
		"file": map[string]any{
			"driver":   "file",
			"filename": fmt.Sprintf("/dev/fdset/%d", info.ID),
			"locking":  "off",
		},
	}

	return blockDev, func() { _ = monitor.RemoveFDFromFDSet(fdSetName) }, nil
}

// addQcow2Node adds the qcow2 image at path to the running QEMU process as a block node of the given name and with
// the given further options, without a guest visible device. The returned function removes the node and its fd
// set. Removing a writable node writes its persistent bitmaps into the image.
func (d *qemu) addQcow2Node(monitor *qmp.Monitor, nodeName string, path string, readOnly bool, options map[string]any) (func(), error) {
	blockDev, removeFDSet, err := d.qcow2BlockDev(monitor, nodeName, nodeName, path, readOnly)
	if err != nil {
		return nil, err
	}

	maps.Copy(blockDev, options)

	err = monitor.AddBlockDevice(blockDev, nil)
	if err != nil {
		removeFDSet()
		return nil, fmt.Errorf("Failed adding block node %q: %w", nodeName, err)
	}

	return func() { d.removeQcow2Node(monitor, nodeName) }, nil
}

// removeQcow2Node removes a block node added by addQcow2Node together with its fd set.
func (d *qemu) removeQcow2Node(monitor *qmp.Monitor, nodeName string) {
	err := monitor.RemoveBlockDevice(nodeName)
	if err != nil {
		d.logger.Warn("Failed removing block node", logger.Ctx{"node": nodeName, "err": err})
	}

	err = monitor.RemoveFDFromFDSet(nodeName)
	if err != nil {
		d.logger.Warn("Failed removing file descriptor set", logger.Ctx{"node": nodeName, "err": err})
	}
}

// createQcow2Node creates an empty qcow2 image of the given virtual size at path and adds it to the running QEMU
// process as a writable block node of the given name and with the given further options. The returned function
// removes the node and leaves the file in place.
func (d *qemu) createQcow2Node(monitor *qmp.Monitor, nodeName string, path string, size int64, options map[string]any) (func(), error) {
	err := os.MkdirAll(filepath.Dir(path), 0700)
	if err != nil {
		return nil, fmt.Errorf("Failed creating directory of image %q: %w", path, err)
	}

	err = storagePools.Qcow2Create(path, size)
	if err != nil {
		return nil, err
	}

	removeNode, err := d.addQcow2Node(monitor, nodeName, path, false, options)
	if err != nil {
		_ = os.Remove(path)
		return nil, err
	}

	return removeNode, nil
}

// addOverlay adds an empty qcow2 overlay block node of the given virtual size to the running QEMU process, for
// blockdev-snapshot to use as the overlay of a disk. The overlay file is created on the instance config volume so
// that the root disk's size.state property limits its growth, and it is unlinked as soon as QEMU has opened it so
// that it is deleted when QEMU exits. The returned function removes the overlay block node and its file descriptor set.
func (d *qemu) addOverlay(monitor *qmp.Monitor, overlayNode string, size int64) (func(), error) {
	overlayFile := filepath.Join(d.Path(), overlayNode+".qcow2")

	removeOverlay, err := d.createQcow2Node(monitor, overlayNode, overlayFile, size, map[string]any{"backing": nil})
	if err != nil {
		return nil, err
	}

	// Remove the overlay file so that it is neither synced to a migration target nor left behind.
	err = os.Remove(overlayFile)
	if err != nil {
		removeOverlay()
		return nil, err
	}

	return removeOverlay, nil
}

// bitmapDisk describes a disk device whose volume supports bitmaps, which is the root disk or a custom block volume
// that is not shared, as the bitmaps of the instance's QEMU process record every write to such a volume.
type bitmapDisk struct {
	deviceName string
	volume     api.InstanceBitmapVolume
}

// nodeName returns the disk node of the disk, the qcow2 node over its volume metadata image.
func (disk bitmapDisk) nodeName() string {
	return blockNodeName(disk.deviceName)
}

// dataNodeName returns the block node of the volume of the disk, the data file of its disk node.
func (disk bitmapDisk) dataNodeName() string {
	return dataNodeName(disk.deviceName)
}

// volumeProject returns the project the volume of the disk is stored in.
func (disk bitmapDisk) volumeProject(instProject *api.Project) string {
	if disk.volume.Type == dbCluster.StoragePoolVolumeTypeNameCustom {
		return project.StorageVolumeProjectFromRecord(instProject, dbCluster.StoragePoolVolumeTypeCustom)
	}

	return instProject.Name
}

// diskVolume returns the volume attached through a disk device, or nil for a disk device whose volume does not
// support bitmaps. pools caches the storage pools by name across calls.
func (d *qemu) diskVolume(deviceName string, devConf map[string]string, isRootDisk bool, pools map[string]storagePools.Pool) (*api.InstanceBitmapVolume, error) {
	// A custom volume attached through a disk device without a path is a block volume, and the root disk of a
	// virtual machine is one as well. Any other disk device has no volume that a bitmap can be exported with.
	if !isRootDisk && !filters.IsCustomVolumeBlockDisk(devConf) {
		return nil, nil
	}

	// A disk device can attach the root volume of another virtual machine or a snapshot, and neither is written by
	// this instance alone. A read-only disk is never written.
	if !isRootDisk && (devConf["source.type"] != "" && devConf["source.type"] != dbCluster.StoragePoolVolumeTypeNameCustom) {
		return nil, nil
	}

	if devConf["source.snapshot"] != "" || shared.IsTrue(devConf["readonly"]) {
		return nil, nil
	}

	poolName := devConf["pool"]
	pool, ok := pools[poolName]
	if !ok {
		var err error
		pool, err = storagePools.LoadByName(d.state, poolName)
		if err != nil {
			return nil, fmt.Errorf("Failed loading storage pool %q: %w", poolName, err)
		}

		pools[poolName] = pool
	}

	volType := storageDrivers.VolumeTypeCustom
	volName := devConf["source"]
	volProject := project.StorageVolumeProjectFromRecord(&d.project, dbCluster.StoragePoolVolumeTypeCustom)
	if isRootDisk {
		volType = storageDrivers.VolumeTypeVM
		volName = d.name
		volProject = d.project.Name
	}

	dbVol, err := storagePools.VolumeDBGet(pool, volProject, volName, volType)
	if err != nil {
		return nil, fmt.Errorf("Failed loading volume %q of disk %q: %w", volName, deviceName, err)
	}

	if dbVol.ContentType != dbCluster.StoragePoolVolumeContentTypeNameBlock {
		return nil, nil
	}

	// Several instances can write to a shared volume, so a bitmap of one QEMU process does not record every
	// write to it.
	if shared.IsTrue(dbVol.Config["security.shared"]) {
		return nil, nil
	}

	return &api.InstanceBitmapVolume{
		Pool:   pool.Name(),
		Type:   dbVol.Type,
		Name:   dbVol.Name,
		UUID:   dbVol.Config["volatile.uuid"],
		Device: deviceName,
	}, nil
}

// bitmapDisk returns the disk device of the given name with its volume, or nil when the device is not a disk whose
// volume supports bitmaps.
func (d *qemu) bitmapDisk(deviceName string) (*bitmapDisk, error) {
	devConf, ok := d.ExpandedDevices()[deviceName]
	if !ok || !filters.IsDisk(devConf) {
		return nil, nil
	}

	rootDiskName, _, err := d.getRootDiskDevice()
	if err != nil {
		return nil, fmt.Errorf("Failed getting root disk: %w", err)
	}

	volume, err := d.diskVolume(deviceName, devConf, deviceName == rootDiskName, make(map[string]storagePools.Pool))
	if err != nil {
		return nil, err
	}

	if volume == nil {
		return nil, nil
	}

	return &bitmapDisk{deviceName: deviceName, volume: *volume}, nil
}

// disksSupportingBitmaps returns the disk devices whose volumes support bitmaps, sorted by device name.
func (d *qemu) disksSupportingBitmaps() ([]bitmapDisk, error) {
	rootDiskName, _, err := d.getRootDiskDevice()
	if err != nil {
		return nil, fmt.Errorf("Failed getting root disk: %w", err)
	}

	pools := make(map[string]storagePools.Pool)
	disks := []bitmapDisk{}
	for deviceName, devConf := range d.ExpandedDevices() {
		if !filters.IsDisk(devConf) {
			continue
		}

		volume, err := d.diskVolume(deviceName, devConf, deviceName == rootDiskName, pools)
		if err != nil {
			return nil, err
		}

		if volume == nil {
			continue
		}

		disks = append(disks, bitmapDisk{deviceName: deviceName, volume: *volume})
	}

	slices.SortFunc(disks, func(a bitmapDisk, b bitmapDisk) int { return strings.Compare(a.deviceName, b.deviceName) })

	return disks, nil
}

// selectDisks returns the disks of the given devices among disks, in the order of disks.
func selectDisks(disks []bitmapDisk, deviceNames []string) []bitmapDisk {
	selected := make([]bitmapDisk, 0, len(deviceNames))
	for _, disk := range disks {
		if slices.Contains(deviceNames, disk.deviceName) {
			selected = append(selected, disk)
		}
	}

	return selected
}

// prepareVolumeMetadataImage makes sure that the volume metadata image of the disk exists on the config volume at
// the size of the data node of the disk, which is the size of the volume, and returns its path. An image of another
// size is replaced, because the volume was resized while the instance was stopped and the bitmaps of the image no
// longer match it.
func (d *qemu) prepareVolumeMetadataImage(monitor *qmp.Monitor, disk bitmapDisk) (string, error) {
	size, err := monitor.BlockNodeSize(disk.dataNodeName())
	if err != nil {
		return "", fmt.Errorf("Failed getting size of disk %q: %w", disk.deviceName, err)
	}

	path := d.volumeMetadataImagePath(disk.volume.UUID)
	if shared.PathExists(path) {
		imageSize, err := storagePools.Qcow2VirtualSize(path)
		if err == nil && imageSize == size {
			return path, nil
		}

		d.logger.Warn("Replacing metadata image that does not match the volume", logger.Ctx{"device": disk.deviceName, "imageSize": imageSize, "volumeSize": size, "err": err})
	}

	err = os.MkdirAll(d.metadataImagesDir(), 0700)
	if err != nil {
		return "", fmt.Errorf("Failed creating metadata images directory: %w", err)
	}

	err = storagePools.Qcow2CreateMetadataImage(path, size)
	if err != nil {
		return "", err
	}

	return path, nil
}

// addVolumeMetadataImageNode adds the disk node of the disk to the running QEMU process as a qcow2 node over its
// volume metadata image, with the data node of the disk as its data file. The data node must exist. The returned
// function removes the disk node and the fd set of the image, and removing the node writes the bitmaps of the
// volume into the image.
func (d *qemu) addVolumeMetadataImageNode(monitor *qmp.Monitor, disk bitmapDisk) (func(), error) {
	path, err := d.prepareVolumeMetadataImage(monitor, disk)
	if err != nil {
		return nil, err
	}

	fdSetName := imageFDSetName(disk.deviceName)
	blockDev, removeFDSet, err := d.qcow2BlockDev(monitor, disk.nodeName(), fdSetName, path, false)
	if err != nil {
		return nil, err
	}

	blockDev["data-file"] = disk.dataNodeName()
	blockDev["discard"] = "unmap" // Forward as an unmap request to the data file.

	err = monitor.AddBlockDevice(blockDev, nil)
	if err != nil {
		removeFDSet()
		return nil, fmt.Errorf("Failed adding block node %q: %w", disk.nodeName(), err)
	}

	return func() {
		err := monitor.RemoveBlockDevice(disk.nodeName())
		if err != nil {
			d.logger.Warn("Failed removing block node", logger.Ctx{"node": disk.nodeName(), "err": err})
		}

		removeFDSet()
	}, nil
}

// pruneMetadataImages deletes from the metadata images directory every file that is not the volume metadata image
// or the overlay of a volume attached through one of the given disks. A snapshot metadata image left on the config
// volume by a failed snapshot and the volume metadata image of a detached volume are removed this way.
func (d *qemu) pruneMetadataImages(disks []bitmapDisk) error {
	entries, err := os.ReadDir(d.metadataImagesDir())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}

		return err
	}

	keep := make([]string, 0, len(disks)*2)
	for _, disk := range disks {
		keep = append(keep, filepath.Base(d.volumeMetadataImagePath(disk.volume.UUID)), filepath.Base(d.overlayPath(disk.volume.UUID)))
	}

	for _, entry := range entries {
		if slices.Contains(keep, entry.Name()) {
			continue
		}

		d.logger.Debug("Removing metadata image", logger.Ctx{"file": entry.Name()})
		err := os.RemoveAll(filepath.Join(d.metadataImagesDir(), entry.Name()))
		if err != nil {
			return err
		}
	}

	return nil
}

// addOverlays adds the overlay file of every disk that has one back to its disk and commits it. An overlay file is
// left on the config volume when QEMU exited before a commit of the overlay succeeded, and it contains guest writes
// that the volume lacks. It runs before the guest starts, so that the guest reads its writes. An overlay that cannot
// be added back fails the start, and one that cannot be committed stays, with the guest writing to it.
func (d *qemu) addOverlays(monitor *qmp.Monitor) error {
	nodeNames, err := monitor.QueryNamedBlockNodes()
	if err != nil {
		return err
	}

	disks, err := d.disksSupportingBitmaps()
	if err != nil {
		return err
	}

	for _, disk := range disks {
		overlayPath := d.overlayPath(disk.volume.UUID)
		if !slices.Contains(nodeNames, disk.nodeName()) || !shared.PathExists(overlayPath) {
			continue
		}

		overlayNode := overlayNodeName(disk.deviceName)
		removeOverlay, err := d.addQcow2Node(monitor, overlayNode, overlayPath, false, map[string]any{"backing": nil})
		if err != nil {
			return fmt.Errorf("Failed adding overlay back to disk %q: %w", disk.deviceName, err)
		}

		err = monitor.BlockDevSnapshot(disk.nodeName(), overlayNode)
		if err != nil {
			removeOverlay()
			return fmt.Errorf("Failed adding overlay back to disk %q: %w", disk.deviceName, err)
		}

		err = d.commitOverlay(monitor, disk)
		if err != nil {
			d.logger.Error("Failed committing overlay", logger.Ctx{"device": disk.deviceName, "err": err})
		}
	}

	return nil
}

// commitOverlay commits the overlay of a disk into its volume, removes the overlay node and deletes the overlay
// file. The bitmaps of the volume record the committed writes. The commit is retried, because the guest writes to
// the overlay until it succeeds.
func (d *qemu) commitOverlay(monitor *qmp.Monitor, disk bitmapDisk) error {
	overlayNode := overlayNodeName(disk.deviceName)

	var err error
	for attempt := range 3 {
		if attempt > 0 {
			time.Sleep(time.Second)
		}

		err = monitor.BlockCommit(overlayNode)
		if err == nil {
			break
		}

		d.logger.Warn("Failed committing overlay", logger.Ctx{"device": disk.deviceName, "attempt": attempt + 1, "err": err})
	}

	if err != nil {
		d.raiseOverlayWarning(disk)
		return fmt.Errorf("Failed committing overlay of disk %q: %w", disk.deviceName, err)
	}

	d.removeQcow2Node(monitor, overlayNode)

	err = os.Remove(d.overlayPath(disk.volume.UUID))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("Failed removing overlay of disk %q: %w", disk.deviceName, err)
	}

	d.resolveOverlayWarning()

	return nil
}

// raiseOverlayWarning raises the warning of a failed commit. Until a commit succeeds the volume lacks the writes of
// the guest, so every storage snapshot, copy and backup of it is inconsistent.
func (d *qemu) raiseOverlayWarning(disk bitmapDisk) {
	_ = d.state.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
		return tx.UpsertWarning(ctx, d.node, d.project.Name, entity.TypeInstance, d.ID(), warningtype.InstanceDiskOverlayNotCommitted, fmt.Sprintf("The volume of disk %q lacks the writes of the guest since the last snapshot with a bitmap", disk.deviceName))
	})
}

// resolveOverlayWarning resolves the warning of a failed commit once no disk of the instance has an overlay file. A
// committed overlay is deleted, so the files on the config volume are the overlays that remain.
func (d *qemu) resolveOverlayWarning() {
	disks, err := d.disksSupportingBitmaps()
	if err != nil {
		return
	}

	for _, disk := range disks {
		if shared.PathExists(d.overlayPath(disk.volume.UUID)) {
			return
		}
	}

	_ = warnings.ResolveWarningsByNodeAndProjectAndTypeAndEntity(d.state.DB.Cluster, d.node, d.project.Name, warningtype.InstanceDiskOverlayNotCommitted, entity.TypeInstance, d.ID())
}

// commitOverlays commits the overlays of the given disks of the running instance. A disk without an overlay node is
// skipped.
func (d *qemu) commitOverlays(monitor *qmp.Monitor, disks []bitmapDisk) error {
	nodeNames, err := monitor.QueryNamedBlockNodes()
	if err != nil {
		return err
	}

	var errs []error
	for _, disk := range disks {
		if !slices.Contains(nodeNames, overlayNodeName(disk.deviceName)) {
			continue
		}

		err := d.commitOverlay(monitor, disk)
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// commitAllOverlays commits the overlays that a failed commit left on the disks of the running instance.
func (d *qemu) commitAllOverlays(monitor *qmp.Monitor) error {
	disks, err := d.disksSupportingBitmaps()
	if err != nil {
		return err
	}

	return d.commitOverlays(monitor, disks)
}

// commitAllOverlayFiles commits the overlay files that a failed commit left on the config volume of the stopped
// instance.
func (d *qemu) commitAllOverlayFiles() error {
	disks, err := d.disksSupportingBitmaps()
	if err != nil {
		return err
	}

	return d.commitOverlayFiles(disks)
}

// diskDevicePath returns the path of the block device of the volume of a disk of the stopped instance, with the
// volume mounted until the returned function runs. rootDevicePath is the block device of the root volume, which
// the caller mounted.
func (d *qemu) diskDevicePath(disk bitmapDisk, rootDevicePath string) (string, func(), error) {
	if disk.volume.Type != dbCluster.StoragePoolVolumeTypeNameCustom {
		if rootDevicePath == "" {
			return "", nil, fmt.Errorf("The volume of disk %q has no block device path", disk.deviceName)
		}

		return rootDevicePath, func() {}, nil
	}

	pool, err := storagePools.LoadByName(d.state, disk.volume.Pool)
	if err != nil {
		return "", nil, err
	}

	volProject := disk.volumeProject(&d.project)
	mountInfo, err := pool.MountCustomVolume(volProject, disk.volume.Name, nil)
	if err != nil {
		return "", nil, err
	}

	unmount := func() {
		_, err := pool.UnmountCustomVolume(volProject, disk.volume.Name, nil)
		if err != nil {
			d.logger.Warn("Failed unmounting volume", logger.Ctx{"device": disk.deviceName, "err": err})
		}
	}

	devSource, ok := mountInfo.DevSource.(deviceConfig.DevSourcePath)
	if !ok || devSource.Path == "" {
		unmount()
		return "", nil, fmt.Errorf("The volume of disk %q has no block device path", disk.deviceName)
	}

	return devSource.Path, unmount, nil
}

// commitOverlayFiles commits the overlay file of every given disk of the stopped instance into its volume, through
// the volume metadata image so that the bitmaps of the volume record the committed writes, and deletes the file. A
// disk without an overlay file is skipped.
func (d *qemu) commitOverlayFiles(disks []bitmapDisk) error {
	return d.withInstanceMounted(func(mountInfo *storagePools.MountInfo) error {
		rootDevicePath := ""
		devSource, ok := mountInfo.DevSource.(deviceConfig.DevSourcePath)
		if ok {
			rootDevicePath = devSource.Path
		}

		var errs []error
		for _, disk := range disks {
			overlayPath := d.overlayPath(disk.volume.UUID)
			if !shared.PathExists(overlayPath) {
				continue
			}

			err := d.commitOverlayFile(disk, overlayPath, rootDevicePath)
			if err != nil {
				d.raiseOverlayWarning(disk)
				errs = append(errs, err)
			}
		}

		return errors.Join(errs...)
	})
}

// commitOverlayFile commits the overlay file of a disk of the stopped instance and deletes it.
func (d *qemu) commitOverlayFile(disk bitmapDisk, overlayPath string, rootDevicePath string) error {
	imagePath := d.volumeMetadataImagePath(disk.volume.UUID)
	if !shared.PathExists(imagePath) {
		return fmt.Errorf("Failed committing overlay of disk %q: The volume metadata image does not exist", disk.deviceName)
	}

	devicePath, unmount, err := d.diskDevicePath(disk, rootDevicePath)
	if err != nil {
		return fmt.Errorf("Failed committing overlay of disk %q: %w", disk.deviceName, err)
	}

	defer unmount()

	err = storagePools.Qcow2Commit(overlayPath, imagePath, devicePath)
	if err != nil {
		return fmt.Errorf("Failed committing overlay of disk %q: %w", disk.deviceName, err)
	}

	err = os.Remove(overlayPath)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("Failed removing overlay of disk %q: %w", disk.deviceName, err)
	}

	d.resolveOverlayWarning()

	return nil
}

// CommitDiskOverlays commits the overlays that a failed commit left on the given disk devices into their volumes,
// through their disk nodes while the instance runs and through their volume metadata images while it is stopped. A
// device without an overlay is skipped.
func (d *qemu) CommitDiskOverlays(deviceNames []string) error {
	disks, err := d.disksSupportingBitmaps()
	if err != nil {
		return err
	}

	disks = selectDisks(disks, deviceNames)
	if len(disks) == 0 {
		return nil
	}

	if !d.IsRunning() {
		return d.commitOverlayFiles(disks)
	}

	monitor, err := qmp.Connect(d.monitorPath(), qemuSerialChardevName, d.getMonitorEventHandler())
	if err != nil {
		return err
	}

	return d.commitOverlays(monitor, disks)
}

// instanceSnapshotUUIDs returns the instance snapshot UUID of every snapshot of the instance by snapshot name, which
// is the UUID of the root volume snapshot of the instance snapshot.
func (d *qemu) instanceSnapshotUUIDs() (map[string]string, error) {
	pool, err := d.getStoragePool()
	if err != nil {
		return nil, err
	}

	dbVolSnaps, err := storagePools.VolumeDBSnapshotsGet(pool, d.project.Name, d.name, storageDrivers.VolumeTypeVM)
	if err != nil {
		return nil, err
	}

	uuids := make(map[string]string, len(dbVolSnaps))
	for _, dbVolSnap := range dbVolSnaps {
		_, snapName, _ := api.GetParentAndSnapshotName(dbVolSnap.Name)
		uuids[snapName] = dbVolSnap.Config["volatile.uuid"]
	}

	return uuids, nil
}

// snapshotBitmapNames returns the names of the instance snapshots a bitmap of the disk can belong to. On the root
// disk these are the instance snapshots. On a custom volume they are the instance snapshots that record a snapshot
// of the volume in volatile.attached_volumes and whose recorded snapshot still exists.
func (d *qemu) snapshotBitmapNames(disk bitmapDisk) (map[string]struct{}, error) {
	snapshots, err := d.Snapshots()
	if err != nil {
		return nil, err
	}

	names := make(map[string]struct{}, len(snapshots))
	if disk.volume.Type != dbCluster.StoragePoolVolumeTypeNameCustom {
		for _, snapshot := range snapshots {
			_, snapName, _ := api.GetParentAndSnapshotName(snapshot.Name())
			names[snapName] = struct{}{}
		}

		return names, nil
	}

	pool, err := storagePools.LoadByName(d.state, disk.volume.Pool)
	if err != nil {
		return nil, err
	}

	dbVolSnaps, err := storagePools.VolumeDBSnapshotsGet(pool, disk.volumeProject(&d.project), disk.volume.Name, storageDrivers.VolumeTypeCustom)
	if err != nil {
		return nil, err
	}

	volSnapUUIDs := make([]string, 0, len(dbVolSnaps))
	for _, dbVolSnap := range dbVolSnaps {
		volSnapUUIDs = append(volSnapUUIDs, dbVolSnap.Config["volatile.uuid"])
	}

	for _, snapshot := range snapshots {
		attachedVolumes, err := parseVolatileAttachedVolumes(snapshot)
		if err != nil {
			return nil, err
		}

		// The volume is attached through one device, whose name may have changed since the snapshot, so the
		// snapshot is matched by the volume snapshot it records rather than by device.
		for _, snapshotUUID := range attachedVolumes {
			if slices.Contains(volSnapUUIDs, snapshotUUID) {
				_, snapName, _ := api.GetParentAndSnapshotName(snapshot.Name())
				names[snapName] = struct{}{}
				break
			}
		}
	}

	return names, nil
}

// removeStaleBitmaps removes from the disk node every bitmap that is invalid, because it did not record every write
// since it was created, and every bitmap whose name matches no snapshot the bitmap can belong to, which a failed
// snapshot left behind. A bitmap named after the snapshot being created was left by a failed snapshot of that name,
// whose record no longer exists, and is removed although the record of the new snapshot exists already.
func (d *qemu) removeStaleBitmaps(monitor *qmp.Monitor, disk bitmapDisk, newBitmapName string) error {
	bitmaps, err := monitor.QueryNodeDirtyBitmaps(disk.nodeName())
	if err != nil {
		return fmt.Errorf("Failed querying bitmaps of disk %q: %w", disk.deviceName, err)
	}

	if len(bitmaps) == 0 {
		return nil
	}

	names, err := d.snapshotBitmapNames(disk)
	if err != nil {
		return err
	}

	for _, bitmap := range bitmaps {
		_, found := names[bitmap.Name]
		if found && bitmap.Name != newBitmapName && bitmap.Recording && !bitmap.Inconsistent {
			continue
		}

		d.logger.Info("Removing stale bitmap", logger.Ctx{"device": disk.deviceName, "bitmap": bitmap.Name, "recording": bitmap.Recording, "inconsistent": bitmap.Inconsistent, "snapshot": found})
		err = monitor.RemoveDirtyBitmap(disk.nodeName(), bitmap.Name)
		if err != nil {
			return fmt.Errorf("Failed removing bitmap %q of disk %q: %w", bitmap.Name, disk.deviceName, err)
		}
	}

	return nil
}

// snapshotMetadataImage is the snapshot metadata image of a volume snapshot, found on the config volume snapshot of
// an instance snapshot.
type snapshotMetadataImage struct {
	path           string
	deviceName     string
	volumeUUID     string
	snapshotUUID   string
	pool           string // Pool of the custom volume snapshot, empty for the root volume snapshot.
	volumeSnapshot string // Name of the custom volume snapshot, empty for the root volume snapshot.
}

// snapshotMetadataImages returns the snapshot metadata images of the config volume snapshot that refer to a volume
// snapshot of the instance snapshot, with the disk device each volume was attached through. An image refers to the
// volume snapshot whose UUID it is named after when that snapshot exists and belongs to the volume the image is
// named after. The image of the root volume snapshot is matched to the snapshot itself, and the image of a custom
// volume snapshot to the snapshot the instance snapshot records in volatile.attached_volumes. The config volume
// snapshot must be mounted.
func (d *qemu) snapshotMetadataImages() ([]snapshotMetadataImage, error) {
	entries, err := os.ReadDir(d.metadataImagesDir())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}

		return nil, err
	}

	pool, err := d.getStoragePool()
	if err != nil {
		return nil, err
	}

	rootSnapVol, err := storagePools.VolumeDBGet(pool, d.project.Name, d.name, storageDrivers.VolumeTypeVM)
	if err != nil {
		return nil, err
	}

	parentName, _, _ := api.GetParentAndSnapshotName(d.name)
	rootVol, err := storagePools.VolumeDBGet(pool, d.project.Name, parentName, storageDrivers.VolumeTypeVM)
	if err != nil {
		return nil, err
	}

	rootDiskName, _, err := d.getRootDiskDevice()
	if err != nil {
		return nil, fmt.Errorf("Failed getting root disk: %w", err)
	}

	attachedVolumes, err := parseVolatileAttachedVolumes(d)
	if err != nil {
		return nil, err
	}

	devices := make(map[string]string, len(attachedVolumes))
	for deviceName, snapshotUUID := range attachedVolumes {
		devices[snapshotUUID] = deviceName
	}

	volProject := project.StorageVolumeProjectFromRecord(&d.project, dbCluster.StoragePoolVolumeTypeCustom)
	customType := dbCluster.StoragePoolVolumeTypeCustom
	pools := map[string]storagePools.Pool{pool.Name(): pool}
	images := []snapshotMetadataImage{}
	for _, entry := range entries {
		volumeUUID, snapshotUUID, ok := parseSnapshotMetadataImageName(entry.Name())
		if !ok {
			continue
		}

		image := snapshotMetadataImage{
			path:         filepath.Join(d.metadataImagesDir(), entry.Name()),
			volumeUUID:   volumeUUID,
			snapshotUUID: snapshotUUID,
		}

		if snapshotUUID == rootSnapVol.Config["volatile.uuid"] {
			if volumeUUID != rootVol.Config["volatile.uuid"] {
				continue
			}

			image.deviceName = rootDiskName
			images = append(images, image)
			continue
		}

		deviceName, ok := devices[snapshotUUID]
		if !ok {
			continue
		}

		// The custom volume snapshot can be deleted on its own.
		var dbSnapVols []*db.StorageVolume
		err = d.state.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
			var err error
			dbSnapVols, err = tx.GetStorageVolumes(ctx, true, db.StorageVolumeFilter{Type: &customType, Project: &volProject, UUIDs: []string{snapshotUUID}})
			return err
		})
		if err != nil {
			return nil, err
		}

		if len(dbSnapVols) == 0 {
			d.logger.Warn("Skipping snapshot metadata image of a volume snapshot that no longer exists", logger.Ctx{"device": deviceName, "snapshotUUID": snapshotUUID})
			continue
		}

		dbSnapVol := dbSnapVols[0]
		volPool, ok := pools[dbSnapVol.Pool]
		if !ok {
			volPool, err = storagePools.LoadByName(d.state, dbSnapVol.Pool)
			if err != nil {
				return nil, err
			}

			pools[dbSnapVol.Pool] = volPool
		}

		volName, _, _ := api.GetParentAndSnapshotName(dbSnapVol.Name)
		dbVol, err := storagePools.VolumeDBGet(volPool, volProject, volName, storageDrivers.VolumeTypeCustom)
		if err != nil {
			return nil, err
		}

		if dbVol.Config["volatile.uuid"] != volumeUUID {
			d.logger.Warn("Skipping snapshot metadata image of a volume snapshot that belongs to another volume", logger.Ctx{"device": deviceName, "snapshotUUID": snapshotUUID, "volumeUUID": volumeUUID})
			continue
		}

		image.deviceName = deviceName
		image.pool = dbSnapVol.Pool
		image.volumeSnapshot = dbSnapVol.Name
		images = append(images, image)
	}

	slices.SortFunc(images, func(a snapshotMetadataImage, b snapshotMetadataImage) int {
		return strings.Compare(a.deviceName, b.deviceName)
	})

	return images, nil
}

// snapshotVolumes returns the volumes of the instance snapshot by disk device, with the pool, type and name they had
// when the snapshot was created, as the devices of the snapshot record them.
func (d *qemu) snapshotVolumes() (map[string]api.InstanceBitmapVolume, error) {
	rootDiskName, rootDiskConf, err := d.getRootDiskDevice()
	if err != nil {
		return nil, fmt.Errorf("Failed getting root disk: %w", err)
	}

	parentName, _, _ := api.GetParentAndSnapshotName(d.name)
	volumes := map[string]api.InstanceBitmapVolume{
		rootDiskName: {Pool: rootDiskConf["pool"], Type: dbCluster.StoragePoolVolumeTypeNameVM, Name: parentName, Device: rootDiskName},
	}

	for deviceName, devConf := range d.ExpandedDevices() {
		if !filters.IsCustomVolumeBlockDisk(devConf) {
			continue
		}

		volumes[deviceName] = api.InstanceBitmapVolume{Pool: devConf["pool"], Type: dbCluster.StoragePoolVolumeTypeNameCustom, Name: devConf["source"], Device: deviceName}
	}

	return volumes, nil
}

// snapshotBackupSnapshotUUIDs returns the instance snapshot UUID of every instance snapshot listed in the backup
// metadata of the config volume snapshot, by snapshot name. The backup metadata is written before the snapshot, so it
// lists the snapshots that the bitmaps of the snapshot were created with, under the names they had at the time.
func (d *qemu) snapshotBackupSnapshotUUIDs() (map[string]string, error) {
	root, err := os.OpenRoot(d.Path())
	if err != nil {
		return nil, err
	}

	defer func() { _ = root.Close() }()

	backupConf, err := backup.ParseConfigYamlFile(root)
	if err != nil {
		return nil, fmt.Errorf("Failed reading backup metadata of snapshot: %w", err)
	}

	rootVol, err := backupConf.RootVolume()
	if err != nil {
		return nil, fmt.Errorf("Failed reading backup metadata of snapshot: %w", err)
	}

	uuids := make(map[string]string, len(rootVol.Snapshots))
	for _, snapshot := range rootVol.Snapshots {
		uuids[snapshot.Name] = snapshot.Config["volatile.uuid"]
	}

	return uuids, nil
}

// SnapshotMetadataImages returns the snapshot metadata images of the volume snapshots of the instance snapshot,
// keyed by the disk device each volume was attached through. The images are on the config volume snapshot, which
// the caller mounts to use them.
func (d *qemu) SnapshotMetadataImages() (map[string]instance.SnapshotMetadataImage, error) {
	if !d.IsSnapshot() {
		return nil, errors.New("Instance must be a snapshot")
	}

	images := map[string]instance.SnapshotMetadataImage{}
	err := d.withConfigVolume(func() error {
		found, err := d.snapshotMetadataImages()
		if err != nil {
			return err
		}

		for _, image := range found {
			images[image.deviceName] = instance.SnapshotMetadataImage{
				Path:           image.path,
				VolumeUUID:     image.volumeUUID,
				SnapshotUUID:   image.snapshotUUID,
				Pool:           image.pool,
				VolumeSnapshot: image.volumeSnapshot,
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return images, nil
}

// bitmapEntry is a bitmap found on one volume.
type bitmapEntry struct {
	name        string
	volume      api.InstanceBitmapVolume
	granularity int64
	recording   bool
}

// groupBitmaps groups the bitmaps found on the volumes by name, sorted by name, with the instance snapshot UUID
// each name maps to.
func groupBitmaps(entries []bitmapEntry, uuids map[string]string) []api.InstanceBitmap {
	byName := make(map[string]*api.InstanceBitmap)
	for _, entry := range entries {
		bitmap, found := byName[entry.name]
		if !found {
			bitmap = &api.InstanceBitmap{Name: entry.name, UUID: uuids[entry.name]}
			byName[entry.name] = bitmap
		}

		volume := entry.volume
		volume.Granularity = entry.granularity
		volume.Recording = entry.recording
		bitmap.Volumes = append(bitmap.Volumes, volume)
	}

	bitmaps := make([]api.InstanceBitmap, 0, len(byName))
	for _, bitmap := range byName {
		slices.SortFunc(bitmap.Volumes, func(a api.InstanceBitmapVolume, b api.InstanceBitmapVolume) int {
			return strings.Compare(a.Device, b.Device)
		})

		bitmaps = append(bitmaps, *bitmap)
	}

	slices.SortFunc(bitmaps, func(a api.InstanceBitmap, b api.InstanceBitmap) int {
		return strings.Compare(a.Name, b.Name)
	})

	return bitmaps
}

// Bitmaps returns the bitmaps of the block volumes of the instance, grouped by name with one entry per volume. On a
// running instance they are read from its QEMU process, on a stopped instance from the volume metadata images on its
// config volume, and on an instance snapshot from the snapshot metadata images on its config volume snapshot. An
// invalid bitmap, which did not record every write since it was created, is reported as not recording.
func (d *qemu) Bitmaps() ([]api.InstanceBitmap, error) {
	if d.IsSnapshot() {
		return d.snapshotBitmaps()
	}

	disks, err := d.disksSupportingBitmaps()
	if err != nil {
		return nil, err
	}

	uuids, err := d.instanceSnapshotUUIDs()
	if err != nil {
		return nil, err
	}

	entries := []bitmapEntry{}
	if d.IsRunning() {
		monitor, err := qmp.Connect(d.monitorPath(), qemuSerialChardevName, d.getMonitorEventHandler())
		if err != nil {
			return nil, err
		}

		nodeNames, err := monitor.QueryNamedBlockNodes()
		if err != nil {
			return nil, err
		}

		for _, disk := range disks {
			if !slices.Contains(nodeNames, disk.nodeName()) {
				continue
			}

			bitmaps, err := monitor.QueryNodeDirtyBitmaps(disk.nodeName())
			if err != nil {
				return nil, fmt.Errorf("Failed querying bitmaps of disk %q: %w", disk.deviceName, err)
			}

			for _, bitmap := range bitmaps {
				entries = append(entries, bitmapEntry{name: bitmap.Name, volume: disk.volume, granularity: int64(bitmap.Granularity), recording: bitmap.Recording && !bitmap.Inconsistent})
			}
		}

		return groupBitmaps(entries, uuids), nil
	}

	err = d.withConfigVolume(func() error {
		for _, disk := range disks {
			path := d.volumeMetadataImagePath(disk.volume.UUID)
			if !shared.PathExists(path) {
				continue
			}

			bitmaps, err := storagePools.Qcow2Bitmaps(path)
			if err != nil {
				return err
			}

			for _, bitmap := range bitmaps {
				entries = append(entries, bitmapEntry{name: bitmap.Name, volume: disk.volume, granularity: bitmap.Granularity, recording: bitmap.Recording})
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return groupBitmaps(entries, uuids), nil
}

// snapshotBitmaps returns the bitmaps stored in the snapshot metadata images of the instance snapshot. Every bitmap
// of a snapshot is disabled, so none is recording.
func (d *qemu) snapshotBitmaps() ([]api.InstanceBitmap, error) {
	entries := []bitmapEntry{}
	uuids := map[string]string{}
	err := d.withConfigVolume(func() error {
		images, err := d.snapshotMetadataImages()
		if err != nil {
			return err
		}

		if len(images) == 0 {
			return nil
		}

		volumes, err := d.snapshotVolumes()
		if err != nil {
			return err
		}

		uuids, err = d.snapshotBackupSnapshotUUIDs()
		if err != nil {
			return err
		}

		for _, image := range images {
			volume, ok := volumes[image.deviceName]
			if !ok {
				d.logger.Warn("Skipping snapshot metadata image of a volume whose disk device the snapshot does not record", logger.Ctx{"device": image.deviceName, "volumeUUID": image.volumeUUID})
				continue
			}

			volume.UUID = image.volumeUUID

			bitmaps, err := storagePools.Qcow2Bitmaps(image.path)
			if err != nil {
				return err
			}

			for _, bitmap := range bitmaps {
				entries = append(entries, bitmapEntry{name: bitmap.Name, volume: volume, granularity: bitmap.Granularity, recording: false})
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return groupBitmaps(entries, uuids), nil
}

// removeBitmaps removes from the given disks every bitmap whose name matches, from the disk nodes of a running
// instance and from the volume metadata images of a stopped one. The snapshot metadata images keep their copies, as
// a snapshot is never modified.
func (d *qemu) removeBitmaps(disks []bitmapDisk, match func(bitmapName string) bool) error {
	if d.IsRunning() {
		monitor, err := qmp.Connect(d.monitorPath(), qemuSerialChardevName, d.getMonitorEventHandler())
		if err != nil {
			return err
		}

		nodeNames, err := monitor.QueryNamedBlockNodes()
		if err != nil {
			return err
		}

		for _, disk := range disks {
			if !slices.Contains(nodeNames, disk.nodeName()) {
				continue
			}

			bitmaps, err := monitor.QueryNodeDirtyBitmaps(disk.nodeName())
			if err != nil {
				return fmt.Errorf("Failed querying bitmaps of disk %q: %w", disk.deviceName, err)
			}

			for _, bitmap := range bitmaps {
				if !match(bitmap.Name) {
					continue
				}

				err = monitor.RemoveDirtyBitmap(disk.nodeName(), bitmap.Name)
				if err != nil {
					return fmt.Errorf("Failed removing bitmap %q of disk %q: %w", bitmap.Name, disk.deviceName, err)
				}
			}
		}

		return nil
	}

	return d.withConfigVolume(func() error {
		for _, disk := range disks {
			path := d.volumeMetadataImagePath(disk.volume.UUID)
			if !shared.PathExists(path) {
				continue
			}

			bitmaps, err := storagePools.Qcow2Bitmaps(path)
			if err != nil {
				return err
			}

			for _, bitmap := range bitmaps {
				if !match(bitmap.Name) {
					continue
				}

				err = storagePools.Qcow2RemoveBitmap(path, bitmap.Name)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
}

// DeleteBitmap removes the named bitmap from every block volume of the instance. A volume without the bitmap is
// left as it is.
func (d *qemu) DeleteBitmap(bitmapName string) error {
	if d.IsSnapshot() {
		return api.StatusErrorf(http.StatusBadRequest, "Bitmaps cannot be deleted from a snapshot")
	}

	disks, err := d.disksSupportingBitmaps()
	if err != nil {
		return err
	}

	return d.removeBitmaps(disks, func(name string) bool { return name == bitmapName })
}

// DeleteDiskBitmap removes the named bitmap from the volume attached through the disk device. A volume without the
// bitmap is left as it is.
func (d *qemu) DeleteDiskBitmap(deviceName string, bitmapName string) error {
	if d.IsSnapshot() {
		return api.StatusErrorf(http.StatusBadRequest, "Bitmaps cannot be deleted from a snapshot")
	}

	disk, err := d.bitmapDisk(deviceName)
	if err != nil {
		return err
	}

	if disk == nil {
		return nil
	}

	return d.removeBitmaps([]bitmapDisk{*disk}, func(name string) bool { return name == bitmapName })
}

// DeleteVolumeBitmaps deletes every bitmap of the volume attached through the disk device, from the disk node of a
// running instance and from the volume metadata image on the config volume. It runs before the volume is written by
// something other than the instance's own QEMU process, or once another instance can write to it.
func (d *qemu) DeleteVolumeBitmaps(deviceName string) error {
	devConf, ok := d.ExpandedDevices()[deviceName]
	if !ok {
		return fmt.Errorf("Disk device %q not found", deviceName)
	}

	return d.deleteDiskBitmaps(deviceName, devConf)
}

// deleteDiskBitmaps deletes every bitmap of the volume attached through the disk device of the given config, which
// is not required to be in the current devices of the instance, and its volume metadata image.
func (d *qemu) deleteDiskBitmaps(deviceName string, devConf map[string]string) error {
	rootDiskName, _, err := d.getRootDiskDevice()
	if err != nil {
		return fmt.Errorf("Failed getting root disk: %w", err)
	}

	volume, err := d.diskVolume(deviceName, devConf, deviceName == rootDiskName, make(map[string]storagePools.Pool))
	if err != nil {
		return err
	}

	if volume == nil {
		return nil
	}

	if d.IsRunning() {
		err = d.removeBitmaps([]bitmapDisk{{deviceName: deviceName, volume: *volume}}, func(string) bool { return true })
		if err != nil {
			return err
		}
	}

	return d.RemoveVolumeMetadataImage(volume.UUID)
}

// DeleteBitmaps deletes every bitmap of every block volume of the instance and every metadata image on its config
// volume.
func (d *qemu) DeleteBitmaps() error {
	if d.IsRunning() {
		disks, err := d.disksSupportingBitmaps()
		if err != nil {
			return err
		}

		err = d.removeBitmaps(disks, func(string) bool { return true })
		if err != nil {
			return err
		}
	}

	return d.RemoveAllMetadataImages()
}

// RemoveVolumeMetadataImage deletes the volume metadata image of the volume of the given UUID from the config
// volume, and the overlay of the volume with it. A running QEMU process that has the image open keeps its own
// descriptor, and writes the bitmaps of the volume into the unlinked file when it closes it.
func (d *qemu) RemoveVolumeMetadataImage(volumeUUID string) error {
	return d.withConfigVolume(func() error {
		for _, path := range []string{d.volumeMetadataImagePath(volumeUUID), d.overlayPath(volumeUUID)} {
			err := os.Remove(path)
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
		}

		return nil
	})
}

// RemoveAllMetadataImages deletes every metadata image and overlay from the config volume. None of them matches the
// volumes of an instance that was created by a copy, a refresh, an import or a move, or that was restored from a
// snapshot.
func (d *qemu) RemoveAllMetadataImages() error {
	return d.withConfigVolume(func() error {
		return os.RemoveAll(d.metadataImagesDir())
	})
}

// removeDetachedMetadataImages deletes the volume metadata images of the volumes that were attached through the
// given removed devices and are no longer attached through any device. A device rename removes and adds the device
// while the volume stays attached, so the image of its volume is kept. On a stopped instance an overlay left on the
// volume is committed first, as the overlay is deleted with the image.
func (d *qemu) removeDetachedMetadataImages(removeDevices deviceConfig.Devices) error {
	disks, err := d.disksSupportingBitmaps()
	if err != nil {
		return err
	}

	attached := make([]string, 0, len(disks))
	for _, disk := range disks {
		attached = append(attached, disk.volume.UUID)
	}

	detached := []bitmapDisk{}
	for deviceName, devConf := range removeDevices {
		if !filters.IsCustomVolumeBlockDisk(devConf) {
			continue
		}

		volume, err := d.diskVolume(deviceName, devConf, false, make(map[string]storagePools.Pool))
		if err != nil {
			return err
		}

		if volume == nil || slices.Contains(attached, volume.UUID) {
			continue
		}

		detached = append(detached, bitmapDisk{deviceName: deviceName, volume: *volume})
	}

	if len(detached) == 0 {
		return nil
	}

	return d.withInstanceMounted(func(mountInfo *storagePools.MountInfo) error {
		rootDevicePath := ""
		devSource, ok := mountInfo.DevSource.(deviceConfig.DevSourcePath)
		if ok {
			rootDevicePath = devSource.Path
		}

		for _, disk := range detached {
			overlayPath := d.overlayPath(disk.volume.UUID)
			if !d.IsRunning() && shared.PathExists(overlayPath) {
				err := d.commitOverlayFile(disk, overlayPath, rootDevicePath)
				if err != nil {
					return err
				}
			}

			for _, path := range []string{d.volumeMetadataImagePath(disk.volume.UUID), overlayPath} {
				err := os.Remove(path)
				if err != nil && !errors.Is(err, fs.ErrNotExist) {
					return fmt.Errorf("Failed removing metadata image of disk %q: %w", disk.deviceName, err)
				}
			}
		}

		return nil
	})
}

// CreateSnapshotBitmaps creates the bitmap of a snapshot on the volumes attached through the given disk devices and
// copies every valid bitmap those volumes have into the snapshot metadata images of their snapshots, all in one QEMU
// transaction. snapshots maps each disk device to the UUID of the volume snapshot about to be created. The snapshot
// metadata image is created on the config volume before the storage snapshot, so that the config volume snapshot
// includes it. The transaction also adds an overlay to each of the volumes, so that the storage snapshots taken
// afterwards match the instant the bitmap was created at, and the guest writes to the overlays until
// CommitDiskOverlays commits them. A disk device whose volume does not support bitmaps is skipped, and the devices
// that got an overlay are returned.
func (d *qemu) CreateSnapshotBitmaps(snapshots map[string]string, bitmapName string) ([]string, error) {
	if !d.IsRunning() {
		return nil, api.StatusErrorf(http.StatusBadRequest, "Instance is not running")
	}

	monitor, err := qmp.Connect(d.monitorPath(), qemuSerialChardevName, d.getMonitorEventHandler())
	if err != nil {
		return nil, err
	}

	nodeNames, err := monitor.QueryNamedBlockNodes()
	if err != nil {
		return nil, err
	}

	disks, err := d.disksSupportingBitmaps()
	if err != nil {
		return nil, err
	}

	type snapshotDisk struct {
		bitmapDisk
		snapshotUUID string
		bitmaps      []qmp.BlockDirtyInfo
	}

	selected := make([]snapshotDisk, 0, len(snapshots))
	for _, disk := range disks {
		snapshotUUID, ok := snapshots[disk.deviceName]
		if !ok || !slices.Contains(nodeNames, disk.nodeName()) {
			continue
		}

		// The bitmaps of the volume are stored in its volume metadata image, which the disk is opened through
		// from the first start after the upgrade on.
		if !slices.Contains(nodeNames, disk.dataNodeName()) {
			return nil, api.StatusErrorf(http.StatusBadRequest, "The volume metadata image of disk %q does not exist", disk.deviceName)
		}

		// An overlay left by a failed commit is committed before a new overlay is added.
		if slices.Contains(nodeNames, overlayNodeName(disk.deviceName)) {
			err = d.commitOverlay(monitor, disk)
			if err != nil {
				return nil, err
			}
		}

		err = d.removeStaleBitmaps(monitor, disk, bitmapName)
		if err != nil {
			return nil, err
		}

		bitmaps, err := monitor.QueryNodeDirtyBitmaps(disk.nodeName())
		if err != nil {
			return nil, fmt.Errorf("Failed querying bitmaps of disk %q: %w", disk.deviceName, err)
		}

		selected = append(selected, snapshotDisk{bitmapDisk: disk, snapshotUUID: snapshotUUID, bitmaps: bitmaps})
	}

	if len(selected) == 0 {
		return []string{}, nil
	}

	revert := revert.New()
	defer revert.Fail()

	deviceNames := make([]string, 0, len(selected))
	closeMetadataImages := make([]func(), 0, len(selected))
	actions := make([]qmp.TransactionAction, 0, len(selected)*2)
	for _, disk := range selected {
		size, err := monitor.BlockNodeSize(disk.nodeName())
		if err != nil {
			return nil, fmt.Errorf("Failed getting size of disk %q: %w", disk.deviceName, err)
		}

		overlayNode := overlayNodeName(disk.deviceName)
		overlayPath := d.overlayPath(disk.volume.UUID)
		removeOverlay, err := d.createQcow2Node(monitor, overlayNode, overlayPath, size, map[string]any{"backing": nil})
		if err != nil {
			return nil, fmt.Errorf("Failed creating overlay of disk %q: %w", disk.deviceName, err)
		}

		revert.Add(func() {
			removeOverlay()
			_ = os.Remove(overlayPath)
		})

		// The snapshot metadata image never reads or writes the volume while the instance has it open, so its
		// data file is a null block device of the size of the volume, which a bitmap of the disk merges into.
		metadataImageNode := metadataImageNodeName(disk.deviceName)
		metadataImagePath := d.snapshotMetadataImagePath(disk.volume.UUID, disk.snapshotUUID)
		err = storagePools.Qcow2CreateMetadataImage(metadataImagePath, size)
		if err != nil {
			return nil, fmt.Errorf("Failed creating snapshot metadata image of disk %q: %w", disk.deviceName, err)
		}

		revert.Add(func() { _ = os.Remove(metadataImagePath) })

		nullDataFile := map[string]any{"driver": "null-co", "size": size, "read-only": true}
		closeMetadataImage, err := d.addQcow2Node(monitor, metadataImageNode, metadataImagePath, false, map[string]any{"data-file": nullDataFile})
		if err != nil {
			return nil, fmt.Errorf("Failed adding snapshot metadata image of disk %q: %w", disk.deviceName, err)
		}

		revert.Add(closeMetadataImage)
		closeMetadataImages = append(closeMetadataImages, closeMetadataImage)
		deviceNames = append(deviceNames, disk.deviceName)

		// The copies are disabled, as they represent the instant of the snapshot.
		for _, bitmap := range disk.bitmaps {
			actions = append(actions, qmp.BlockDirtyBitmapAddAction(metadataImageNode, bitmap.Name, bitmap.Granularity, true, true), qmp.BlockDirtyBitmapMergeAction(metadataImageNode, bitmap.Name, disk.nodeName(), bitmap.Name))
		}

		// The new bitmap is created on the disk node rather than on the overlay, so that it records the writes
		// committed from the overlay.
		actions = append(actions, qmp.BlockDirtyBitmapAddAction(disk.nodeName(), bitmapName, 0, true, false), qmp.BlockDevSnapshotAction(disk.nodeName(), overlayNode))
	}

	err = monitor.RunTransaction(actions)
	if err != nil {
		return nil, fmt.Errorf("Failed creating bitmaps: %w", err)
	}

	// From here on the guest writes to the overlays, which only a commit may remove.
	revert.Success()

	// QEMU writes the bitmaps into a snapshot metadata image when its node is removed.
	for _, closeMetadataImage := range closeMetadataImages {
		closeMetadataImage()
	}

	return deviceNames, nil
}

// RemoveSnapshotMetadataImages deletes from the config volume the snapshot metadata images of the volume snapshots
// that snapshots maps the disk devices to. The config volume snapshot keeps its copies.
func (d *qemu) RemoveSnapshotMetadataImages(snapshots map[string]string) error {
	disks, err := d.disksSupportingBitmaps()
	if err != nil {
		return err
	}

	return d.withConfigVolume(func() error {
		for _, disk := range disks {
			snapshotUUID, ok := snapshots[disk.deviceName]
			if !ok {
				continue
			}

			err := os.Remove(d.snapshotMetadataImagePath(disk.volume.UUID, snapshotUUID))
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
		}

		return nil
	})
}
