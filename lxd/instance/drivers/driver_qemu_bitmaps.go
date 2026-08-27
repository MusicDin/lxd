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

	"go.yaml.in/yaml/v2"
	"golang.org/x/sys/unix"

	"github.com/canonical/lxd/lxd/db"
	dbCluster "github.com/canonical/lxd/lxd/db/cluster"
	"github.com/canonical/lxd/lxd/device/filters"
	"github.com/canonical/lxd/lxd/instance"
	"github.com/canonical/lxd/lxd/instance/drivers/qmp"
	"github.com/canonical/lxd/lxd/project"
	storagePools "github.com/canonical/lxd/lxd/storage"
	storageDrivers "github.com/canonical/lxd/lxd/storage/drivers"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/features"
	"github.com/canonical/lxd/shared/logger"
)

// qemuOverlayNodePrefix used as part of the name given the QEMU block nodes of overlays. Device names may contain
// underscores, so a suffix on qemuDeviceNamePrefix would collide with a device named <name>_overlay. No block node
// of a device starts with this prefix.
const qemuOverlayNodePrefix = "lxdoverlay_"

// qemuStoreNodePrefix used as part of the name given the store node of a disk, the QEMU block node over its volume
// metadata image, and to the fd set that passes the image to QEMU.
const qemuStoreNodePrefix = "lxdimage_"

// blockNodeName returns the QEMU block node name of a disk device, which the guest device is attached to.
func blockNodeName(deviceName string) string {
	return qemuDeviceNameOrID(qemuDeviceNamePrefix, deviceName, "", qemuDeviceNameMaxLength)
}

// overlayNodeName returns the QEMU block node name of the overlay of a disk device.
func overlayNodeName(deviceName string) string {
	return qemuDeviceNameOrID(qemuOverlayNodePrefix, deviceName, "", qemuDeviceNameMaxLength)
}

// storeNodeName returns the QEMU block node name of the store node of a disk device, which is also the name of the
// fd set that passes its volume metadata image to QEMU.
func storeNodeName(deviceName string) string {
	return qemuDeviceNameOrID(qemuStoreNodePrefix, deviceName, "", qemuDeviceNameMaxLength)
}

// qemuMetadataImagesDir is the directory on the config volume that stores the metadata images, the qcow2 images
// that store the bitmaps of the block volumes of the instance. The volume metadata image of a volume is named after
// the UUID of the volume. A snapshot with a bitmap writes the images before the storage snapshot, so that the config
// volume snapshot holds the bitmaps as they were at the instant of the snapshot, and records what they hold in a
// snapshot bitmap file next to them.
const qemuMetadataImagesDir = "metadata_images"

// qemuMetadataImageSuffix is the file name suffix of the metadata images.
const qemuMetadataImageSuffix = ".qcow2"

// qemuOverlaySuffix is the file name suffix of the overlay of a volume, which is stored next to its metadata image.
const qemuOverlaySuffix = ".overlay.qcow2"

// metadataImagesDir returns the directory on the config volume that stores the metadata images.
func (d *qemu) metadataImagesDir() string {
	return filepath.Join(d.Path(), qemuMetadataImagesDir)
}

// volumeMetadataImagePath returns the volume metadata image of a volume, which stores the bitmaps of the volume as
// they were when the store node over it last closed.
func (d *qemu) volumeMetadataImagePath(volumeUUID string) string {
	return filepath.Join(d.metadataImagesDir(), volumeUUID+qemuMetadataImageSuffix)
}

// overlayPath returns the overlay that the guest's writes to a volume go to while a snapshot with a bitmap is
// created. It stays on the config volume until it is committed into the volume.
func (d *qemu) overlayPath(volumeUUID string) string {
	return filepath.Join(d.metadataImagesDir(), volumeUUID+qemuOverlaySuffix)
}

// qemuSnapshotBitmapFilePrefix and qemuSnapshotBitmapFileSuffix frame the name of a snapshot bitmap file,
// snapshot.<name>.yaml, in the metadata images directory.
const qemuSnapshotBitmapFilePrefix = "snapshot."
const qemuSnapshotBitmapFileSuffix = ".yaml"

// snapshotBitmapFile records which bitmaps the volume metadata images of a config volume snapshot hold. A snapshot
// with a bitmap writes it next to the images once they are written, so that the config volume snapshot includes it,
// and removes it from the config volume afterwards. A snapshot without one was not created with a bitmap.
type snapshotBitmapFile struct {
	// Snapshot is the instance snapshot the file was written for.
	Snapshot snapshotBitmapFileSnapshot `yaml:"snapshot"`

	// Volumes holds an entry per disk device whose volume the snapshot created its bitmap on.
	Volumes map[string]snapshotBitmapFileVolume `yaml:"volumes"`
}

// snapshotBitmapFileSnapshot identifies an instance snapshot by its instance snapshot UUID, the UUID of its root
// volume snapshot, and by the name it was created under.
type snapshotBitmapFileSnapshot struct {
	UUID string `yaml:"uuid"`
	Name string `yaml:"name"`
}

// snapshotBitmapFileVolume describes the volume metadata image of one disk device of the snapshot. UUID is the UUID
// of the volume, which the image is named after. Bitmaps are the bitmaps the image holds besides the one created with
// the snapshot, each with the instance snapshot UUID of the snapshot it was created with.
type snapshotBitmapFileVolume struct {
	UUID    string                     `yaml:"uuid"`
	Bitmaps []snapshotBitmapFileBitmap `yaml:"bitmaps"`
}

// snapshotBitmapFileBitmap is a bitmap of a volume metadata image with the instance snapshot UUID of the snapshot it
// was created with and its granularity in bytes.
type snapshotBitmapFileBitmap struct {
	Name        string `yaml:"name"`
	UUID        string `yaml:"uuid"`
	Granularity int64  `yaml:"granularity"`
}

// snapshotBitmapFilePath returns the snapshot bitmap file of the snapshot of the given name in the given metadata
// images directory.
func snapshotBitmapFilePath(dir string, snapshotName string) string {
	return filepath.Join(dir, qemuSnapshotBitmapFilePrefix+snapshotName+qemuSnapshotBitmapFileSuffix)
}

// writeSnapshotBitmapFile writes the snapshot bitmap file into the given metadata images directory, named after the
// snapshot it records.
func writeSnapshotBitmapFile(dir string, file *snapshotBitmapFile) error {
	data, err := yaml.Marshal(file)
	if err != nil {
		return err
	}

	return os.WriteFile(snapshotBitmapFilePath(dir, file.Snapshot.Name), data, 0600)
}

// readSnapshotBitmapFile returns the snapshot bitmap file of the given metadata images directory that records the
// instance snapshot of the given UUID, or nil when there is none. The files are matched by the UUID they record
// rather than by name, because a snapshot renamed after its creation keeps the file of its name at creation, and a
// file left by a failed snapshot can sit next to it.
func readSnapshotBitmapFile(dir string, snapshotUUID string) (*snapshotBitmapFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}

		return nil, err
	}

	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), qemuSnapshotBitmapFilePrefix) || !strings.HasSuffix(entry.Name(), qemuSnapshotBitmapFileSuffix) {
			continue
		}

		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}

		var file snapshotBitmapFile
		err = yaml.Unmarshal(data, &file)
		if err != nil {
			return nil, fmt.Errorf("Failed parsing snapshot bitmap file %q: %w", entry.Name(), err)
		}

		if file.Snapshot.UUID == snapshotUUID {
			return &file, nil
		}
	}

	return nil, nil
}

// metadataImagesEnabled reports whether the bitmaps of the block disks of the instance are stored in their volume
// metadata images. A live migration target starts without them, because the images on its config volume are the ones
// of the source, which still has them open, and it removes them once the migration is complete.
func (d *qemu) metadataImagesEnabled() bool {
	return features.IsEnabled(features.ChangedBlockTracking) && d.migrationReceiveStateful == nil
}

// withInstanceMounted runs task with the volumes of the instance, or of the instance snapshot, mounted. The config
// volume stores the metadata images. An instance whose QEMU process runs has its volumes mounted, and they are not
// mounted again, because a mount of a volume that the instance holds leaves an LVM logical volume active once the
// instance releases it. The process is checked rather than the state, because the stop hook reports the instance as
// running while it holds the stop lock, after the process ended and before the devices release the volumes. The
// mount then only counts, and the mount info gives the block device of the root volume. It is nil for a running
// instance.
func (d *qemu) withInstanceMounted(task func(mountInfo *storagePools.MountInfo) error) error {
	if !d.IsSnapshot() {
		pid, _ := d.pid()
		if pid > 0 {
			return task(nil)
		}
	}

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

	// A writable node is reopened read-only once it becomes the backing node of an overlay, which needs a
	// read-only descriptor in the fd set. The reopen fails without it after the persistent bitmaps of the node
	// were already marked read-only, and QEMU then rejects every write to the node.
	if !readOnly {
		roFile, err := os.OpenFile(path, unix.O_RDONLY, 0)
		if err != nil {
			_ = monitor.RemoveFDFromFDSet(fdSetName)
			return nil, nil, fmt.Errorf("Failed opening image %q: %w", path, err)
		}

		defer func() { _ = roFile.Close() }()

		err = monitor.AddFileToFDSet(info.ID, fdSetName, roFile, true)
		if err != nil {
			_ = monitor.RemoveFDFromFDSet(fdSetName)
			return nil, nil, fmt.Errorf("Failed sending read-only file descriptor of %q for block node %q: %w", path, nodeName, err)
		}
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

// nodeName returns the disk node of the disk, the block node of its volume that the guest device is attached to.
func (disk bitmapDisk) nodeName() string {
	return blockNodeName(disk.deviceName)
}

// storeNodeName returns the store node of the disk, the qcow2 node over its volume metadata image.
func (disk bitmapDisk) storeNodeName() string {
	return storeNodeName(disk.deviceName)
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
// the size of the disk node, which is the size of the volume, and returns its path and that size. An image of
// another size is replaced, because the volume was resized and the bitmaps of the image no longer match it.
func (d *qemu) prepareVolumeMetadataImage(monitor *qmp.Monitor, disk bitmapDisk) (string, int64, error) {
	size, err := monitor.BlockNodeSize(disk.nodeName())
	if err != nil {
		return "", 0, fmt.Errorf("Failed getting size of disk %q: %w", disk.deviceName, err)
	}

	path := d.volumeMetadataImagePath(disk.volume.UUID)
	if shared.PathExists(path) {
		imageSize, err := storagePools.Qcow2VirtualSize(path)
		if err == nil && imageSize == size {
			return path, size, nil
		}

		d.logger.Warn("Replacing metadata image that does not match the volume", logger.Ctx{"device": disk.deviceName, "imageSize": imageSize, "volumeSize": size, "err": err})
	}

	err = os.MkdirAll(d.metadataImagesDir(), 0700)
	if err != nil {
		return "", 0, fmt.Errorf("Failed creating metadata images directory: %w", err)
	}

	err = storagePools.Qcow2CreateMetadataImage(path, size)
	if err != nil {
		return "", 0, err
	}

	return path, size, nil
}

// nullBlockDev returns the options of a read-only null block device of the given size, which stands in for the
// volume as the data file of a metadata image node. Nothing reads or writes guest data through such a node.
func nullBlockDev(size int64) map[string]any {
	return map[string]any{"driver": "null-co", "size": size, "read-only": true}
}

// addStoreNode adds the store node of the disk to the running QEMU process, a writable qcow2 node over the volume
// metadata image of its volume without a guest device, with a null block device of the size of the disk node as its
// data file. The disk node must exist. The persistent bitmaps of the image load with the node. The returned function
// removes the store node and its fd set, and removing the node writes the bitmaps of the volume into the image.
func (d *qemu) addStoreNode(monitor *qmp.Monitor, disk bitmapDisk) (func(), error) {
	path, size, err := d.prepareVolumeMetadataImage(monitor, disk)
	if err != nil {
		return nil, err
	}

	return d.addQcow2Node(monitor, disk.storeNodeName(), path, false, map[string]any{"data-file": nullBlockDev(size)})
}

// bitmapPair is a bitmap of the volume of a disk as the running QEMU process holds it. The disk bitmap is the
// transient bitmap of the disk node, which records the writes of this run. The store bitmap is the persistent,
// disabled bitmap of the same name on the store node, which holds the writes of the earlier runs and which the disk
// bitmap is merged into before the process ends. A half is nil when its node lacks the bitmap.
type bitmapPair struct {
	name  string
	disk  *qmp.BlockDirtyInfo
	store *qmp.BlockDirtyInfo
}

// valid reports whether the bitmap recorded every write since it was created: both halves exist, the disk bitmap is
// recording and neither half is inconsistent.
func (pair bitmapPair) valid() bool {
	return pair.disk != nil && pair.store != nil && pair.disk.Recording && !pair.disk.Inconsistent && !pair.store.Inconsistent
}

// queryBitmapPairs returns the bitmaps of the disk node and of the store node of the disk as pairs, sorted by name.
// Both nodes must exist.
func (d *qemu) queryBitmapPairs(monitor *qmp.Monitor, disk bitmapDisk) ([]bitmapPair, error) {
	diskBitmaps, err := monitor.QueryNodeDirtyBitmaps(disk.nodeName())
	if err != nil {
		return nil, fmt.Errorf("Failed querying bitmaps of disk %q: %w", disk.deviceName, err)
	}

	storeBitmaps, err := monitor.QueryNodeDirtyBitmaps(disk.storeNodeName())
	if err != nil {
		return nil, fmt.Errorf("Failed querying bitmaps of the volume metadata image of disk %q: %w", disk.deviceName, err)
	}

	byName := make(map[string]*bitmapPair, len(storeBitmaps))
	for i := range diskBitmaps {
		byName[diskBitmaps[i].Name] = &bitmapPair{name: diskBitmaps[i].Name, disk: &diskBitmaps[i]}
	}

	for i := range storeBitmaps {
		pair, found := byName[storeBitmaps[i].Name]
		if !found {
			pair = &bitmapPair{name: storeBitmaps[i].Name}
			byName[storeBitmaps[i].Name] = pair
		}

		pair.store = &storeBitmaps[i]
	}

	pairs := make([]bitmapPair, 0, len(byName))
	for _, pair := range byName {
		pairs = append(pairs, *pair)
	}

	slices.SortFunc(pairs, func(a bitmapPair, b bitmapPair) int { return strings.Compare(a.name, b.name) })

	return pairs, nil
}

// removeBitmapPair removes the bitmap from the nodes of the disk that have it.
func (d *qemu) removeBitmapPair(monitor *qmp.Monitor, disk bitmapDisk, pair bitmapPair) error {
	if pair.disk != nil {
		err := monitor.RemoveDirtyBitmap(disk.nodeName(), pair.name)
		if err != nil {
			return fmt.Errorf("Failed removing bitmap %q of disk %q: %w", pair.name, disk.deviceName, err)
		}
	}

	if pair.store != nil {
		err := monitor.RemoveDirtyBitmap(disk.storeNodeName(), pair.name)
		if err != nil {
			return fmt.Errorf("Failed removing bitmap %q of the volume metadata image of disk %q: %w", pair.name, disk.deviceName, err)
		}
	}

	return nil
}

// refreshStoreNode replaces the store node of a disk of the running instance when the disk node no longer has the
// size of the store node, because the volume was resized. The store node is removed, the volume metadata image is
// created again at the size of the disk node and the store node is added again, without bitmaps. A disk without a
// store node is left as it is.
func (d *qemu) refreshStoreNode(monitor *qmp.Monitor, disk bitmapDisk) error {
	nodeNames, err := monitor.QueryNamedBlockNodes()
	if err != nil {
		return err
	}

	if !slices.Contains(nodeNames, disk.storeNodeName()) {
		return nil
	}

	diskSize, err := monitor.BlockNodeSize(disk.nodeName())
	if err != nil {
		return fmt.Errorf("Failed getting size of disk %q: %w", disk.deviceName, err)
	}

	storeSize, err := monitor.BlockNodeSize(disk.storeNodeName())
	if err != nil {
		return fmt.Errorf("Failed getting size of the volume metadata image of disk %q: %w", disk.deviceName, err)
	}

	if diskSize == storeSize {
		return nil
	}

	d.removeQcow2Node(monitor, disk.storeNodeName())

	_, err = d.addStoreNode(monitor, disk)
	if err != nil {
		return fmt.Errorf("Failed adding volume metadata image of disk %q: %w", disk.deviceName, err)
	}

	return nil
}

// pruneMetadataImages deletes from the metadata images directory every file that is not the volume metadata image
// or the overlay of a volume attached through one of the given disks. A snapshot bitmap file left on the config
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

// snapshotMetadataImage is the snapshot metadata image of a volume snapshot, the volume metadata image of the volume
// as the config volume snapshot of an instance snapshot holds it.
type snapshotMetadataImage struct {
	path           string
	deviceName     string
	volumeUUID     string
	snapshotUUID   string
	bitmaps        []snapshotBitmapFileBitmap // The bitmaps the image holds, as the snapshot bitmap file records them.
	pool           string                     // Pool of the custom volume snapshot, empty for the root volume snapshot.
	volumeSnapshot string                     // Name of the custom volume snapshot, empty for the root volume snapshot.
}

// snapshotMetadataImages returns the snapshot metadata images that the snapshot bitmap file of the instance snapshot
// records, with the disk device each volume was attached through. The image of the root volume snapshot belongs to
// the snapshot itself, and the image of a custom volume snapshot to the snapshot the instance snapshot records for
// the device in volatile.attached_volumes, which must exist and belong to the volume the file records. The config
// volume snapshot must be mounted.
func (d *qemu) snapshotMetadataImages() ([]snapshotMetadataImage, error) {
	pool, err := d.getStoragePool()
	if err != nil {
		return nil, err
	}

	rootSnapVol, err := storagePools.VolumeDBGet(pool, d.project.Name, d.name, storageDrivers.VolumeTypeVM)
	if err != nil {
		return nil, err
	}

	file, err := readSnapshotBitmapFile(d.metadataImagesDir(), rootSnapVol.Config["volatile.uuid"])
	if err != nil {
		return nil, err
	}

	if file == nil {
		return nil, nil
	}

	rootDiskName, _, err := d.getRootDiskDevice()
	if err != nil {
		return nil, fmt.Errorf("Failed getting root disk: %w", err)
	}

	attachedVolumes, err := parseVolatileAttachedVolumes(d)
	if err != nil {
		return nil, err
	}

	volProject := project.StorageVolumeProjectFromRecord(&d.project, dbCluster.StoragePoolVolumeTypeCustom)
	customType := dbCluster.StoragePoolVolumeTypeCustom
	pools := map[string]storagePools.Pool{pool.Name(): pool}
	images := []snapshotMetadataImage{}
	for deviceName, volume := range file.Volumes {
		image := snapshotMetadataImage{
			path:       d.volumeMetadataImagePath(volume.UUID),
			deviceName: deviceName,
			volumeUUID: volume.UUID,
			bitmaps:    volume.Bitmaps,
		}

		if deviceName == rootDiskName {
			image.snapshotUUID = file.Snapshot.UUID
			images = append(images, image)
			continue
		}

		snapshotUUID, ok := attachedVolumes[deviceName]
		if !ok {
			d.logger.Warn("Skipping snapshot metadata image of a device whose volume snapshot the snapshot does not record", logger.Ctx{"device": deviceName})
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

		if dbVol.Config["volatile.uuid"] != volume.UUID {
			d.logger.Warn("Skipping snapshot metadata image of a volume snapshot that belongs to another volume", logger.Ctx{"device": deviceName, "snapshotUUID": snapshotUUID, "volumeUUID": volume.UUID})
			continue
		}

		image.snapshotUUID = snapshotUUID
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
			if !slices.Contains(nodeNames, disk.nodeName()) || !slices.Contains(nodeNames, disk.storeNodeName()) {
				continue
			}

			pairs, err := d.queryBitmapPairs(monitor, disk)
			if err != nil {
				return nil, err
			}

			for _, pair := range pairs {
				if pair.store == nil {
					continue
				}

				entries = append(entries, bitmapEntry{name: pair.name, volume: disk.volume, granularity: int64(pair.store.Granularity), recording: pair.valid()})
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
				entries = append(entries, bitmapEntry{name: bitmap.Name, volume: disk.volume, granularity: bitmap.Granularity, recording: bitmap.Valid})
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return groupBitmaps(entries, uuids), nil
}

// snapshotBitmaps returns the bitmaps that the snapshot bitmap file of the instance snapshot lists, with the instance
// snapshot UUID and the granularity the file records for each. The file was written once the volume metadata images
// were verified, so the images are not read. The bitmap created with the snapshot is not listed. Every bitmap of a
// snapshot is disabled, so none is recording.
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

		for _, image := range images {
			volume, ok := volumes[image.deviceName]
			if !ok {
				d.logger.Warn("Skipping snapshot metadata image of a volume whose disk device the snapshot does not record", logger.Ctx{"device": image.deviceName, "volumeUUID": image.volumeUUID})
				continue
			}

			volume.UUID = image.volumeUUID

			for _, bitmap := range image.bitmaps {
				uuids[bitmap.Name] = bitmap.UUID
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

// removeBitmaps removes from the given disks every bitmap whose name matches, from both nodes of a running instance
// and from the volume metadata images of a stopped one. The snapshot metadata images keep their copies, as a
// snapshot is never modified.
func (d *qemu) removeBitmaps(disks []bitmapDisk, match func(bitmapName string) bool) error {
	if d.IsRunning() {
		monitor, err := qmp.Connect(d.monitorPath(), qemuSerialChardevName, d.getMonitorEventHandler())
		if err != nil {
			return err
		}

		return d.removeLiveBitmaps(monitor, disks, match)
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

// removeLiveBitmaps removes from both nodes of the given disks of the running instance every bitmap whose name
// matches. A disk without both nodes is skipped.
func (d *qemu) removeLiveBitmaps(monitor *qmp.Monitor, disks []bitmapDisk, match func(bitmapName string) bool) error {
	nodeNames, err := monitor.QueryNamedBlockNodes()
	if err != nil {
		return err
	}

	for _, disk := range disks {
		if !slices.Contains(nodeNames, disk.nodeName()) || !slices.Contains(nodeNames, disk.storeNodeName()) {
			continue
		}

		pairs, err := d.queryBitmapPairs(monitor, disk)
		if err != nil {
			return err
		}

		for _, pair := range pairs {
			if !match(pair.name) {
				continue
			}

			err = d.removeBitmapPair(monitor, disk, pair)
			if err != nil {
				return err
			}
		}
	}

	return nil
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

	// The running instance keeps its store node open and writes the bitmaps created later into the image when the
	// node closes, so the image is kept and only its bitmaps are removed. A volume resized while the instance runs
	// gets a store node of the new size.
	if d.IsRunning() {
		monitor, err := qmp.Connect(d.monitorPath(), qemuSerialChardevName, d.getMonitorEventHandler())
		if err != nil {
			return err
		}

		disk := bitmapDisk{deviceName: deviceName, volume: *volume}
		err = d.removeLiveBitmaps(monitor, []bitmapDisk{disk}, func(string) bool { return true })
		if err != nil {
			return err
		}

		return d.refreshStoreNode(monitor, disk)
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
