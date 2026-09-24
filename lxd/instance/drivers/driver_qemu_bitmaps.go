package drivers

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"

	"github.com/canonical/lxd/lxd/db"
	dbCluster "github.com/canonical/lxd/lxd/db/cluster"
	"github.com/canonical/lxd/lxd/db/warningtype"
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
	"github.com/canonical/lxd/shared/logger"
	"github.com/canonical/lxd/shared/revert"
)

// qemuOverlayNodePrefix used as part of the name given the QEMU block nodes of overlays. Device names may
// contain underscores, so a suffix on qemuDeviceNamePrefix would collide with a device named <name>_overlay.
// No block node of a device starts with this prefix.
const qemuOverlayNodePrefix = "lxdoverlay_"

// qemuMetadataImageNodePrefix used as part of the name given the QEMU block nodes of metadata images, the qcow2
// images that store bitmaps. Like qemuOverlayNodePrefix it cannot be a suffix on qemuDeviceNamePrefix.
const qemuMetadataImageNodePrefix = "lxdmeta_"

// blockNodeName returns the QEMU block node name of a disk device.
func blockNodeName(deviceName string) string {
	return qemuDeviceNameOrID(qemuDeviceNamePrefix, deviceName, "", qemuDeviceNameMaxLength)
}

// overlayNodeName returns the QEMU block node name of the overlay of a disk device.
func overlayNodeName(deviceName string) string {
	return qemuDeviceNameOrID(qemuOverlayNodePrefix, deviceName, "", qemuDeviceNameMaxLength)
}

// metadataImageNodeName returns the QEMU block node name of the metadata image of a disk device.
func metadataImageNodeName(deviceName string) string {
	return qemuDeviceNameOrID(qemuMetadataImageNodePrefix, deviceName, "", qemuDeviceNameMaxLength)
}

// createQcow2 creates an empty qcow2 image of the given virtual size at path, replacing any existing file.
func (d *qemu) createQcow2(path string, size int64) error {
	err := os.Remove(path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	_, err = shared.RunCommand(d.state.ShutdownCtx, "qemu-img", "create", "-f", "qcow2", path, strconv.FormatInt(size, 10))
	if err != nil {
		// A failed creation can leave a partially written file behind.
		_ = os.Remove(path)
		return fmt.Errorf("Failed creating image %q: %w", path, err)
	}

	return nil
}

// addQcow2Node passes file to the running QEMU process by file descriptor and adds it as a qcow2 block node of the
// given name, without a guest visible device. The returned function removes the node and its file descriptor set,
// which closes the file in QEMU.
func (d *qemu) addQcow2Node(monitor *qmp.Monitor, nodeName string, file *os.File, readOnly bool) (func(), error) {
	info, err := monitor.SendFileWithFDSet(nodeName, file, readOnly)
	if err != nil {
		return nil, fmt.Errorf("Failed sending file descriptor of %q for block node %q: %w", file.Name(), nodeName, err)
	}

	revert := revert.New()
	defer revert.Fail()

	revert.Add(func() { _ = monitor.RemoveFDFromFDSet(nodeName) })

	err = monitor.AddBlockDevice(map[string]any{
		"driver":    "qcow2",
		"node-name": nodeName,
		"read-only": readOnly,
		"file": map[string]any{
			"driver":   "file",
			"filename": fmt.Sprintf("/dev/fdset/%d", info.ID),
		},
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("Failed adding block node %q: %w", nodeName, err)
	}

	revert.Success()
	return func() { d.removeQcow2Node(monitor, nodeName) }, nil
}

// removeQcow2Node removes a block node added by addQcow2Node together with its file descriptor set.
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
// process as a writable block node of the given name. The returned function removes the node, which writes any
// persistent bitmap of the node into the file, and leaves the file in place.
func (d *qemu) createQcow2Node(monitor *qmp.Monitor, nodeName string, path string, size int64) (func(), error) {
	err := os.MkdirAll(filepath.Dir(path), 0700)
	if err != nil {
		return nil, fmt.Errorf("Failed creating directory of image %q: %w", path, err)
	}

	err = d.createQcow2(path, size)
	if err != nil {
		return nil, err
	}

	revert := revert.New()
	defer revert.Fail()

	revert.Add(func() { _ = os.Remove(path) })

	file, err := os.OpenFile(path, unix.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("Failed opening image %q: %w", path, err)
	}

	// Closed as soon as QEMU has its own descriptor, so that it does not prevent a clean unmount on stop.
	defer func() { _ = file.Close() }()

	removeNode, err := d.addQcow2Node(monitor, nodeName, file, false)
	if err != nil {
		return nil, err
	}

	revert.Success()
	return removeNode, nil
}

// openQcow2Node adds the existing qcow2 image at path to the running QEMU process as a block node of the given
// name. The returned function removes the node.
func (d *qemu) openQcow2Node(monitor *qmp.Monitor, nodeName string, path string, readOnly bool) (func(), error) {
	flags := unix.O_RDWR
	if readOnly {
		flags = unix.O_RDONLY
	}

	file, err := os.OpenFile(path, flags, 0)
	if err != nil {
		return nil, fmt.Errorf("Failed opening image %q: %w", path, err)
	}

	// Closed as soon as QEMU has its own descriptor, so that it does not prevent a clean unmount on stop.
	defer func() { _ = file.Close() }()

	return d.addQcow2Node(monitor, nodeName, file, readOnly)
}

// addOverlay adds an empty qcow2 overlay block node of the given virtual size to the running QEMU process, for
// blockdev-snapshot to use as the overlay of a disk. The overlay file is created on the instance config volume so
// that the root disk's size.state property limits its growth, and it is unlinked as soon as QEMU has opened it so
// that it is deleted when QEMU exits. The returned function removes the overlay block node and its file descriptor set.
func (d *qemu) addOverlay(monitor *qmp.Monitor, overlayNode string, size int64) (func(), error) {
	overlayFile := filepath.Join(d.Path(), overlayNode+".qcow2")

	removeOverlay, err := d.createQcow2Node(monitor, overlayNode, overlayFile, size)
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

// qemuMetadataImagesDir is the directory on the instance config volume that stores the metadata images, the qcow2
// images that store the bitmaps of the block volumes of the instance. The images are named after the UUID of their
// volume and, for the metadata image of a snapshot, the UUID of the volume snapshot. A snapshot metadata image is
// created on the config volume before the storage snapshot, so that the config volume snapshot includes it, and
// removed from the config volume afterwards.
const qemuMetadataImagesDir = "metadata_images"

// qemuMetadataImageSuffix is the file name suffix of the metadata images.
const qemuMetadataImageSuffix = ".qcow2"

// qemuOverlaySuffix is the file name suffix of the overlay of a volume, which is stored next to its metadata image.
const qemuOverlaySuffix = ".overlay.qcow2"

// qemuBitmapUUIDSeparator separates the UUID of a bitmap from its name in the name QEMU knows the bitmap by.
const qemuBitmapUUIDSeparator = "/"

// qemuBitmapName returns the name QEMU knows a bitmap by, the name of the bitmap prefixed with the UUID generated
// when it was created. A snapshot of the same name created later gets a bitmap of another UUID, so the UUID tells
// the two apart.
func qemuBitmapName(bitmapUUID string, bitmapName string) string {
	return bitmapUUID + qemuBitmapUUIDSeparator + bitmapName
}

// parseQEMUBitmapName returns the UUID and the name of a bitmap from the name QEMU knows it by. It returns false
// for a bitmap that LXD did not create, whose name has no UUID prefix.
func parseQEMUBitmapName(name string) (bitmapUUID string, bitmapName string, ok bool) {
	bitmapUUID, bitmapName, found := strings.Cut(name, qemuBitmapUUIDSeparator)
	if !found || bitmapName == "" || uuid.Validate(bitmapUUID) != nil {
		return "", "", false
	}

	return bitmapUUID, bitmapName, true
}

// metadataImagesDir returns the directory on the instance config volume that stores the metadata images.
func (d *qemu) metadataImagesDir() string {
	return filepath.Join(d.Path(), qemuMetadataImagesDir)
}

// volumeMetadataImagePath returns the metadata image that stores the bitmaps of a volume while the instance is
// stopped.
func (d *qemu) volumeMetadataImagePath(volumeUUID string) string {
	return filepath.Join(d.metadataImagesDir(), volumeUUID+qemuMetadataImageSuffix)
}

// overlayPath returns the overlay that the guest's writes to a volume go to while a snapshot with a bitmap is
// created. It stays on the config volume until it is committed into the volume.
func (d *qemu) overlayPath(volumeUUID string) string {
	return filepath.Join(d.metadataImagesDir(), volumeUUID+qemuOverlaySuffix)
}

// snapshotMetadataImagePath returns the metadata image that stores the bitmaps of a volume as they were when the
// volume snapshot of the given UUID was created.
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

// withConfigVolume runs task with the config volume of the instance, or of the instance snapshot, mounted. The
// config volume stores the metadata images and is otherwise mounted only while the instance runs.
func (d *qemu) withConfigVolume(task func() error) error {
	pool, err := d.getStoragePool()
	if err != nil {
		return err
	}

	if d.IsSnapshot() {
		_, err = pool.MountInstanceSnapshot(d, nil)
		if err != nil {
			return err
		}

		defer func() {
			err := pool.UnmountInstanceSnapshot(d, nil)
			if err != nil {
				d.logger.Warn("Failed unmounting config volume of snapshot", logger.Ctx{"err": err})
			}
		}()

		return task()
	}

	_, err = pool.MountInstance(d, nil)
	if err != nil {
		return err
	}

	defer func() {
		err := pool.UnmountInstance(d, nil)
		if err != nil && !errors.Is(err, storageDrivers.ErrInUse) {
			d.logger.Warn("Failed unmounting config volume", logger.Ctx{"err": err})
		}
	}()

	return task()
}

// bitmapDisk describes a disk device whose volume supports bitmaps, which is the root disk or a custom block volume
// that is not shared, as the bitmaps of the instance's QEMU process record every write to such a volume.
type bitmapDisk struct {
	deviceName string
	nodeName   string
	volume     api.InstanceBitmapVolume
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
			d.logger.Debug("Skipping disk whose volume does not support bitmaps", logger.Ctx{"device": deviceName})
			continue
		}

		disks = append(disks, bitmapDisk{deviceName: deviceName, nodeName: blockNodeName(deviceName), volume: *volume})
	}

	slices.SortFunc(disks, func(a bitmapDisk, b bitmapDisk) int { return strings.Compare(a.deviceName, b.deviceName) })

	return disks, nil
}

// snapshotMetadataImage is the metadata image of a volume snapshot, found on the config volume snapshot of an
// instance snapshot.
type snapshotMetadataImage struct {
	path         string
	deviceName   string
	volumeUUID   string
	snapshotUUID string
}

// snapshotMetadataImages returns the metadata images of the volume snapshots that the instance snapshot records,
// which are the root volume snapshot and the attached volume snapshots, with the disk device each volume was
// attached through. The config volume snapshot must be mounted.
func (d *qemu) snapshotMetadataImages() ([]snapshotMetadataImage, error) {
	pool, err := d.getStoragePool()
	if err != nil {
		return nil, err
	}

	dbVol, err := storagePools.VolumeDBGet(pool, d.project.Name, d.name, storageDrivers.VolumeTypeVM)
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

	devices := map[string]string{dbVol.Config["volatile.uuid"]: rootDiskName}
	for deviceName, snapshotUUID := range attachedVolumes {
		devices[snapshotUUID] = deviceName
	}

	entries, err := os.ReadDir(d.metadataImagesDir())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}

		return nil, err
	}

	images := []snapshotMetadataImage{}
	for _, entry := range entries {
		volumeUUID, snapshotUUID, ok := parseSnapshotMetadataImageName(entry.Name())
		if !ok {
			continue
		}

		deviceName, ok := devices[snapshotUUID]
		if !ok {
			continue
		}

		images = append(images, snapshotMetadataImage{
			path:         filepath.Join(d.metadataImagesDir(), entry.Name()),
			deviceName:   deviceName,
			volumeUUID:   volumeUUID,
			snapshotUUID: snapshotUUID,
		})
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

// SnapshotMetadataImages returns the metadata images of the volume snapshots that the instance snapshot records,
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
				Path:         image.path,
				VolumeUUID:   image.volumeUUID,
				SnapshotUUID: image.snapshotUUID,
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return images, nil
}

// pruneMetadataImages deletes from the metadata images directory every file that is not the metadata image or the
// overlay of a volume attached through one of the given disks. A snapshot metadata image left on the config volume
// by a failed snapshot and the metadata image of a detached volume are removed this way.
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

// bitmapEntry is a bitmap found on one volume.
type bitmapEntry struct {
	qemuName    string
	volume      api.InstanceBitmapVolume
	granularity int64
	recording   bool
}

// groupBitmaps groups the bitmaps found on the volumes by UUID and name, sorted by name. A bitmap whose name has no
// UUID prefix is left out, as it is either the copy of a bitmap named after the bitmap alone, which a snapshot
// metadata image stores next to the copy named after the UUID and the bitmap, or a bitmap LXD did not create.
func groupBitmaps(entries []bitmapEntry) []api.InstanceBitmap {
	byName := make(map[string]*api.InstanceBitmap)
	for _, entry := range entries {
		bitmapUUID, bitmapName, ok := parseQEMUBitmapName(entry.qemuName)
		if !ok {
			continue
		}

		bitmap, found := byName[entry.qemuName]
		if !found {
			bitmap = &api.InstanceBitmap{Name: bitmapName, UUID: bitmapUUID}
			byName[entry.qemuName] = bitmap
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
		return strings.Compare(a.Name+qemuBitmapUUIDSeparator+a.UUID, b.Name+qemuBitmapUUIDSeparator+b.UUID)
	})

	return bitmaps
}

// Bitmaps returns the bitmaps of the block volumes of the instance, grouped by name with one entry per volume. On a
// running instance they are read from its QEMU process, on a stopped instance from the volume metadata images on its
// config volume, and on an instance snapshot from the snapshot metadata images on its config volume snapshot.
func (d *qemu) Bitmaps() ([]api.InstanceBitmap, error) {
	if d.IsSnapshot() {
		return d.snapshotBitmaps()
	}

	disks, err := d.disksSupportingBitmaps()
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
			if !slices.Contains(nodeNames, disk.nodeName) {
				continue
			}

			bitmaps, err := monitor.QueryNodeDirtyBitmaps(disk.nodeName)
			if err != nil {
				return nil, fmt.Errorf("Failed querying bitmaps of disk %q: %w", disk.deviceName, err)
			}

			for _, bitmap := range bitmaps {
				entries = append(entries, bitmapEntry{qemuName: bitmap.Name, volume: disk.volume, granularity: int64(bitmap.Granularity), recording: bitmap.Recording})
			}
		}

		return groupBitmaps(entries), nil
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
				entries = append(entries, bitmapEntry{qemuName: bitmap.Name, volume: disk.volume, granularity: bitmap.Granularity, recording: bitmap.Recording})
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return groupBitmaps(entries), nil
}

// snapshotBitmaps returns the bitmaps stored in the snapshot metadata images of the instance snapshot.
func (d *qemu) snapshotBitmaps() ([]api.InstanceBitmap, error) {
	entries := []bitmapEntry{}
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
				d.logger.Warn("Skipping metadata image of a volume whose disk device the snapshot does not record", logger.Ctx{"device": image.deviceName, "volumeUUID": image.volumeUUID})
				continue
			}

			volume.UUID = image.volumeUUID

			bitmaps, err := storagePools.Qcow2Bitmaps(image.path)
			if err != nil {
				return err
			}

			for _, bitmap := range bitmaps {
				entries = append(entries, bitmapEntry{qemuName: bitmap.Name, volume: volume, granularity: bitmap.Granularity, recording: bitmap.Recording})
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return groupBitmaps(entries), nil
}

// removeNodeBitmaps deletes the bitmaps of a block node whose names match, and returns how many it deleted.
func (d *qemu) removeNodeBitmaps(monitor *qmp.Monitor, disk bitmapDisk, match func(qemuName string) bool) (int, error) {
	bitmaps, err := monitor.QueryNodeDirtyBitmaps(disk.nodeName)
	if err != nil {
		return 0, fmt.Errorf("Failed querying bitmaps of disk %q: %w", disk.deviceName, err)
	}

	removed := 0
	for _, bitmap := range bitmaps {
		if !match(bitmap.Name) {
			continue
		}

		err = monitor.RemoveDirtyBitmap(disk.nodeName, bitmap.Name)
		if err != nil {
			return removed, fmt.Errorf("Failed deleting bitmap %q of disk %q: %w", bitmap.Name, disk.deviceName, err)
		}

		removed++
	}

	return removed, nil
}

// removeImageBitmaps deletes the bitmaps of a metadata image whose names match, and returns how many it deleted.
// An image that does not exist has no bitmaps.
func (d *qemu) removeImageBitmaps(path string, match func(qemuName string) bool) (int, error) {
	if !shared.PathExists(path) {
		return 0, nil
	}

	bitmaps, err := storagePools.Qcow2Bitmaps(path)
	if err != nil {
		return 0, err
	}

	removed := 0
	for _, bitmap := range bitmaps {
		if !match(bitmap.Name) {
			continue
		}

		err = storagePools.Qcow2RemoveBitmap(path, bitmap.Name)
		if err != nil {
			return removed, err
		}

		removed++
	}

	return removed, nil
}

// DeleteBitmap deletes the named bitmap from every block volume of the instance, from the QEMU process of a running
// instance and from the volume metadata images of a stopped one. The snapshot metadata images keep their copies, as
// a snapshot is never modified.
func (d *qemu) DeleteBitmap(bitmapName string) error {
	if d.IsSnapshot() {
		return api.StatusErrorf(http.StatusBadRequest, "Bitmaps cannot be deleted from a snapshot")
	}

	disks, err := d.disksSupportingBitmaps()
	if err != nil {
		return err
	}

	match := func(qemuName string) bool {
		_, name, ok := parseQEMUBitmapName(qemuName)
		return ok && name == bitmapName
	}

	removed := 0
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
			if !slices.Contains(nodeNames, disk.nodeName) {
				continue
			}

			n, err := d.removeNodeBitmaps(monitor, disk, match)
			if err != nil {
				return err
			}

			removed += n
		}
	} else {
		err = d.withConfigVolume(func() error {
			for _, disk := range disks {
				n, err := d.removeImageBitmaps(d.volumeMetadataImagePath(disk.volume.UUID), match)
				if err != nil {
					return err
				}

				removed += n
			}

			return nil
		})
		if err != nil {
			return err
		}
	}

	if removed == 0 {
		return api.StatusErrorf(http.StatusNotFound, "Bitmap not found")
	}

	return nil
}

// DeleteVolumeBitmaps deletes every bitmap of the volume attached through the disk device, from the block node of a
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
// is not required to be in the current devices of the instance.
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
		monitor, err := qmp.Connect(d.monitorPath(), qemuSerialChardevName, d.getMonitorEventHandler())
		if err != nil {
			return err
		}

		nodeNames, err := monitor.QueryNamedBlockNodes()
		if err != nil {
			return err
		}

		disk := bitmapDisk{deviceName: deviceName, nodeName: blockNodeName(deviceName), volume: *volume}
		if slices.Contains(nodeNames, disk.nodeName) {
			_, err = d.removeNodeBitmaps(monitor, disk, func(string) bool { return true })
			if err != nil {
				return err
			}
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

		monitor, err := qmp.Connect(d.monitorPath(), qemuSerialChardevName, d.getMonitorEventHandler())
		if err != nil {
			return err
		}

		nodeNames, err := monitor.QueryNamedBlockNodes()
		if err != nil {
			return err
		}

		for _, disk := range disks {
			if !slices.Contains(nodeNames, disk.nodeName) {
				continue
			}

			_, err = d.removeNodeBitmaps(monitor, disk, func(string) bool { return true })
			if err != nil {
				return err
			}
		}
	}

	return d.RemoveAllMetadataImages()
}

// RemoveVolumeMetadataImage deletes the metadata image of the volume of the given UUID from the config volume.
func (d *qemu) RemoveVolumeMetadataImage(volumeUUID string) error {
	return d.withConfigVolume(func() error {
		err := os.Remove(d.volumeMetadataImagePath(volumeUUID))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}

		return nil
	})
}

// RemoveAllMetadataImages deletes every metadata image from the config volume. None of them matches the volumes of an
// instance that was created by a copy, a refresh, an import or a move, or that was restored from a snapshot.
func (d *qemu) RemoveAllMetadataImages() error {
	return d.withConfigVolume(func() error {
		return os.RemoveAll(d.metadataImagesDir())
	})
}

// commitOverlay commits the overlay of a disk device into its volume, removes the overlay node and deletes the
// overlay file. The commit is retried, because the guest writes to the overlay until it succeeds.
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
		// Until a commit succeeds the volume lacks the guest's writes, so every storage snapshot, copy and backup
		// of it is inconsistent.
		_ = d.state.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
			return tx.UpsertWarning(ctx, d.node, d.project.Name, entity.TypeInstance, d.ID(), warningtype.InstanceDiskOverlayNotCommitted, fmt.Sprintf("The volume of disk %q lacks the writes of the guest since the last snapshot with a bitmap", disk.deviceName))
		})

		return fmt.Errorf("Failed committing overlay of disk %q: %w", disk.deviceName, err)
	}

	d.removeQcow2Node(monitor, overlayNode)

	err = os.Remove(d.overlayPath(disk.volume.UUID))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("Failed removing overlay of disk %q: %w", disk.deviceName, err)
	}

	d.resolveOverlayWarning(monitor)

	return nil
}

// resolveOverlayWarning resolves the warning of a failed commit once no disk of the instance has an overlay.
func (d *qemu) resolveOverlayWarning(monitor *qmp.Monitor) {
	nodeNames, err := monitor.QueryNamedBlockNodes()
	if err != nil {
		return
	}

	for deviceName := range d.expandedDevices {
		if slices.Contains(nodeNames, overlayNodeName(deviceName)) {
			return
		}
	}

	_ = warnings.ResolveWarningsByNodeAndProjectAndTypeAndEntity(d.state.DB.Cluster, d.node, d.project.Name, warningtype.InstanceDiskOverlayNotCommitted, entity.TypeInstance, d.ID())
}

// CreateSnapshotBitmaps creates the bitmap of a snapshot on the volumes attached through the given disk devices and
// copies every bitmap those volumes have into the metadata images of their snapshots, all in one QEMU transaction.
// snapshots maps each disk device to the UUID of the volume snapshot about to be created. The metadata image of a
// snapshot is created on the config volume before the storage snapshot, so that the config volume snapshot includes
// it. The transaction also adds an overlay to each of the volumes, so that the storage snapshots taken afterwards
// match the instant the bitmap was created at, and the guest writes to the overlays until CommitDiskOverlays commits
// them. A disk device whose volume does not support bitmaps is skipped, and the devices that got an overlay are
// returned.
func (d *qemu) CreateSnapshotBitmaps(snapshots map[string]string, bitmapName string, bitmapUUID string) ([]string, error) {
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
		if !ok || !slices.Contains(nodeNames, disk.nodeName) {
			continue
		}

		// An overlay left by a failed commit is committed before a new overlay is added.
		if slices.Contains(nodeNames, overlayNodeName(disk.deviceName)) {
			err = d.commitOverlay(monitor, disk)
			if err != nil {
				return nil, err
			}
		}

		bitmaps, err := monitor.QueryNodeDirtyBitmaps(disk.nodeName)
		if err != nil {
			return nil, fmt.Errorf("Failed querying bitmaps of disk %q: %w", disk.deviceName, err)
		}

		// The name of a bitmap identifies it in the API, so a volume gets one bitmap of a name at a time.
		for _, bitmap := range bitmaps {
			_, name, ok := parseQEMUBitmapName(bitmap.Name)
			if ok && name == bitmapName {
				return nil, api.StatusErrorf(http.StatusConflict, "Bitmap %q already exists on disk %q", bitmapName, disk.deviceName)
			}
		}

		selected = append(selected, snapshotDisk{bitmapDisk: disk, snapshotUUID: snapshotUUID, bitmaps: bitmaps})
	}

	revert := revert.New()
	defer revert.Fail()

	qemuName := qemuBitmapName(bitmapUUID, bitmapName)
	deviceNames := make([]string, 0, len(selected))
	closeMetadataImages := make([]func(), 0, len(selected))
	actions := make([]qmp.TransactionAction, 0, len(selected)*2)
	for _, disk := range selected {
		size, err := monitor.BlockNodeSize(disk.nodeName)
		if err != nil {
			return nil, fmt.Errorf("Failed getting size of disk %q: %w", disk.deviceName, err)
		}

		overlayNode := overlayNodeName(disk.deviceName)
		overlayPath := d.overlayPath(disk.volume.UUID)
		removeOverlay, err := d.createQcow2Node(monitor, overlayNode, overlayPath, size)
		if err != nil {
			return nil, fmt.Errorf("Failed creating overlay of disk %q: %w", disk.deviceName, err)
		}

		revert.Add(func() {
			removeOverlay()
			_ = os.Remove(overlayPath)
		})

		// The metadata image is created at the size of the disk, as a bitmap is only merged between nodes of one size.
		metadataImageNode := metadataImageNodeName(disk.deviceName)
		metadataImagePath := d.snapshotMetadataImagePath(disk.volume.UUID, disk.snapshotUUID)
		closeMetadataImage, err := d.createQcow2Node(monitor, metadataImageNode, metadataImagePath, size)
		if err != nil {
			return nil, fmt.Errorf("Failed creating metadata image of disk %q: %w", disk.deviceName, err)
		}

		revert.Add(func() {
			closeMetadataImage()
			_ = os.Remove(metadataImagePath)
		})

		closeMetadataImages = append(closeMetadataImages, closeMetadataImage)
		deviceNames = append(deviceNames, disk.deviceName)

		// The new bitmap is created on the disk node rather than on the overlay, so that it records the writes
		// committed from the overlay.
		actions = append(actions, qmp.BlockDevSnapshotAction(disk.nodeName, overlayNode), qmp.BlockDirtyBitmapAddAction(disk.nodeName, qemuName, 0, false, false))

		// Each bitmap is copied under the name QEMU knows it by and under the name of the bitmap alone, so that a
		// client of the snapshot export selects it with or without its UUID. The copies are disabled, as they
		// represent the instant of the snapshot.
		for _, bitmap := range disk.bitmaps {
			_, name, ok := parseQEMUBitmapName(bitmap.Name)
			if !ok {
				continue
			}

			for _, copyName := range []string{bitmap.Name, name} {
				actions = append(actions, qmp.BlockDirtyBitmapAddAction(metadataImageNode, copyName, bitmap.Granularity, true, true), qmp.BlockDirtyBitmapMergeAction(metadataImageNode, copyName, disk.nodeName, bitmap.Name))
			}
		}
	}

	if len(actions) == 0 {
		return deviceNames, nil
	}

	err = monitor.RunTransaction(actions)
	if err != nil {
		return nil, fmt.Errorf("Failed creating bitmaps: %w", err)
	}

	// From here on the guest writes to the overlays, which only a commit may remove.
	revert.Success()

	// QEMU writes the bitmaps into a metadata image when its node is removed.
	for _, closeMetadataImage := range closeMetadataImages {
		closeMetadataImage()
	}

	return deviceNames, nil
}

// CommitDiskOverlays commits the overlays of the given disk devices into their volumes. A device without an overlay
// node is skipped. After a failed commit the guest writes to the overlay until the next snapshot with a bitmap, stop,
// start or detach of the device commits it.
func (d *qemu) CommitDiskOverlays(deviceNames []string) error {
	monitor, err := qmp.Connect(d.monitorPath(), qemuSerialChardevName, d.getMonitorEventHandler())
	if err != nil {
		return err
	}

	nodeNames, err := monitor.QueryNamedBlockNodes()
	if err != nil {
		return err
	}

	disks, err := d.disksSupportingBitmaps()
	if err != nil {
		return err
	}

	var errs []error
	for _, disk := range disks {
		if !slices.Contains(deviceNames, disk.deviceName) || !slices.Contains(nodeNames, overlayNodeName(disk.deviceName)) {
			continue
		}

		err := d.commitOverlay(monitor, disk)
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// RemoveSnapshotMetadataImages deletes from the config volume the metadata images of the volume snapshots that
// snapshots maps the disk devices to. The config volume snapshot keeps its copies.
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

// persistBitmaps commits every overlay left uncommitted and copies the bitmaps of every disk that supports them into
// the metadata image of its volume, so that they are recreated at the next start. The guest must be paused, as a
// write after the copy is not recorded. The metadata image of a volume without bitmaps is deleted, so that a copy
// from an earlier stop is not reloaded.
func (d *qemu) persistBitmaps(monitor *qmp.Monitor) error {
	nodeNames, err := monitor.QueryNamedBlockNodes()
	if err != nil {
		return err
	}

	disks, err := d.disksSupportingBitmaps()
	if err != nil {
		return err
	}

	var errs []error
	for _, disk := range disks {
		if !slices.Contains(nodeNames, disk.nodeName) {
			continue
		}

		// The bitmaps record the committed writes, so the overlay is committed before they are copied.
		if slices.Contains(nodeNames, overlayNodeName(disk.deviceName)) {
			err := d.commitOverlay(monitor, disk)
			if err != nil {
				errs = append(errs, err)
			}
		}

		bitmaps, err := monitor.QueryNodeDirtyBitmaps(disk.nodeName)
		if err != nil {
			errs = append(errs, fmt.Errorf("Failed querying bitmaps of disk %q: %w", disk.deviceName, err))
			continue
		}

		metadataImagePath := d.volumeMetadataImagePath(disk.volume.UUID)
		if len(bitmaps) == 0 {
			err = os.Remove(metadataImagePath)
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				errs = append(errs, fmt.Errorf("Failed removing metadata image of disk %q: %w", disk.deviceName, err))
			}

			continue
		}

		size, err := monitor.BlockNodeSize(disk.nodeName)
		if err != nil {
			errs = append(errs, fmt.Errorf("Failed getting size of disk %q: %w", disk.deviceName, err))
			continue
		}

		metadataImageNode := metadataImageNodeName(disk.deviceName)
		closeMetadataImage, err := d.createQcow2Node(monitor, metadataImageNode, metadataImagePath, size)
		if err != nil {
			errs = append(errs, fmt.Errorf("Failed creating metadata image of disk %q: %w", disk.deviceName, err))
			continue
		}

		// The copies keep recording once reloaded, so they are not disabled. A bitmap LXD did not create is not kept.
		actions := make([]qmp.TransactionAction, 0, len(bitmaps)*2)
		for _, bitmap := range bitmaps {
			_, _, ok := parseQEMUBitmapName(bitmap.Name)
			if !ok {
				continue
			}

			actions = append(actions, qmp.BlockDirtyBitmapAddAction(metadataImageNode, bitmap.Name, bitmap.Granularity, true, false), qmp.BlockDirtyBitmapMergeAction(metadataImageNode, bitmap.Name, disk.nodeName, bitmap.Name))
		}

		if len(actions) > 0 {
			err = monitor.RunTransaction(actions)
		}

		// QEMU writes the bitmaps into the metadata image when its node is removed.
		closeMetadataImage()

		if err != nil {
			_ = os.Remove(metadataImagePath)
			errs = append(errs, fmt.Errorf("Failed persisting bitmaps of disk %q: %w", disk.deviceName, err))
		}
	}

	return errors.Join(errs...)
}

// persistAndQuit pauses the guest, persists the bitmaps and asks QEMU to quit. A failure to persist is logged,
// as it must not keep the process from ending.
func (d *qemu) persistAndQuit(monitor *qmp.Monitor) error {
	err := monitor.Pause()
	if err != nil {
		return fmt.Errorf("Failed pausing instance: %w", err)
	}

	err = d.persistBitmaps(monitor)
	if err != nil {
		d.logger.Error("Failed persisting bitmaps", logger.Ctx{"err": err})
	}

	return monitor.Quit()
}

// restoreBitmaps recreates on every disk that supports bitmaps the bitmaps stored in the metadata image of its volume
// and commits an overlay left uncommitted by an earlier stop. It runs before the guest starts, so that every write is
// recorded. The metadata image of a volume is deleted once read, so that a start after a crash does not reload
// bitmaps that missed writes, and a disk whose image cannot be reloaded starts without bitmaps. An overlay that
// cannot be added back to its disk fails the start, because the guest would otherwise run without the writes stored
// in it.
func (d *qemu) restoreBitmaps(monitor *qmp.Monitor) error {
	nodeNames, err := monitor.QueryNamedBlockNodes()
	if err != nil {
		return err
	}

	disks, err := d.disksSupportingBitmaps()
	if err != nil {
		return err
	}

	err = d.pruneMetadataImages(disks)
	if err != nil {
		return fmt.Errorf("Failed pruning metadata images: %w", err)
	}

	for _, disk := range disks {
		if !slices.Contains(nodeNames, disk.nodeName) {
			continue
		}

		metadataImagePath := d.volumeMetadataImagePath(disk.volume.UUID)
		if shared.PathExists(metadataImagePath) {
			err := d.reloadBitmaps(monitor, disk, metadataImagePath)
			if err != nil {
				d.logger.Warn("Failed reloading persisted bitmaps, the disk starts without bitmaps", logger.Ctx{"device": disk.deviceName, "err": err})
			}

			err = os.Remove(metadataImagePath)
			if err != nil {
				return fmt.Errorf("Failed removing metadata image of disk %q: %w", disk.deviceName, err)
			}
		}

		// The bitmaps are reloaded first, so that they record the committed writes.
		overlayPath := d.overlayPath(disk.volume.UUID)
		if !shared.PathExists(overlayPath) {
			continue
		}

		overlayNode := overlayNodeName(disk.deviceName)
		removeOverlay, err := d.openQcow2Node(monitor, overlayNode, overlayPath, false)
		if err != nil {
			return fmt.Errorf("Failed adding overlay back to disk %q: %w", disk.deviceName, err)
		}

		err = monitor.BlockDevSnapshot(disk.nodeName, overlayNode)
		if err != nil {
			removeOverlay()
			return fmt.Errorf("Failed adding overlay back to disk %q: %w", disk.deviceName, err)
		}

		// The guest writes to the overlay until a commit succeeds.
		err = d.commitOverlay(monitor, disk)
		if err != nil {
			d.logger.Error("Failed committing overlay", logger.Ctx{"device": disk.deviceName, "err": err})
		}
	}

	return nil
}

// reloadBitmaps creates on a disk every bitmap stored in the metadata image of its volume and merges the stored copy
// into it, in one transaction.
func (d *qemu) reloadBitmaps(monitor *qmp.Monitor, disk bitmapDisk, metadataImagePath string) error {
	metadataImageNode := metadataImageNodeName(disk.deviceName)
	closeMetadataImage, err := d.openQcow2Node(monitor, metadataImageNode, metadataImagePath, true)
	if err != nil {
		return err
	}

	defer closeMetadataImage()

	bitmaps, err := monitor.QueryNodeDirtyBitmaps(metadataImageNode)
	if err != nil {
		return fmt.Errorf("Failed querying bitmaps of metadata image: %w", err)
	}

	if len(bitmaps) == 0 {
		return nil
	}

	actions := make([]qmp.TransactionAction, 0, len(bitmaps)*2)
	for _, bitmap := range bitmaps {
		actions = append(actions, qmp.BlockDirtyBitmapAddAction(disk.nodeName, bitmap.Name, bitmap.Granularity, false, false), qmp.BlockDirtyBitmapMergeAction(disk.nodeName, bitmap.Name, metadataImageNode, bitmap.Name))
	}

	err = monitor.RunTransaction(actions)
	if err != nil {
		return fmt.Errorf("Failed recreating bitmaps: %w", err)
	}

	return nil
}
