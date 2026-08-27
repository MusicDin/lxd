package drivers

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"

	"golang.org/x/sys/unix"

	"github.com/canonical/lxd/lxd/instance/drivers/qmp"
	"github.com/canonical/lxd/shared"
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
