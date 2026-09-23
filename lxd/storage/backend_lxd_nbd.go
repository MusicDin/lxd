package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"

	"github.com/canonical/lxd/lxd/db"
	"github.com/canonical/lxd/lxd/db/cluster"
	"github.com/canonical/lxd/lxd/device/config"
	"github.com/canonical/lxd/lxd/instance"
	"github.com/canonical/lxd/lxd/instance/instancetype"
	"github.com/canonical/lxd/lxd/project"
	"github.com/canonical/lxd/lxd/refcount"
	"github.com/canonical/lxd/lxd/storage/drivers"
	"github.com/canonical/lxd/lxd/storage/filesystem"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/logger"
	"github.com/canonical/lxd/shared/revert"
)

// nbdSocketPath returns the socket path of the NBD server session called name. The name is encoded so that a
// snapshot name cannot produce a nested path, and hashed when the path would not fit the unix socket path limit.
func nbdSocketPath(name string) string {
	dir := shared.VarPath("nbd")
	name = filesystem.PathNameEncode(name)

	// sun_path is 108 bytes long including the terminating NUL, so the path is kept below 100 bytes as a margin.
	maxNameLength := 100 - len(dir) - len("/.sock")
	if maxNameLength > 0 && len(name) > maxNameLength {
		hash := sha256.Sum256([]byte(name))
		name = base64.RawURLEncoding.EncodeToString(hash[:])
		if len(name) > maxNameLength {
			name = name[:maxNameLength]
		}
	}

	return filepath.Join(dir, name+".sock")
}

// nbdVolumeLockMember returns the member name that scopes a custom volume's NBD lock. It is this member for a
// local pool, where the same volume name is a different volume per member, and empty for a remote pool, whose
// volume is one shared entity across the cluster.
func (b *lxdBackend) nbdVolumeLockMember() string {
	if b.driver.Info().Remote {
		return ""
	}

	return b.state.ServerName
}

// nbdExportRefName returns the name of the reference counter of the snapshot NBD exports served by this member for
// an instance. volName is a snapshot, or the instance itself for the exports of all its snapshots.
func nbdExportRefName(poolName string, projectName string, volName string) string {
	return drivers.OperationLockName("NBDExport", poolName, drivers.VolumeTypeVM, drivers.ContentTypeBlock, project.StorageVolume(projectName, volName))
}

// nbdExportCount counts an NBD export of the named instance snapshot until the returned function runs. Every client
// opens its own export of a snapshot, so the exports are counted where an import session takes a lock.
func nbdExportCount(poolName string, projectName string, snapshotName string) func() {
	parentName, _, _ := api.GetParentAndSnapshotName(snapshotName)
	refNames := []string{
		nbdExportRefName(poolName, projectName, snapshotName),
		nbdExportRefName(poolName, projectName, parentName),
	}

	for _, refName := range refNames {
		refcount.Increment(refName, 1)
	}

	return func() {
		for _, refName := range refNames {
			refcount.Decrement(refName, 1)
		}
	}
}

// NBDExportInUse returns an in use error while this member serves an NBD export of the named instance snapshot.
// Given an instance, it returns the error while an export of any of its snapshots is served. An export has the
// volume snapshots and the config volume snapshot of the instance snapshot mounted.
func NBDExportInUse(poolName string, projectName string, volName string) error {
	if refcount.Get(nbdExportRefName(poolName, projectName, volName)) == 0 {
		return nil
	}

	if shared.IsSnapshot(volName) {
		return api.StatusErrorf(http.StatusLocked, "Snapshot %q is exported over NBD: %w", volName, drivers.ErrInUse)
	}

	return api.StatusErrorf(http.StatusLocked, "A snapshot of %q is exported over NBD: %w", volName, drivers.ErrInUse)
}

// GetVolumeNBD returns an NBD connection to a block volume, served by qemu-nbd against the volume and read-write
// when writable is set. The virtual machine whose root volume it is, or that the volume is attached to, must be
// stopped and on this member, as qemu-nbd opens the volume itself. A read-write export writes the volume, so the
// bitmaps of the volume are deleted before it. The returned conflict reference is the lock name of the session.
func (b *lxdBackend) GetVolumeNBD(projectName string, volType drivers.VolumeType, volName string, writable bool) (net.Conn, func(), string, error) {
	l := b.logger.AddContext(logger.Ctx{"project": projectName, "volType": volType, "volume": volName, "writable": writable})
	l.Debug("GetVolumeNBD started")
	defer l.Debug("GetVolumeNBD finished")

	err := b.isStatusReady()
	if err != nil {
		return nil, nil, "", err
	}

	if shared.IsSnapshot(volName) {
		return nil, nil, "", api.StatusErrorf(http.StatusBadRequest, "Snapshots are exported over NBD with their instance snapshot")
	}

	switch volType {
	case drivers.VolumeTypeVM:
		inst, rootDiskName, err := InstanceByVolumeName(b.state, b.name, projectName, volName, cluster.StoragePoolVolumeTypeVM)
		if err != nil {
			return nil, nil, "", err
		}

		dbVol, err := VolumeDBGet(b, projectName, volName, drivers.VolumeTypeVM)
		if err != nil {
			return nil, nil, "", err
		}

		if shared.IsTrue(dbVol.Config["security.shared"]) {
			return nil, nil, "", api.StatusErrorf(http.StatusBadRequest, "NBD export is not supported for shared volumes")
		}

		if inst.IsRunning() {
			return nil, nil, "", api.StatusErrorf(http.StatusBadRequest, "NBD export requires the instance to be stopped")
		}

		// Generate the effective root device volume for instance.
		vol := b.GetVolume(drivers.VolumeTypeVM, drivers.ContentTypeBlock, project.Instance(projectName, volName), dbVol.Config)
		err = b.applyInstanceRootDiskOverrides(inst, &vol)
		if err != nil {
			return nil, nil, "", err
		}

		lockName := nbdInstanceLockName(projectName, volName)
		return nbdLockedSession(b.state, lockName, fmt.Sprintf("instance %q", volName), func() (net.Conn, func(), error) {
			if writable {
				err := inst.DeleteVolumeBitmaps(rootDiskName)
				if err != nil {
					return nil, nil, fmt.Errorf("Failed deleting bitmaps: %w", err)
				}
			}

			return b.connectOfflineNBD(vol, writable)
		})
	case drivers.VolumeTypeCustom:
		dbVol, err := VolumeDBGet(b, projectName, volName, drivers.VolumeTypeCustom)
		if err != nil {
			return nil, nil, "", err
		}

		if dbVol.ContentType != cluster.StoragePoolVolumeContentTypeNameBlock {
			return nil, nil, "", api.StatusErrorf(http.StatusBadRequest, "NBD export is only supported for block volumes")
		}

		if shared.IsTrue(dbVol.Config["security.shared"]) {
			return nil, nil, "", api.StatusErrorf(http.StatusBadRequest, "NBD export is not supported for shared volumes")
		}

		// A non-shared block volume is attached to at most one instance, which must be stopped and on this
		// member before qemu-nbd can open the volume.
		instanceDevices := make(map[instance.Instance][]string)
		err = VolumeUsedByInstanceDevices(b.state, b.name, projectName, &dbVol.StorageVolume, true, func(dbInst db.InstanceArgs, project api.Project, usedByDevices []string) error {
			if dbInst.Node != b.state.ServerName {
				return api.StatusErrorf(http.StatusBadRequest, "Volume is attached to instance %q on cluster member %q", dbInst.Name, dbInst.Node)
			}

			inst, err := instance.Load(b.state, dbInst, project)
			if err != nil {
				return err
			}

			if inst.IsRunning() {
				return api.StatusErrorf(http.StatusBadRequest, "NBD export requires instance %q to be stopped", dbInst.Name)
			}

			instanceDevices[inst] = usedByDevices
			return nil
		})
		if err != nil {
			return nil, nil, "", err
		}

		vol := b.GetVolume(drivers.VolumeTypeCustom, drivers.ContentTypeBlock, project.StorageVolume(projectName, volName), dbVol.Config)

		lockName := nbdVolumeLockName(b.nbdVolumeLockMember(), b.name, projectName, volName)
		return nbdLockedSession(b.state, lockName, fmt.Sprintf("volume %q", b.name+"/"+volName), func() (net.Conn, func(), error) {
			if writable {
				for inst, deviceNames := range instanceDevices {
					for _, deviceName := range deviceNames {
						err := inst.DeleteVolumeBitmaps(deviceName)
						if err != nil {
							return nil, nil, fmt.Errorf("Failed deleting bitmaps: %w", err)
						}
					}
				}
			}

			return b.connectOfflineNBD(vol, writable)
		})
	default:
		return nil, nil, "", api.StatusErrorf(http.StatusBadRequest, "NBD export is not supported for volumes of type %q", volType)
	}
}

// connectOfflineNBD serves the volume through qemu-nbd and returns a connection to it. The volume stays
// activated until qemu-nbd exits, which happens once the connection is closed or the returned disconnect
// function is called. The disconnect function returns once the volume has been released.
func (b *lxdBackend) connectOfflineNBD(vol drivers.Volume, writable bool) (net.Conn, func(), error) {
	socketPath := nbdSocketPath(vol.Pool() + "_" + string(vol.Type()) + "_" + vol.Name())

	err := os.MkdirAll(filepath.Dir(socketPath), 0700)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed creating NBD socket directory: %w", err)
	}

	// A socket left behind by a session that did not shut down cleanly would pass the readiness check before
	// qemu-nbd listens on it.
	_ = os.Remove(socketPath)

	// LXD block volumes are raw, and format probing must never run on guest controlled data.
	args := []string{"--socket=" + socketPath, "--format=raw"}
	if !writable {
		args = append(args, "--read-only")
	}

	// Share the qemu-nbd process safely between the activation goroutine and this function.
	var procMu sync.Mutex
	var proc *os.Process
	aborted := false

	// Buffered so the goroutine never blocks if this function has already given up.
	errCh := make(chan error, 1)

	// started is closed once qemu-nbd runs and done once the goroutine has released the volume.
	started := make(chan struct{})
	done := make(chan struct{})

	// The goroutine returns only when qemu-nbd exits so that the volume stays active for the whole session.
	go func() {
		defer close(done)

		err := b.driver.ActivateTask(vol, func(devPath string) error {
			var stderr bytes.Buffer
			cmd := exec.Command("qemu-nbd", append(args, devPath)...)
			cmd.Stderr = &stderr

			procMu.Lock()
			if aborted {
				procMu.Unlock()
				return errors.New("Request timed out before qemu-nbd could be started")
			}

			err := cmd.Start()
			if err != nil {
				procMu.Unlock()
				return fmt.Errorf("Failed starting qemu-nbd: %w", err)
			}

			proc = cmd.Process
			procMu.Unlock()
			close(started)

			err = cmd.Wait()
			if err != nil {
				return fmt.Errorf("Failed running qemu-nbd: %w (%s)", err, strings.TrimSpace(stderr.String()))
			}

			return nil
		})
		if err != nil {
			procMu.Lock()
			stopped := aborted
			procMu.Unlock()

			// qemu-nbd reports an error when LXD itself stopped it.
			if stopped {
				b.logger.Debug("Stopped serving volume over NBD", logger.Ctx{"volume": vol.Name(), "err": err})
			} else {
				b.logger.Error("Failed serving volume over NBD", logger.Ctx{"volume": vol.Name(), "err": err})
			}

			errCh <- err
		}
	}()

	// stopNBD signals qemu-nbd if it was started and waits for the goroutine to release the volume. The
	// process is killed if it has not exited after 30 seconds.
	stopNBD := func(sig os.Signal) {
		procMu.Lock()
		aborted = true
		if proc != nil {
			_ = proc.Signal(sig)
		}

		procMu.Unlock()

		select {
		case <-done:
		case <-time.After(30 * time.Second):
			procMu.Lock()
			if proc != nil {
				_ = proc.Kill()
			}

			procMu.Unlock()

			select {
			case <-done:
			case <-time.After(30 * time.Second):
				b.logger.Error("Timed out waiting for qemu-nbd to exit", logger.Ctx{"volume": vol.Name()})
			}
		}

		_ = os.Remove(socketPath)
	}

	// Activating the volume can take a while on drivers that wait for device nodes, so the socket readiness
	// timeout only starts once qemu-nbd runs.
	select {
	case <-started:
	case err := <-errCh:
		stopNBD(unix.SIGKILL)
		return nil, nil, err
	case <-time.After(2 * time.Minute):
		stopNBD(unix.SIGKILL)
		return nil, nil, errors.New("Timed out waiting for volume activation")
	}

	// Wait for qemu-nbd to listen on the socket.
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for ready := false; !ready; {
		select {
		case <-timeout:
			stopNBD(unix.SIGKILL)
			return nil, nil, errors.New("Timed out waiting for qemu-nbd socket")
		case err := <-errCh:
			stopNBD(unix.SIGKILL)
			return nil, nil, err
		case <-ticker.C:
			_, err := os.Stat(socketPath)
			ready = err == nil
		}
	}

	b.logger.Debug("Connecting to qemu-nbd socket", logger.Ctx{"volume": vol.Name(), "socketPath": socketPath})
	nbdConn, err := net.Dial("unix", socketPath)
	if err != nil {
		// qemu-nbd may have exited before the connection, in which case its error contains the reason.
		select {
		case err = <-errCh:
		default:
			err = fmt.Errorf("Failed connecting to qemu-nbd socket: %w", err)
		}

		stopNBD(unix.SIGKILL)
		return nil, nil, err
	}

	disconnect := func() {
		b.logger.Debug("Stopping qemu-nbd", logger.Ctx{"volume": vol.Name()})
		_ = nbdConn.Close()
		stopNBD(unix.SIGTERM)
	}

	return nbdConn, disconnect, nil
}

// nbdExport describes one export served by a snapshot NBD server.
type nbdExport struct {
	name              string // Export name.
	devPath           string // Block device holding the snapshot data.
	metadataImagePath string // Metadata image with the bitmaps to publish.
}

// serveSnapshotNBD serves the given exports read-only through qemu-storage-daemon listening on socketPath and returns
// a connection to it. Each export publishes the bitmaps of its metadata image, which the daemon opens as a separate
// read-only node. The returned disconnect function ends the daemon and returns once it has exited.
func (b *lxdBackend) serveSnapshotNBD(socketPath string, exports []nbdExport) (net.Conn, func(), error) {
	err := os.MkdirAll(filepath.Dir(socketPath), 0700)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed creating NBD socket directory: %w", err)
	}

	// A socket left behind by a session that did not shut down cleanly would pass the readiness check before
	// the daemon listens on it.
	_ = os.Remove(socketPath)

	// The daemon processes its options in order, so the server is started before the exports are added.
	args := []string{"--nbd-server", "addr.type=unix,addr.path=" + socketPath}
	for i, export := range exports {
		dataNode := fmt.Sprintf("data%d", i)
		metadataImageNode := fmt.Sprintf("metadata%d", i)
		options := []map[string]any{
			{"driver": "file", "node-name": dataNode + "-file", "filename": export.devPath, "read-only": true},
			// LXD block volumes are raw, and format probing must never run on guest controlled data.
			{"driver": "raw", "node-name": dataNode, "file": dataNode + "-file", "read-only": true},
			{"driver": "file", "node-name": metadataImageNode + "-file", "filename": export.metadataImagePath, "read-only": true},
			{"driver": "qcow2", "node-name": metadataImageNode, "file": metadataImageNode + "-file", "read-only": true},
		}

		bitmaps, err := Qcow2Bitmaps(export.metadataImagePath)
		if err != nil {
			return nil, nil, err
		}

		published := make([]map[string]any, 0, len(bitmaps))
		for _, bitmap := range bitmaps {
			published = append(published, map[string]any{"node": metadataImageNode, "name": bitmap.Name})
		}

		exportOptions := map[string]any{"type": "nbd", "id": fmt.Sprintf("export%d", i), "node-name": dataNode, "name": export.name}
		if len(published) > 0 {
			exportOptions["bitmaps"] = published
		}

		// The options are given as JSON, which needs no escaping of the paths and names in them.
		for _, option := range options {
			blockdev, err := json.Marshal(option)
			if err != nil {
				return nil, nil, err
			}

			args = append(args, "--blockdev", string(blockdev))
		}

		exportArg, err := json.Marshal(exportOptions)
		if err != nil {
			return nil, nil, err
		}

		args = append(args, "--export", string(exportArg))
	}

	var stderr bytes.Buffer
	cmd := exec.Command("qemu-storage-daemon", args...)
	cmd.Stderr = &stderr

	err = cmd.Start()
	if err != nil {
		return nil, nil, fmt.Errorf("Failed starting qemu-storage-daemon: %w", err)
	}

	// exited is closed once the daemon has exited.
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()

	// stop signals the daemon and waits for it to exit. The process is killed if it has not exited after 30 seconds.
	stop := func(sig os.Signal) {
		_ = cmd.Process.Signal(sig)

		select {
		case <-exited:
		case <-time.After(30 * time.Second):
			_ = cmd.Process.Kill()
			<-exited
		}

		_ = os.Remove(socketPath)
	}

	// Wait for the daemon to listen on the socket.
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for ready := false; !ready; {
		select {
		case <-timeout:
			stop(unix.SIGKILL)
			return nil, nil, errors.New("Timed out waiting for qemu-storage-daemon socket")
		case <-exited:
			_ = os.Remove(socketPath)
			return nil, nil, fmt.Errorf("Failed running qemu-storage-daemon: %s", strings.TrimSpace(stderr.String()))
		case <-ticker.C:
			_, err := os.Stat(socketPath)
			ready = err == nil
		}
	}

	nbdConn, err := net.Dial("unix", socketPath)
	if err != nil {
		stop(unix.SIGKILL)
		return nil, nil, fmt.Errorf("Failed connecting to qemu-storage-daemon socket: %w", err)
	}

	disconnect := func() {
		b.logger.Debug("Stopping qemu-storage-daemon", logger.Ctx{"socketPath": socketPath})
		_ = nbdConn.Close()
		stop(unix.SIGTERM)
	}

	return nbdConn, disconnect, nil
}

// GetInstanceSnapshotNBD returns a read-only NBD connection serving the volume snapshots of a virtual machine
// snapshot that have a metadata image, each under an export named after its disk device together with the bitmaps
// of the snapshot. deviceNames selects a subset of the devices, and every device is served when it is empty. A
// volume snapshot that no longer exists is skipped. The returned conflict reference is empty, as every client opens
// its own session.
func (b *lxdBackend) GetInstanceSnapshotNBD(snapInst instance.Instance, deviceNames []string) (net.Conn, func(), string, error) {
	l := b.logger.AddContext(logger.Ctx{"project": snapInst.Project().Name, "instance": snapInst.Name()})
	l.Debug("GetInstanceSnapshotNBD started")
	defer l.Debug("GetInstanceSnapshotNBD finished")

	err := b.isStatusReady()
	if err != nil {
		return nil, nil, "", err
	}

	if !snapInst.IsSnapshot() {
		return nil, nil, "", errors.New("Instance must be a snapshot")
	}

	if snapInst.Type() != instancetype.VM {
		return nil, nil, "", api.StatusErrorf(http.StatusBadRequest, "NBD export is only supported for virtual machines")
	}

	images, err := snapInst.SnapshotMetadataImages()
	if err != nil {
		return nil, nil, "", err
	}

	if len(images) == 0 {
		return nil, nil, "", api.StatusErrorf(http.StatusBadRequest, "Snapshot was not created with a bitmap")
	}

	for _, deviceName := range deviceNames {
		_, ok := images[deviceName]
		if !ok {
			return nil, nil, "", api.StatusErrorf(http.StatusBadRequest, "Snapshot has no volume snapshot with bitmaps for device %q", deviceName)
		}
	}

	revert := revert.New()
	defer revert.Fail()

	cleanups := []func(){}

	// A snapshot cannot be deleted or renamed while it is exported.
	uncount := nbdExportCount(b.name, snapInst.Project().Name, snapInst.Name())
	revert.Add(uncount)
	cleanups = append(cleanups, uncount)

	// The metadata images are on the config volume snapshot, which is mounted with the root volume snapshot.
	mountInfo, err := b.MountInstanceSnapshot(snapInst, nil)
	if err != nil {
		return nil, nil, "", err
	}

	unmountRoot := func() { _ = b.UnmountInstanceSnapshot(snapInst, nil) }
	revert.Add(unmountRoot)
	cleanups = append(cleanups, unmountRoot)

	devSource, isPath := mountInfo.DevSource.(config.DevSourcePath)
	if !isPath {
		return nil, nil, "", errors.New("Failed getting disk path of snapshot")
	}

	rootDBVol, err := VolumeDBGet(b, snapInst.Project().Name, snapInst.Name(), drivers.VolumeTypeVM)
	if err != nil {
		return nil, nil, "", err
	}

	instProject := snapInst.Project()
	effectiveProject := project.StorageVolumeProjectFromRecord(&instProject, cluster.StoragePoolVolumeTypeCustom)
	customType := cluster.StoragePoolVolumeTypeCustom
	pools := map[string]Pool{b.name: b}
	exports := []nbdExport{}
	for _, deviceName := range slices.Sorted(maps.Keys(images)) {
		image := images[deviceName]
		if len(deviceNames) > 0 && !slices.Contains(deviceNames, deviceName) {
			continue
		}

		if image.SnapshotUUID == rootDBVol.Config["volatile.uuid"] {
			exports = append(exports, nbdExport{name: deviceName, devPath: devSource.Path, metadataImagePath: image.Path})
			continue
		}

		// The volume snapshot of an attached custom volume, which can be deleted on its own.
		var dbSnapVols []*db.StorageVolume
		err = b.state.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
			var err error
			dbSnapVols, err = tx.GetStorageVolumes(ctx, true, db.StorageVolumeFilter{Type: &customType, Project: &effectiveProject, UUIDs: []string{image.SnapshotUUID}})
			return err
		})
		if err != nil {
			return nil, nil, "", err
		}

		if len(dbSnapVols) == 0 {
			l.Warn("Volume snapshot of the snapshot no longer exists, not exporting it", logger.Ctx{"device": deviceName, "snapshotUUID": image.SnapshotUUID})
			continue
		}

		dbSnapVol := dbSnapVols[0]
		pool, ok := pools[dbSnapVol.Pool]
		if !ok {
			pool, err = LoadByName(b.state, dbSnapVol.Pool)
			if err != nil {
				return nil, nil, "", err
			}

			pools[dbSnapVol.Pool] = pool
		}

		lxdPool, ok := pool.(*lxdBackend)
		if !ok {
			return nil, nil, "", fmt.Errorf("Unexpected storage pool type for %q", dbSnapVol.Pool)
		}

		snapVol := lxdPool.GetVolume(drivers.VolumeTypeCustom, drivers.ContentTypeBlock, project.StorageVolume(effectiveProject, dbSnapVol.Name), dbSnapVol.Config)
		if lxdPool.driver.Info().PopulateParentVolumeUUID {
			parentUUID, err := lxdPool.getParentVolumeUUID(snapVol, effectiveProject)
			if err != nil {
				return nil, nil, "", err
			}

			snapVol.SetParentUUID(parentUUID)
		}

		err = lxdPool.driver.MountVolumeSnapshot(snapVol, nil)
		if err != nil {
			return nil, nil, "", err
		}

		unmount := func() { _, _ = lxdPool.driver.UnmountVolumeSnapshot(snapVol, nil) }
		revert.Add(unmount)
		cleanups = append(cleanups, unmount)

		devPath, err := lxdPool.driver.GetVolumeDiskPath(snapVol)
		if err != nil {
			return nil, nil, "", fmt.Errorf("Failed getting disk path of volume snapshot %q: %w", dbSnapVol.Name, err)
		}

		exports = append(exports, nbdExport{name: deviceName, devPath: devPath, metadataImagePath: image.Path})
	}

	if len(exports) == 0 {
		return nil, nil, "", api.StatusErrorf(http.StatusBadRequest, "Snapshot has no volume snapshot to export")
	}

	socketPath := nbdSocketPath(project.Instance(snapInst.Project().Name, snapInst.Name()) + "_" + uuid.New().String())
	conn, disconnect, err := b.serveSnapshotNBD(socketPath, exports)
	if err != nil {
		return nil, nil, "", err
	}

	revert.Success()
	return conn, func() {
		disconnect()

		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}, "", nil
}
