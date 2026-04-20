package connectors

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/canonical/lxd/lxd/storage/block"
	"github.com/canonical/lxd/lxd/util"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/logger"
	"github.com/canonical/lxd/shared/revert"
)

// fcDiskDevicePrefix is the prefix of the FC disk device name in /dev/disk/by-id/.
const fcDiskDevicePrefix = "wwn-"

var _ Connector = &connectorISCSIFC{}

type connectorISCSIFC struct {
	common
}

// FCDiscoveryRecord represents an FC target port found on the fabric.
type FCDiscoveryRecord struct {
	PortName  string // Target WWPN (for example "0x2100001b32abcdef").
	NodeName  string // Target WWNN.
	PortState string // Important values are "Online" or "Blocked".
	Roles     string
	RPortName string // Remote port sysfs entry name (for example "rport-0:0-1")
}

// Type returns the type of the connector.
func (c *connectorISCSIFC) Type() string {
	return TypeISCSI
}

// Version returns "fc" if FC host adapters are present on the system, error otherwise.
func (c *connectorISCSIFC) Version() (string, error) {
	_, err := os.ReadDir("/sys/class/fc_host")
	if err != nil {
		return "", fmt.Errorf("No FC host adapters found: %w", err)
	}

	return "fc", nil
}

// LoadModules loads the FC SCSI transport kernel module.
func (c *connectorISCSIFC) LoadModules() error {
	return util.LoadModule("scsi_transport_fc")
}

// QualifiedName returns the World Wide Port Name (WWPN) of the first FC host initiator.
func (c *connectorISCSIFC) QualifiedName() (string, error) {
	fcHostPath := "/sys/class/fc_host"

	hosts, err := os.ReadDir(fcHostPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("No FC hosts found: directory %q does not exist", fcHostPath)
		}

		return "", fmt.Errorf("Failed reading FC hosts: %w", err)
	}

	for _, host := range hosts {
		portNameBytes, err := os.ReadFile(filepath.Join(fcHostPath, host.Name(), "port_name"))
		if err != nil {
			continue
		}

		return strings.TrimSpace(string(portNameBytes)), nil
	}

	return "", errors.New("No FC host initiators found")
}

// Connect triggers an FC rescan to discover LUNs mapped to the target.
// For FCP, fabric login is handled automatically by the HBA driver; this function
// ensures newly mapped LUNs are visible to the OS by issuing a SCSI bus rescan.
// Each entry in targetAddresses is the target WWPN.
func (c *connectorISCSIFC) Connect(ctx context.Context, targetQN string, targetAddresses ...string) (revert.Hook, error) {
	logger.Warn("Connecting to iSCSI/FC target", logger.Ctx{"target_qn": targetQN, "target_addresses": targetAddresses})
	defer logger.Warn("iSCSI/FC target connected", logger.Ctx{"target_qn": targetQN})

	connectFunc := func(ctx context.Context, s *session, targetAddr string) error {
		return c.scanFCHosts(ctx)
	}

	return connect(ctx, c, targetQN, targetAddresses, connectFunc)
}

// scanFCHosts triggers a rescan on all SCSI hosts backed by FC adapters so that
// newly mapped LUNs are discovered by the OS.
func (c *connectorISCSIFC) scanFCHosts(ctx context.Context) error {
	fcHostPath := "/sys/class/fc_host"

	hosts, err := os.ReadDir(fcHostPath)
	if err != nil {
		return fmt.Errorf("Failed reading FC hosts: %w", err)
	}

	for _, host := range hosts {
		// FC host names correspond 1:1 with SCSI host names (host0, host1, …).
		hostNum := strings.TrimPrefix(host.Name(), "host")
		scanPath := filepath.Join("/sys/class/scsi_host", "host"+hostNum, "scan")

		// "- - -" means scan all channels, targets, and LUNs.
		err := os.WriteFile(scanPath, []byte("- - -"), 0200)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			logger.Warn("Failed scanning FC host", logger.Ctx{"host": host.Name(), "err": err})
		}
	}

	return nil
}

// Disconnect is a no-op for iSCSI/FC; the HBA driver manages fabric connectivity automatically.
// Callers must remove disk devices via RemoveDiskDevice before unmapping volumes on the array.
func (c *connectorISCSIFC) Disconnect(targetQN string) error {
	return nil
}

// findSession returns the FC remote port session for the given target WWPN.
// It searches /sys/class/fc_remote_ports for a port matching targetQN.
// A non-nil session is returned whenever the remote port entry exists, regardless
// of its state. The session addresses slice is populated only when the port is Online.
func (c *connectorISCSIFC) findSession(targetQN string) (*session, error) {
	rportBasePath := "/sys/class/fc_remote_ports"

	rports, err := os.ReadDir(rportBasePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}

		return nil, fmt.Errorf("Failed reading FC remote ports: %w", err)
	}

	for _, rport := range rports {
		portNameBytes, err := os.ReadFile(filepath.Join(rportBasePath, rport.Name(), "port_name"))
		if err != nil {
			continue
		}

		if strings.TrimSpace(string(portNameBytes)) != targetQN {
			continue
		}

		s := &session{
			id:       rport.Name(),
			targetQN: targetQN,
		}

		// Populate the addresses list only when the port is actually Online.
		// This mirrors the NVMe/TCP behaviour of returning a session even when
		// no active connections exist, allowing recovery once the fabric restores.
		stateBytes, err := os.ReadFile(filepath.Join(rportBasePath, rport.Name(), "port_state"))
		if err == nil && strings.TrimSpace(string(stateBytes)) == "Online" {
			s.addresses = []string{targetQN}
		}

		return s, nil
	}

	return nil, nil
}

// Discover returns the FC target ports visible on the fabric.
// If targetAddresses are provided they act as a WWPN allowlist.
func (c *connectorISCSIFC) Discover(ctx context.Context, targetAddresses ...string) ([]any, error) {
	rportBasePath := "/sys/class/fc_remote_ports"

	rports, err := os.ReadDir(rportBasePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, errors.New("No FC remote ports found")
		}

		return nil, fmt.Errorf("Failed reading FC remote ports: %w", err)
	}

	result := make([]any, 0, len(rports))
	for _, rport := range rports {
		portNameBytes, err := os.ReadFile(filepath.Join(rportBasePath, rport.Name(), "port_name"))
		if err != nil {
			continue
		}

		portName := strings.TrimSpace(string(portNameBytes))

		if len(targetAddresses) > 0 {
			found := false
			for _, addr := range targetAddresses {
				if strings.EqualFold(portName, addr) {
					found = true
					break
				}
			}

			if !found {
				continue
			}
		}

		record := FCDiscoveryRecord{
			PortName:  portName,
			RPortName: rport.Name(),
		}

		nodeNameBytes, err := os.ReadFile(filepath.Join(rportBasePath, rport.Name(), "node_name"))
		if err == nil {
			record.NodeName = strings.TrimSpace(string(nodeNameBytes))
		}

		stateBytes, err := os.ReadFile(filepath.Join(rportBasePath, rport.Name(), "port_state"))
		if err == nil {
			record.PortState = strings.TrimSpace(string(stateBytes))
		}

		rolesBytes, err := os.ReadFile(filepath.Join(rportBasePath, rport.Name(), "roles"))
		if err == nil {
			record.Roles = strings.TrimSpace(string(rolesBytes))
		}

		result = append(result, record)
	}

	if len(result) == 0 {
		return nil, errors.New("No iSCSI/FC targets found on the fabric")
	}

	return result, nil
}

// WaitDiskDevicePath waits for the mapped iSCSI/FC device to appear.
// If the device is not a multipath device, multipath is forced and the device is waited for again.
func (c *connectorISCSIFC) WaitDiskDevicePath(ctx context.Context, diskPathFilter block.DevicePathFilterFunc) (string, error) {
	logger.Warn("Waiting for iSCSI/FC disk device path")
	_, ok := ctx.Deadline()
	if !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	devicePath, err := block.WaitDiskDevicePath(ctx, fcDiskDevicePrefix, diskPathFilter)
	if err != nil {
		return "", err
	}

	if isMultipathDevice(devicePath) {
		return devicePath, nil
	}

	// Device is not yet a multipath device — create one explicitly.
	_, err = shared.RunCommand(ctx, "multipath", devicePath)
	if err != nil {
		return "", fmt.Errorf("Failed configuring multipath for iSCSI/FC device %q: %w", devicePath, err)
	}

	// udev updates /dev/disk/by-id symlinks asynchronously after multipath creates
	// the device-mapper node, so wait for the multipath-backed symlink to appear.
	multipathDeviceFilter := func(devicePath string) bool {
		if !diskPathFilter(devicePath) {
			return false
		}

		path, err := filepath.EvalSymlinks(devicePath)
		if err != nil {
			return false
		}

		return isMultipathDevice(path)
	}

	return block.WaitDiskDevicePath(ctx, fcDiskDevicePrefix, multipathDeviceFilter)
}

// GetDiskDevicePath returns the path of the mapped iSCSI/FC device if it already exists.
func (c *connectorISCSIFC) GetDiskDevicePath(diskPathFilter block.DevicePathFilterFunc) (string, error) {
	return block.GetDiskDevicePath(fcDiskDevicePrefix, diskPathFilter)
}

// RemoveDiskDevice removes the iSCSI/FC disk device from the system.
//
// iSCSI/FC devices should be removed from the host before being unmapped on the storage array.
// On some arrays, removing a LUN mapping immediately makes the device inaccessible,
// trapping any task (including udevd) that tries to access it in D-state.
// Removing the device node first avoids this.
func (c *connectorISCSIFC) RemoveDiskDevice(ctx context.Context, devicePath string) error {
	logger.Warn("Removing iSCSI/FC disk device", logger.Ctx{"device_path": devicePath})
	defer logger.Warn("iSCSI/FC disk device removed", logger.Ctx{"device_path": devicePath})

	if devicePath == "" {
		return nil
	}

	removeDevice := func(devName string) error {
		path := "/sys/block/" + devName + "/device/delete"
		err := os.WriteFile(path, []byte("1"), 0400)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}

		return nil
	}

	deviceName := filepath.Base(devicePath)

	if isMultipathDevice(devicePath) {
		// Collect slave devices before removing the map because
		// /sys/block/dm-X/slaves/ disappears after the map is flushed.
		slavesPath := filepath.Join("/sys/block", deviceName, "slaves")
		slaves, _ := os.ReadDir(slavesPath)

		removeMultipathDevice := func() error {
			var err error
			for range 10 {
				if ctx.Err() != nil {
					if err == nil {
						err = ctx.Err()
					}

					break
				}

				_, err = shared.RunCommand(ctx, "multipath", "-f", devicePath)
				if err == nil {
					return nil
				}

				time.Sleep(500 * time.Millisecond)
			}

			return fmt.Errorf("Failed removing multipath device %q: %w", devicePath, err)
		}

		err := removeMultipathDevice()
		if err != nil {
			return err
		}

		for _, slave := range slaves {
			err := removeDevice(slave.Name())
			if err != nil {
				return fmt.Errorf("Failed removing multipath slave device %q: %w", slave.Name(), err)
			}
		}

		// multipathd may recreate the map before the paths are fully gone;
		// flush again if the device reappeared so WaitDiskDeviceGone works.
		if shared.PathExists(devicePath) {
			err := removeMultipathDevice()
			if err != nil {
				return err
			}
		}
	} else {
		err := removeDevice(deviceName)
		if err != nil {
			return fmt.Errorf("Failed removing iSCSI/FC device %q: %w", devicePath, err)
		}
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if !block.WaitDiskDeviceGone(ctx, devicePath) {
		return fmt.Errorf("Timeout exceeded waiting for iSCSI/FC device %q to disappear", devicePath)
	}

	return nil
}

// WaitDiskDeviceResize waits until the iSCSI/FC disk device reflects the new size.
// For multipath devices the device-mapper map is refreshed before waiting.
func (c *connectorISCSIFC) WaitDiskDeviceResize(ctx context.Context, diskPath string, newSizeBytes int64) error {
	logger.Warn("Waiting for iSCSI/FC disk device resize", logger.Ctx{"disk_path": diskPath, "new_size_bytes": newSizeBytes})
	_, ok := ctx.Deadline()
	if !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	if isMultipathDevice(diskPath) {
		_, err := shared.RunCommand(ctx, "multipath", "-r", diskPath)
		if err != nil {
			return fmt.Errorf("Failed updating multipath iSCSI/FC device %q size: %w", diskPath, err)
		}
	}

	return block.WaitDiskDeviceResize(ctx, diskPath, newSizeBytes)
}
