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
	"github.com/canonical/lxd/shared/revert"
)

var _ Connector = &connectorSCSIFC{}

type connectorSCSIFC struct {
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
func (c *connectorSCSIFC) Type() string {
	return TypeSCSIFC
}

// Version returns "fc" if FC host adapters are present on the system, error otherwise.
func (c *connectorSCSIFC) Version() (string, error) {
	_, err := os.ReadDir("/sys/class/fc_host")
	if err != nil {
		return "", fmt.Errorf("No FC host adapters found: %w", err)
	}

	return "fc", nil
}

// LoadModules loads the FC SCSI transport kernel module.
func (c *connectorSCSIFC) LoadModules() error {
	return util.LoadModule("scsi_transport_fc")
}

// QualifiedName returns the World Wide Port Name (WWPN) of the first FC host initiator.
func (c *connectorSCSIFC) QualifiedName() (string, error) {
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

		// Linux sysfs reports WWPNs as "0x" + 16 hex chars (e.g., "0x210034800d7035b3").
		// PowerStore identifies initiators on the FC fabric using the colon-separated
		// byte format (e.g., "21:00:34:80:0d:70:35:b3"). Registering with the raw
		// sysfs string causes a format mismatch and the array never presents data LUNs
		// to the host. Convert to colon-separated for PowerStore API compatibility.
		wwpn := strings.TrimPrefix(strings.TrimSpace(string(portNameBytes)), "0x")
		if len(wwpn) == 16 {
			parts := make([]string, 8)
			for i := range 8 {
				parts[i] = wwpn[i*2 : i*2+2]
			}

			return strings.Join(parts, ":"), nil
		}

		return wwpn, nil
	}

	return "", errors.New("No FC host initiators found")
}

// Connect triggers a SCSI bus rescan on local hosts that have a remote FC port
// matching WWPN. The HBA driver handles fabric login automatically; the rescan
// makes newly mapped LUNs visible to the host.
func (c *connectorSCSIFC) Connect(ctx context.Context, WWPN string, luns ...string) (revert.Hook, error) {
	reverter := revert.New()
	defer reverter.Fail()

	rportBasePath := "/sys/class/fc_remote_ports"
	rports, err := os.ReadDir(rportBasePath)
	if err != nil {
		return nil, fmt.Errorf("Failed reading FC remote ports: %w", err)
	}

	wwpn := normalizeWWPN(WWPN)

	type scanTarget struct {
		host    string
		channel string
		target  string
	}

	var scanTargets []scanTarget
	for _, rport := range rports {
		portNameBytes, err := os.ReadFile(filepath.Join(rportBasePath, rport.Name(), "port_name"))
		if err != nil {
			continue
		}

		portName := normalizeWWPN(string(portNameBytes))
		if portName != wwpn {
			continue
		}

		// rport directory name has form "rport-H:C-R":
		// H = local SCSI host index, C = channel, R = rport index.
		name := strings.TrimPrefix(rport.Name(), "rport-")
		hostIdx, rest, ok := strings.Cut(name, ":")
		if !ok {
			continue
		}

		// We extract the channel, but safely ignore the rport index (the second variable).
		channel, _, ok := strings.Cut(rest, "-")
		if !ok {
			// Unexpected format, skip
			continue
		}

		// Read the actual SCSI target ID bound to this rport.
		targetBytes, err := os.ReadFile(filepath.Join(rportBasePath, rport.Name(), "scsi_target_id"))
		if err != nil {
			// Attribute missing, skip
			continue
		}

		target := strings.TrimSpace(string(targetBytes))

		// If target is "-1" or empty, the FC transport class hasn't bound it to a SCSI target yet.
		if target == "-1" || target == "" {
			continue
		}

		scanTargets = append(scanTargets, scanTarget{
			host:    "host" + hostIdx,
			channel: channel,
			target:  target,
		})
	}

	if len(scanTargets) == 0 {
		return nil, fmt.Errorf("No FC remote port with WWPN %q found", WWPN)
	}

	if len(luns) == 0 {
		luns = []string{"-"}
	}

	for _, scanTarget := range scanTargets {
		scanPath := filepath.Join("/sys/class/scsi_host", scanTarget.host, "scan")

		for _, lun := range luns {
			scan := scanTarget.channel + " " + scanTarget.target + " " + lun

			err := os.WriteFile(scanPath, []byte(scan), 0200)
			if err != nil {
				return nil, fmt.Errorf("Failed scanning FC host %q target %q LUN %q: %w", scanTarget.host, scanTarget.target, lun, err)
			}
		}
	}

	reverter.Success()
	return reverter.Fail, nil
}

// Disconnect is a no-op for FC. The HBA driver manages fabric connectivity automatically.
// Callers must remove disk devices via RemoveDiskDevice before unmapping volumes on the array.
func (c *connectorSCSIFC) Disconnect(targetQN string) error {
	return nil
}

// findSession returns the FC remote port session for the given target WWPN.
// It searches /sys/class/fc_remote_ports for a port matching targetQN.
// A non-nil session is returned whenever the remote port entry exists, regardless
// of its state. The session addresses slice is populated only when the port is Online.
func (c *connectorSCSIFC) findSession(targetQN string) (*session, error) {
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
func (c *connectorSCSIFC) Discover(ctx context.Context, targetAddresses ...string) ([]any, error) {
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
		return nil, errors.New("No SCSI/FC targets found on the fabric")
	}

	return result, nil
}

// WaitDiskDevicePath waits for the mapped FC device to appear.
// Unlike SCSI where the initiator continuously handles LUN discovery, FC requires
// explicit SCSI bus rescans to discover newly mapped LUNs. This function periodically
// re-triggers SCSI rescans while polling for the device to handle propagation delays
// between the storage array confirming a LUN mapping and the LUN being visible on the
// FC fabric.
// If the device is not a multipath device, multipath is forced and the device path is looked up again.
// An error is returned if no multipath device is found after that.
func (c *connectorSCSIFC) WaitDiskDevicePath(ctx context.Context, diskPathFilter block.DevicePathFilterFunc) (string, error) {
	_, ok := ctx.Deadline()
	if !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	devicePath, err := block.WaitDiskDevicePath(ctx, scsiDiskDevicePrefix, diskPathFilter)
	if err != nil {
		return "", err
	}

	if isMultipathDevice(devicePath) {
		return devicePath, nil
	}

	// Device is not a multipath device.
	// Create multipath device from a found device path.
	_, err = shared.RunCommand(ctx, "multipath", devicePath)
	if err != nil {
		return "", fmt.Errorf("Failed configuring multipath for SCSI/FC device %q: %w", devicePath, err)
	}

	// Filter that makes sure the found device resolves to a multipath device.
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

	// The multipath command is synchronous, but udev updates the /dev/disk/by-id
	// symlinks asynchronously. Wait for the multipath-backed device path to appear.
	return block.WaitDiskDevicePath(ctx, scsiDiskDevicePrefix, multipathDeviceFilter)
}

// GetDiskDevicePath returns the path of the mapped SCSI/FC device if it already exists.
func (c *connectorSCSIFC) GetDiskDevicePath(diskPathFilter block.DevicePathFilterFunc) (string, error) {
	return block.GetDiskDevicePath(scsiDiskDevicePrefix, diskPathFilter)
}

// RemoveDiskDevice removes the SCSI/FC disk device from the system.
//
// SCSI/FC devices should be removed from the host before being unmapped on the storage array.
// On some arrays, removing a LUN mapping immediately makes the device inaccessible,
// trapping any task that tries to access it in D-state (including udevd).
// Removing the device node first avoids this.
func (c *connectorSCSIFC) RemoveDiskDevice(ctx context.Context, devicePath string) error {
	// Wait until the device has disappeared.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if devicePath == "" {
		return nil
	}

	deviceName := filepath.Base(devicePath)

	var slaves []os.DirEntry
	if isMultipathDevice(devicePath) {
		// Collect the slave devices before removing the multipath map,
		// as /sys/block/dm-X/slaves/ will be gone after removal.
		slavesPath := filepath.Join("/sys/block", deviceName, "slaves")
		slaves, _ = os.ReadDir(slavesPath)
	}

	// removeDevice removes device from the system if the device is removable.
	removeDevice := func(devName string) error {
		path := "/sys/block/" + devName + "/device/delete"

		err := os.WriteFile(path, []byte("1"), 0200)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}

		return nil
	}

	for ctx.Err() == nil {
		if isMultipathDevice(devicePath) {
			// Remove multipath map.
			//
			// This may fail transiently with "map in use" if the device is still
			// briefly open (for example by udev), so retry a few times before giving up.
			var err error
			for range 10 {
				ctxErr := ctx.Err()
				if ctxErr != nil {
					// Preserve the command error if we already have one.
					// Otherwise return the generic context error.
					if err == nil {
						err = ctxErr
					}

					break
				}

				_, err = shared.RunCommand(ctx, "multipath", "-f", devicePath)
				if err == nil {
					break
				}

				time.Sleep(500 * time.Millisecond)
			}

			if err != nil {
				return fmt.Errorf("Failed removing multipath device %q: %w", devicePath, err)
			}

			// Remove underlying SCSI devices that were part of the multipath map.
			// If not removed, they remain on the system and cause I/O errors when the
			// volume is disconnected from a storage array.
			for _, slave := range slaves {
				err := removeDevice(slave.Name())
				if err != nil {
					return fmt.Errorf("Failed removing multipath slave device %q: %w", slave.Name(), err)
				}
			}
		} else {
			// For non-multipath device (/dev/sd*), remove the device itself.
			err := removeDevice(deviceName)
			if err != nil {
				return fmt.Errorf("Failed removing device %q: %w", devicePath, err)
			}
		}

		if !shared.PathExists(devicePath) {
			break
		}

		time.Sleep(500 * time.Millisecond)
	}

	if ctx.Err() != nil {
		return fmt.Errorf("Timeout exceeded waiting for SCSI/FC device %q to disappear", devicePath)
	}

	return nil
}

// WaitDiskDeviceResize waits until the SCSI/FC disk device reflects the new size.
// For multipath devices the device-mapper map is refreshed before waiting.
func (c *connectorSCSIFC) WaitDiskDeviceResize(ctx context.Context, diskPath string, newSizeBytes int64) error {
	_, ok := ctx.Deadline()
	if !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	if isMultipathDevice(diskPath) {
		_, err := shared.RunCommand(ctx, "multipath", "-r", diskPath)
		if err != nil {
			return fmt.Errorf("Failed updating multipath SCSI/FC device %q size: %w", diskPath, err)
		}
	}

	return block.WaitDiskDeviceResize(ctx, diskPath, newSizeBytes)
}

// normalizeWWPN normalizes the WWPN string by trimming whitespace, converting to lowercase,
// removing "0x" prefix if present, and removing colons.
func normalizeWWPN(wwpn string) string {
	wwpn = strings.TrimSpace(wwpn)
	wwpn = strings.ToLower(wwpn)
	wwpn = strings.TrimPrefix(wwpn, "0x")
	wwpn = strings.ReplaceAll(wwpn, ":", "")
	return wwpn
}
