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

var _ Connector = &connectorSCSIFC{}

type connectorSCSIFC struct {
	common
}

// FCDiscoveryRecord represents an FC target port found on the fabric.
type FCDiscoveryRecord struct {
	PortName string // Target WWPN (for example "2100001b32abcdef").
}

// Type returns the type of the connector.
func (c *connectorSCSIFC) Type() string {
	return TypeSCSIFC
}

// Transport returns the transport type of the connector.
func (c *connectorSCSIFC) Transport() TransportType {
	return TransportFC
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

		wwpn := normalizeWWPN(string(portNameBytes))
		return wwpn, nil
	}

	return "", errors.New("No FC host initiators found")
}

// Connect triggers a SCSI bus rescan on local hosts that have a remote FC port
// matching WWPN. The HBA driver handles fabric login automatically; the rescan
// makes newly mapped LUNs visible to the host.
func (c *connectorSCSIFC) Connect(ctx context.Context, WWPN string, luns ...string) (revert.Hook, error) {
	rportBasePath := "/sys/class/fc_remote_ports"
	rports, err := os.ReadDir(rportBasePath)
	if err != nil {
		return nil, fmt.Errorf("Failed reading FC remote ports: %w", err)
	}

	if len(luns) == 0 {
		return nil, errors.New("At least one LUN must be provided to connect to an FC target")
	}

	wwpn := normalizeWWPN(WWPN)

	type scanTarget struct {
		host    string
		channel string
		target  string
	}

	var scanTargets []scanTarget
	for _, rport := range rports {
		logger.Warn("Checking rport", logger.Ctx{
			"name": rport.Name(),
		})

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

		channel, _, ok := strings.Cut(rest, "-")
		if !ok {
			// Unexpected format, skip
			continue
		}

		targetBytes, err := os.ReadFile(filepath.Join(rportBasePath, rport.Name(), "scsi_target_id"))
		if err != nil {
			// Attribute missing, skip
			continue
		}

		target := strings.TrimSpace(string(targetBytes))

		scanTarget := scanTarget{
			host:    "host" + hostIdx,
			channel: channel,
			target:  target,
		}

		scanTargets = append(scanTargets, scanTarget)
	}

	if len(scanTargets) == 0 {
		return nil, fmt.Errorf("No FC remote port with WWPN %q found", WWPN)
	}

	// Trigger SCSI bus rescan on each host, by writing the scan parameters to the host's
	// scan file. This will make the newly mapped LUNs visible to the host.
	for _, scanTarget := range scanTargets {
		scanPath := filepath.Join("/sys/class/scsi_host", scanTarget.host, "scan")

		for _, lun := range luns {
			logger.Warn("Triggering SCSI bus rescan for FC target", logger.Ctx{
				"WWPN":    WWPN,
				"host":    scanTarget.host,
				"channel": scanTarget.channel,
				"target":  scanTarget.target,
				"lun":     lun,
			})

			scan := scanTarget.channel + " " + scanTarget.target + " " + lun

			err := os.WriteFile(scanPath, []byte(scan), 0200)
			if err != nil {
				return nil, fmt.Errorf("Failed scanning FC host %q target %q LUN %q: %w", scanTarget.host, scanTarget.target, lun, err)
			}
		}
	}

	cleanup := func() {}
	return cleanup, nil
}

// Disconnect is a no-op for FC.
func (c *connectorSCSIFC) Disconnect(targetQN string) error {
	return nil
}

// findSession returns nil as FC doesn't have sessions.
func (c *connectorSCSIFC) findSession(targetQN string) (*session, error) {
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

		stateBytes, err := os.ReadFile(filepath.Join(rportBasePath, rport.Name(), "port_state"))
		if err != nil {
			continue
		}

		state := strings.TrimSpace(string(stateBytes))
		if state != "Online" {
			// Skip offline or blocked ports, as they are not usable.
			continue
		}

		record := FCDiscoveryRecord{
			PortName: normalizeWWPN(portName),
		}

		result = append(result, record)
	}

	if len(result) == 0 {
		return nil, errors.New("No SCSI/FC targets found on the fabric")
	}

	return result, nil
}

// WaitDiskDevicePath waits for the mapped FC device to appear.
// If the discovered device is not a multipath device, multipath is forced and the device path
// is looked up again. An error is returned if no multipath device is found after that.
func (c *connectorSCSIFC) WaitDiskDevicePath(ctx context.Context, diskPathFilter block.DevicePathFilterFunc) (string, error) {
	_, ok := ctx.Deadline()
	if !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	devicePath, err := block.WaitDiskDevicePath(ctx, scsiDiskDevicePrefix, diskPathFilter)
	if err != nil {
		return "", fmt.Errorf("Failed waiting for device path to appear: %w", err)
	}

	if isMultipathDevice(devicePath) {
		err = waitMultipathReady(ctx, devicePath)
		if err != nil {
			return "", fmt.Errorf("Failed waiting for multipath device %q to be ready: %w", devicePath, err)
		}

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
	mpDevicePath, err := block.WaitDiskDevicePath(ctx, scsiDiskDevicePrefix, multipathDeviceFilter)
	if err != nil {
		return "", fmt.Errorf("Failed waiting for forced multipath device path to appear: %w", err)
	}

	err = waitMultipathReady(ctx, mpDevicePath)
	if err != nil {
		return "", fmt.Errorf("Failed waiting for forced multipath device %q to be ready: %w", mpDevicePath, err)
	}

	return mpDevicePath, nil
}

// GetDiskDevicePath returns the path of the mapped SCSI/FC device if it already exists.
func (c *connectorSCSIFC) GetDiskDevicePath(diskPathFilter block.DevicePathFilterFunc) (string, error) {
	return block.GetDiskDevicePath(scsiDiskDevicePrefix, diskPathFilter)
}

// collectSCSIPathsForMpath returns every /sys/block/sd* basename that
// belongs to the multipath device-mapper device named by dmName. The
// returned set is the union of:
//
//  1. The device-mapper map's current slaves (/sys/block/<dm>/slaves).
//  2. Every /sys/block/sd* whose device/wwid matches the multipath
//     device's wwid (extracted from /sys/block/<dm>/dm/uuid). This
//     includes sd devices multipathd previously failed and dropped
//     from the map, which would otherwise persist as zombies and
//     trigger "Logical unit not supported" probe storms once the
//     array detaches the underlying LUN.
//
// If the multipath wwid cannot be parsed, the slaves alone are returned.
func collectSCSIPathsForMpath(dmName string) ([]string, error) {
	seen := make(map[string]struct{})
	var ordered []string

	add := func(name string) {
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		ordered = append(ordered, name)
	}

	slavesPath := filepath.Join("/sys/block", dmName, "slaves")
	slaves, err := os.ReadDir(slavesPath)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("Failed reading slaves of %q: %w", dmName, err)
	}

	for _, s := range slaves {
		add(s.Name())
	}

	// Extract the bare hex wwid from the mpath uuid. Format observed on
	// PowerStore (NAA-6): "mpath-3<32 hex chars>". The "3" is the SCSI
	// VPD-83 type byte. On the sd side, device/wwid reports as either
	// "naa.6<hex>" or "0x6<hex>" or just the bare hex. Normalize both
	// sides to lowercase bare hex before comparison.
	uuidBytes, err := os.ReadFile(filepath.Join("/sys/block", dmName, "dm", "uuid"))
	if err != nil {
		// No usable wwid — slave list is all we have.
		return ordered, nil
	}

	uuid := strings.ToLower(strings.TrimSpace(string(uuidBytes)))
	rest, ok := strings.CutPrefix(uuid, "mpath-")
	if !ok {
		return ordered, nil
	}
	// Drop the SCSI VPD-83 type-byte prefix if present.
	rest = strings.TrimPrefix(rest, "3")

	if rest == "" {
		return ordered, nil
	}

	entries, err := os.ReadDir("/sys/block")
	if err != nil {
		return nil, fmt.Errorf("Failed reading /sys/block: %w", err)
	}

	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "sd") {
			continue
		}

		b, err := os.ReadFile(filepath.Join("/sys/block", name, "device", "wwid"))
		if err != nil {
			continue
		}

		w := strings.ToLower(strings.TrimSpace(string(b)))
		w = strings.TrimPrefix(w, "naa.")
		w = strings.TrimPrefix(w, "0x")
		w = strings.TrimPrefix(w, "eui.")

		if w == rest {
			add(name)
		}
	}

	return ordered, nil
}

// RemoveDiskDevice removes the FC disk device from the system.
//
// The devices should be removed from the host before being unmapped on the storage array.
// Removing a LUN mapping immediately can cause the device to be trapped in unresponsive (D state)
// if there are still open references to it, for example by udev.
func (c *connectorSCSIFC) RemoveDiskDevice(ctx context.Context, devicePath string) error {
	if devicePath == "" {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Make sure the symlink is resolved to prevent udev symlink flapping.
	// If it fails, fallback to the original path.
	realPath, err := filepath.EvalSymlinks(devicePath)
	if err != nil {
		realPath = devicePath
	}

	deviceName := filepath.Base(realPath)

	// removeDevice safely attempts to write to the SCSI delete attribute.
	removeDevice := func(devName string) error {
		path := filepath.Join("/sys/block", devName, "device", "delete")
		err := os.WriteFile(path, []byte("1"), 0200)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		// If the device is gone, we are done.
		if !shared.PathExists(realPath) {
			return nil
		}

		if isMultipathDevice(realPath) {
			// Build the set of SCSI paths to remove. Start with the
			// multipath device's current slaves, then extend with any
			// /sys/block/sd* whose VPD-83 wwid matches the multipath's
			// wwid. Paths that multipathd has previously failed and
			// dropped from the map are no longer slaves but still exist
			// as kernel SCSI devices. Leaving them behind across cycles
			// is what causes "Logical unit not supported" probe storms
			// and stale dm tables with non-existent path minors on the
			// next reuse of this wwid.
			toRemove, err := collectSCSIPathsForMpath(deviceName)
			if err != nil {
				return fmt.Errorf("Failed enumerating SCSI paths for multipath device %q: %w", devicePath, err)
			}

			var flushErr error
			for range 10 {
				_, flushErr = shared.RunCommand(ctx, "multipath", "-f", devicePath)

				// Break if successful or if the device vanished during the command.
				if flushErr == nil || !shared.PathExists(realPath) {
					break
				}

				select {
				case <-ctx.Done():
					return fmt.Errorf("Timeout exceeded waiting to flush multipath device %q: %w", devicePath, ctx.Err())
				case <-ticker.C:
				}
			}

			// Only return a failure if the map still exists after our retries.
			if flushErr != nil && shared.PathExists(realPath) {
				return fmt.Errorf("Failed removing multipath device %q: %w", devicePath, flushErr)
			}

			// Annihilate the underlying physical SCSI paths.
			for _, sn := range toRemove {
				if err := removeDevice(sn); err != nil {
					return fmt.Errorf("Failed removing SCSI path %q for %q: %w", sn, devicePath, err)
				}
			}

			// Wait for each SCSI path's /sys/block entry to actually
			// disappear. device/delete is asynchronous: returning
			// while the kernel still holds the (host,channel,target,
			// LUN) slot lets the next attach rescan short-circuit on
			// scsi_probe_and_add_lun (LUN_PRESENT) and reuse the
			// stale sd device — keeping its old wwid even after the
			// array remapped the LUN.
			for _, sn := range toRemove {
				slavePath := filepath.Join("/sys/block", sn)
				if !block.WaitDiskDeviceGone(ctx, slavePath) {
					return fmt.Errorf("Timeout exceeded waiting for SCSI path %q of %q to disappear", sn, devicePath)
				}
			}
		} else {
			// For non-multipath device (/dev/sd*), remove the device itself.
			if err := removeDevice(deviceName); err != nil {
				return fmt.Errorf("Failed removing device %q: %w", devicePath, err)
			}
		}

		// Check again immediately to avoid unnecessary delay.
		if !shared.PathExists(realPath) {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("Timeout exceeded waiting for SCSI/FC device %q to disappear: %w", devicePath, ctx.Err())
		case <-ticker.C:
		}
	}
}

// WaitDiskDeviceResize waits until the SCSI/FC disk device reflects the new size.
// For multipath devices the device-mapper map is refreshed before waiting.
func (c *connectorSCSIFC) WaitDiskDeviceResize(ctx context.Context, devicePath string, newSizeBytes int64) error {
	_, ok := ctx.Deadline()
	if !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	if isMultipathDevice(devicePath) {
		_, err := shared.RunCommand(ctx, "multipath", "-r", devicePath)
		if err != nil {
			return fmt.Errorf("Failed updating multipath SCSI/FC device %q size: %w", devicePath, err)
		}
	}

	return block.WaitDiskDeviceResize(ctx, devicePath, newSizeBytes)
}

// normalizeWWPN normalizes the WWPN string to make it comparable regardless of the format
// it's provided in. Linux sysfs reports WWPNs as "0x" with 16 hex chars ("0x210034800d7035b3"),
// while storage array might report it using colon-separated byte format ("21:00:34:80:0d:70:35:b3").
func normalizeWWPN(wwpn string) string {
	wwpn = strings.TrimSpace(wwpn)
	wwpn = strings.ToLower(wwpn)
	wwpn = strings.TrimPrefix(wwpn, "0x")
	wwpn = strings.ReplaceAll(wwpn, ":", "")
	return wwpn
}
