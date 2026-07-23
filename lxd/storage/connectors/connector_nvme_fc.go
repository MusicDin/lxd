package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/canonical/lxd/lxd/storage/block"
	"github.com/canonical/lxd/lxd/util"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/logger"
	"github.com/canonical/lxd/shared/revert"
)

var _ Connector = &connectorNVMeFC{}

// connectorNVMeFC is an NVMe connector that operates over the Fibre Channel
// fabric. It shares the NVMe protocol logic (host NQN, subsystem sessions and
// device identification) with the NVMe/TCP connector, but discovers and
// connects to targets through the FC fabric rather than over IP.
type connectorNVMeFC struct {
	common
}

// Type returns the type of the connector.
func (c *connectorNVMeFC) Type() string {
	return TypeNVMeFC
}

// Transport returns the transport type of the connector.
func (c *connectorNVMeFC) Transport() TransportType {
	return TransportFC
}

// Version returns the version of the NVMe CLI.
func (c *connectorNVMeFC) Version() (string, error) {
	return nvmeVersion()
}

// LoadModules loads the NVMe/FC kernel modules.
func (c *connectorNVMeFC) LoadModules() error {
	err := util.LoadModule("nvme_fabrics")
	if err != nil {
		return err
	}

	return util.LoadModule("nvme_fc")
}

// QualifiedName returns a custom host NQN generated from the server UUID.
func (c *connectorNVMeFC) QualifiedName() (string, error) {
	return nvmeQualifiedName(c.serverUUID)
}

// Connect establishes a connection with the target subsystem over the FC fabric.
// The provided target addresses are the subsystem's FC transport addresses
// ("nn-<wwnn>:pn-<wwpn>"). Each target port is connected through every local FC
// HBA that can reach it, establishing a path per local HBA for multipathing.
func (c *connectorNVMeFC) Connect(ctx context.Context, targetQN string, targetAddresses ...string) (revert.Hook, error) {
	connectFunc := func(ctx context.Context, session *session, targetAddr string) error {
		if session != nil && slices.Contains(session.addresses, targetAddr) {
			// Already connected.
			return nil
		}

		hostNQN, err := c.QualifiedName()
		if err != nil {
			return err
		}

		hostAddrs, err := localFCTransportAddresses()
		if err != nil {
			return err
		}

		// Attempt to connect the target port through every local HBA. A port is
		// generally reachable only through the HBAs it is zoned to, so failures
		// on the other HBAs are expected and ignored as long as one path succeeds.
		var connected bool
		var lastErr error
		for _, hostAddr := range hostAddrs {
			_, err = shared.RunCommand(ctx, "nvme", "connect", "--transport", "fc", "--traddr", targetAddr, "--host-traddr", hostAddr, "--nqn", targetQN, "--hostnqn", hostNQN, "--hostid", c.serverUUID)
			if err != nil {
				lastErr = err
				continue
			}

			connected = true
		}

		if !connected {
			return fmt.Errorf("Failed connecting to target %q on %q via NVMe/FC: %w", targetQN, targetAddr, lastErr)
		}

		return nil
	}

	return connect(ctx, c, targetQN, targetAddresses, connectFunc)
}

// Disconnect terminates a connection with the target.
func (c *connectorNVMeFC) Disconnect(targetQN string) error {
	return nvmeDisconnect(c, targetQN)
}

// findSession returns an active NVMe subsystem that matches the given targetQN.
func (c *connectorNVMeFC) findSession(targetQN string) (*session, error) {
	return nvmeFindSession(targetQN, c.Transport())
}

// Discover returns the NVMe subsystems reachable over the FC fabric.
//
// The provided target addresses are the array's NVMe FC target ports (obtained
// from the storage array), which are used to bootstrap the discovery. For each
// target address, the discovery is run against every local FC HBA until one
// succeeds. The array's discovery controller returns the full set of subsystem
// ports, so a single successful discovery per target is sufficient.
func (c *connectorNVMeFC) Discover(ctx context.Context, targetAddresses ...string) ([]any, error) {
	hostNQN, err := c.QualifiedName()
	if err != nil {
		return nil, err
	}

	// Set a deadline for the overall discovery.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	hostAddrs, err := localFCTransportAddresses()
	if err != nil {
		return nil, err
	}

	var discoveryLog nvmeDiscoveryLog
	seen := make(map[string]struct{})

	for _, targetAddr := range targetAddresses {
		for _, hostAddr := range hostAddrs {
			stdout, err := shared.RunCommand(ctx, "nvme", "discover", "--transport", "fc", "--traddr", targetAddr, "--host-traddr", hostAddr, "--hostnqn", hostNQN, "--hostid", c.serverUUID, "--output-format", "json")
			if err != nil {
				// The target may not be reachable through this local HBA.
				logger.Warn("Failed connecting to NVMe/FC discovery target", logger.Ctx{"target_address": targetAddr, "host_address": hostAddr, "err": err})
				continue
			}

			// In case no discovery log entries can be fetched the nvme command doesn't return JSON formatted text.
			if strings.Trim(stdout, "\n") == "No discovery log entries to fetch." {
				continue
			}

			var log nvmeDiscoveryLog
			err = json.Unmarshal([]byte(stdout), &log)
			if err != nil {
				// Don't just log this error.
				// Something is clearly wrong with the returned output.
				return nil, fmt.Errorf("Failed unmarshaling the returned discovery log entries from %q: %w", targetAddr, err)
			}

			nvmeFilterDiscoveryLog(&log, nvmeTransportTypeFC)

			// Accumulate unique records.
			for _, record := range log.Records {
				key := record.SubNQN + "|" + record.TransportAddress
				_, ok := seen[key]
				if ok {
					continue
				}

				seen[key] = struct{}{}
				discoveryLog.Records = append(discoveryLog.Records, record)
			}

			// The discovery controller returned the full log for this target,
			// so there is no need to try the remaining local HBAs.
			break
		}
	}

	if len(discoveryLog.Records) == 0 {
		return nil, errors.New("Failed fetching a discovery log record from any of the FC targets")
	}

	result := make([]any, 0, len(discoveryLog.Records))
	for _, value := range discoveryLog.Records {
		result = append(result, value)
	}

	return result, nil
}

// WaitDiskDevicePath waits for the mapped device to appear and returns its path.
func (c *connectorNVMeFC) WaitDiskDevicePath(ctx context.Context, diskPathFilter block.DevicePathFilterFunc) (string, error) {
	return block.WaitDiskDevicePath(ctx, nvmeDiskDevicePrefix, diskPathFilter)
}

// GetDiskDevicePath returns the path of the mapped device if it exists.
func (c *connectorNVMeFC) GetDiskDevicePath(diskPathFilter block.DevicePathFilterFunc) (string, error) {
	return block.GetDiskDevicePath(nvmeDiskDevicePrefix, diskPathFilter)
}

// RemoveDiskDevice does nothing. Device is removed when volume is unmapped on the storage array.
func (c *connectorNVMeFC) RemoveDiskDevice(ctx context.Context, devicePath string) error {
	return nil
}

// WaitDiskDeviceResize waits until the disk device reflects the new size.
func (c *connectorNVMeFC) WaitDiskDeviceResize(ctx context.Context, diskPath string, newSizeBytes int64) error {
	return block.WaitDiskDeviceResize(ctx, diskPath, newSizeBytes)
}

// nvmeFCTransportAddress builds an NVMe/FC transport address ("nn-<wwnn>:pn-<wwpn>")
// from the FC node name and port name as reported by sysfs (e.g. "0x2000..").
func nvmeFCTransportAddress(nodeName string, portName string) string {
	return "nn-" + strings.TrimSpace(nodeName) + ":pn-" + strings.TrimSpace(portName)
}

// fcTransportAddressFromPath reads the node_name and port_name attributes from
// the given sysfs FC port directory and returns the corresponding NVMe/FC
// transport address.
func fcTransportAddressFromPath(dirPath string) (string, error) {
	nodeName, err := os.ReadFile(filepath.Join(dirPath, "node_name"))
	if err != nil {
		return "", err
	}

	portName, err := os.ReadFile(filepath.Join(dirPath, "port_name"))
	if err != nil {
		return "", err
	}

	return nvmeFCTransportAddress(string(nodeName), string(portName)), nil
}

// localFCTransportAddresses returns the NVMe/FC transport addresses
// ("nn-<wwnn>:pn-<wwpn>") of the local FC HBA ports that are online. These are
// used as host transport addresses when discovering and connecting to targets.
func localFCTransportAddresses() ([]string, error) {
	fcHostBasePath := "/sys/class/fc_host"

	hosts, err := os.ReadDir(fcHostBasePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, errors.New("No FC host adapters found")
		}

		return nil, fmt.Errorf("Failed reading FC hosts: %w", err)
	}

	var addresses []string
	for _, host := range hosts {
		hostPath := filepath.Join(fcHostBasePath, host.Name())

		// Skip ports that are not online.
		stateBytes, err := os.ReadFile(filepath.Join(hostPath, "port_state"))
		if err != nil || strings.TrimSpace(string(stateBytes)) != "Online" {
			continue
		}

		addr, err := fcTransportAddressFromPath(hostPath)
		if err != nil {
			continue
		}

		addresses = append(addresses, addr)
	}

	if len(addresses) == 0 {
		return nil, errors.New("No online local FC HBA ports found")
	}

	return addresses, nil
}
