package connectors

import (
	"bytes"
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

const (
	// nvmeFCDiskDevicePrefix is the prefix of the NVMe/FC disk device name in /dev/disk/by-id/.
	// NVMe namespaces are identified by EUI regardless of the transport type.
	nvmeFCDiskDevicePrefix = "nvme-eui."

	nvmeFCTransportType = "fc"
)

var _ Connector = &connectorNVMeFC{}

type connectorNVMeFC struct {
	common
}

// Type returns the type of the connector.
func (c *connectorNVMeFC) Type() string {
	return TypeNVME
}

// Version returns the version of the NVMe CLI.
func (c *connectorNVMeFC) Version() (string, error) {
	out, err := shared.RunCommand(context.Background(), "nvme", "version")
	if err != nil {
		return "", fmt.Errorf("Failed getting nvme-cli version: %w", err)
	}

	fields := strings.Split(strings.TrimSpace(out), " ")
	if strings.HasPrefix(out, "nvme version ") && len(fields) > 2 {
		return fields[2] + " (nvme-cli)", nil
	}

	return "", fmt.Errorf("Failed getting nvme-cli version: Unexpected output %q", out)
}

// LoadModules loads the NVMe/FC kernel modules.
func (c *connectorNVMeFC) LoadModules() error {
	err := util.LoadModule("nvme_fabrics")
	if err != nil {
		return err
	}

	return util.LoadModule("nvme_fc")
}

// QualifiedName returns a custom NQN generated from the server UUID.
func (c *connectorNVMeFC) QualifiedName() (string, error) {
	return "nqn.2014-08.org.nvmexpress:uuid:" + c.serverUUID, nil
}

// Connect establishes an NVMe/FC connection to the target NQN at the given FC address.
// Each entry in targetAddresses must be an FC transport address in the format
// "nn-0x<WWNN>:pn-0x<WWPN>" as expected by nvme-cli --traddr.
func (c *connectorNVMeFC) Connect(ctx context.Context, targetQN string, targetAddresses ...string) (revert.Hook, error) {
	connectFunc := func(ctx context.Context, session *session, targetAddr string) error {
		if session != nil && slices.Contains(session.addresses, targetAddr) {
			// Already connected on this path.
			return nil
		}

		hostNQN, err := c.QualifiedName()
		if err != nil {
			return err
		}

		_, err = shared.RunCommand(ctx, "nvme", "connect",
			"--transport", nvmeFCTransportType,
			"--traddr", targetAddr,
			"--nqn", targetQN,
			"--hostnqn", hostNQN,
			"--hostid", c.serverUUID)
		if err != nil {
			return fmt.Errorf("Failed connecting to target %q on %q via NVMe/FC: %w", targetQN, targetAddr, err)
		}

		return nil
	}

	return connect(ctx, c, targetQN, targetAddresses, connectFunc)
}

// Disconnect terminates the NVMe/FC connection with the target.
func (c *connectorNVMeFC) Disconnect(targetQN string) error {
	session, err := c.findSession(targetQN)
	if err != nil {
		return err
	}

	if session != nil {
		_, err := shared.RunCommand(context.Background(), "nvme", "disconnect", "--nqn", targetQN)
		if err != nil {
			return fmt.Errorf("Failed disconnecting from NVMe/FC target %q: %w", targetQN, err)
		}
	}

	return nil
}

// findSession returns an active NVMe/FC subsystem matching the given targetQN.
//
// Session detection is identical to NVMe/TCP: /sys/class/nvme-subsystem tracks all
// subsystems regardless of connection state (allowing recovery after transient FC
// link failures), and /sys/class/nvme provides the currently active controllers.
// The only difference is the address format — FC controllers report their address as
// "traddr=nn-0x<WWNN>:pn-0x<WWPN>,..." rather than "traddr=<IP>,trsvcid=<port>,...".
func (c *connectorNVMeFC) findSession(targetQN string) (*session, error) {
	subsysBasePath := "/sys/class/nvme-subsystem"

	subsystems, err := os.ReadDir(subsysBasePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}

		return nil, fmt.Errorf("Failed getting a list of existing NVMe subsystems: %w", err)
	}

	sessionID := ""
	for _, subsys := range subsystems {
		nqnBytes, err := os.ReadFile(filepath.Join(subsysBasePath, subsys.Name(), "subsysnqn"))
		if err != nil {
			return nil, fmt.Errorf("Failed getting the target NQN for subsystem %q: %w", subsys.Name(), err)
		}

		if strings.Contains(string(nqnBytes), targetQN) {
			sessionID = strings.TrimPrefix(subsys.Name(), "nvme-subsys")
			break
		}
	}

	if sessionID == "" {
		return nil, nil
	}

	session := &session{
		id:       sessionID,
		targetQN: targetQN,
	}

	basePath := "/sys/class/nvme"

	controllers, err := os.ReadDir(basePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return session, nil
		}

		return nil, fmt.Errorf("Failed getting a list of NVMe controllers: %w", err)
	}

	for _, ctrl := range controllers {
		nqnBytes, err := os.ReadFile(filepath.Join(basePath, ctrl.Name(), "subsysnqn"))
		if err != nil {
			return nil, fmt.Errorf("Failed getting the target NQN for controller %q: %w", ctrl.Name(), err)
		}

		if !strings.Contains(string(nqnBytes), targetQN) {
			continue
		}

		fileBytes, err := os.ReadFile(filepath.Join(basePath, ctrl.Name(), "address"))
		if err != nil {
			return nil, fmt.Errorf("Failed getting connection address of controller %q for target %q: %w", ctrl.Name(), targetQN, err)
		}

		// The address file for NVMe/FC contains lines of the form:
		// "traddr=nn-0x<WWNN>:pn-0x<WWPN>,trsvcid=none,..."
		// Extract the traddr value as the session address.
		for line := range bytes.SplitSeq(bytes.TrimSpace(fileBytes), []byte{'\n'}) {
			parts := strings.Split(string(bytes.TrimSpace(line)), ",")
			for _, part := range parts {
				addr, ok := strings.CutPrefix(part, "traddr=")
				if ok {
					session.addresses = append(session.addresses, addr)
					break
				}
			}
		}
	}

	return session, nil
}

// Discover returns NVMe/FC targets found via the given discovery controller FC addresses.
// Each entry in targetAddresses must be an FC transport address ("nn-0x<WWNN>:pn-0x<WWPN>")
// of a discovery controller.
func (c *connectorNVMeFC) Discover(ctx context.Context, targetAddresses ...string) ([]any, error) {
	hostNQN, err := c.QualifiedName()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var discoveryLog nvmeDiscoveryLog
	for _, targetAddr := range targetAddresses {
		stdout, err := shared.RunCommand(ctx, "nvme", "discover",
			"--transport", nvmeFCTransportType,
			"--traddr", targetAddr,
			"--hostnqn", hostNQN,
			"--hostid", c.serverUUID,
			"--output-format", "json")
		if err != nil {
			logger.Warn("Failed connecting to NVMe/FC discovery target", logger.Ctx{"target_address": targetAddr, "err": err})
			continue
		}

		if strings.Trim(stdout, "\n") == "No discovery log entries to fetch." {
			logger.Warn("No NVMe/FC discovery log entries", logger.Ctx{"target_address": targetAddr})
			continue
		}

		err = json.Unmarshal([]byte(stdout), &discoveryLog)
		if err != nil {
			return nil, fmt.Errorf("Failed unmarshaling NVMe/FC discovery log from %q: %w", targetAddr, err)
		}

		nvmeFilterDiscoveryLog(&discoveryLog, nvmeFCTransportType)
		break
	}

	if len(discoveryLog.Records) == 0 {
		return nil, errors.New("Failed fetching a discovery log record from any of the target addresses")
	}

	result := make([]any, 0, len(discoveryLog.Records))
	for _, value := range discoveryLog.Records {
		result = append(result, value)
	}

	return result, nil
}

// WaitDiskDevicePath waits for the mapped NVMe/FC device to appear and returns its path.
func (c *connectorNVMeFC) WaitDiskDevicePath(ctx context.Context, diskPathFilter block.DevicePathFilterFunc) (string, error) {
	return block.WaitDiskDevicePath(ctx, nvmeFCDiskDevicePrefix, diskPathFilter)
}

// GetDiskDevicePath returns the path of the mapped NVMe/FC device if it already exists.
func (c *connectorNVMeFC) GetDiskDevicePath(diskPathFilter block.DevicePathFilterFunc) (string, error) {
	return block.GetDiskDevicePath(nvmeFCDiskDevicePrefix, diskPathFilter)
}

// RemoveDiskDevice is a no-op; the device is removed when the volume is unmapped on the storage array.
func (c *connectorNVMeFC) RemoveDiskDevice(ctx context.Context, devicePath string) error {
	return nil
}

// WaitDiskDeviceResize waits until the NVMe/FC disk device reflects the new size.
func (c *connectorNVMeFC) WaitDiskDeviceResize(ctx context.Context, diskPath string, newSizeBytes int64) error {
	return block.WaitDiskDeviceResize(ctx, diskPath, newSizeBytes)
}
