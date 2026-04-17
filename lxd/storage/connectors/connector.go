package connectors

import (
	"context"
	"fmt"

	"github.com/canonical/lxd/lxd/storage/block"
	"github.com/canonical/lxd/shared/revert"
)

const (
	// TypeUnknown represents an unknown storage connector.
	TypeUnknown string = "unknown"

	// TypeNVME represents an NVMe over TCP storage connector.
	TypeNVME string = "nvme"

	// TypeNVMEFC represents an NVMe over FC storage connector.
	TypeNVMEFC string = "nvme-fc"

	// TypeSDC represents Dell SDC storage connector.
	TypeSDC string = "sdc"

	// TypeISCSI represents a SCSI over TCP (iSCSI) storage connector.
	TypeISCSI string = "iscsi"

	// TypeISCSIFC represents a SCSI over fiber channel storage connector.
	TypeISCSIFC string = "scsi-fc"
)

// session represents a connector session that is established with a target.
type session struct {
	// id is a unique identifier of the session.
	id string

	// targetQN is the qualified name of the target.
	targetQN string

	// addresses is a list of active addresses associated with the session.
	addresses []string
}

// Connector represents a storage connector that handles connections through
// appropriate storage subsystem.
type Connector interface {
	Type() string
	Version() (string, error)
	QualifiedName() (string, error)
	LoadModules() error
	Connect(ctx context.Context, targetQN string, targetAddrs ...string) (revert.Hook, error)
	Disconnect(targetQN string) error
	Discover(ctx context.Context, targetAddresses ...string) ([]any, error)
	GetDiskDevicePath(diskPathFilter block.DevicePathFilterFunc) (string, error)
	WaitDiskDevicePath(ctx context.Context, diskPathFilter block.DevicePathFilterFunc) (string, error)
	WaitDiskDeviceResize(ctx context.Context, diskPath string, newSizeBytes int64) error
	RemoveDiskDevice(ctx context.Context, devicePath string) error
	findSession(targetQN string) (*session, error)
}

// NewConnector instantiates a new connector for the given protocol type and transport.
// For TypeISCSI and TypeNVME an empty transport defaults to TransportTCP.
// Transport is not applicable for TypeSDC.
func NewConnector(connectorType string, serverUUID string) (Connector, error) {
	common := common{serverUUID: serverUUID}

	switch connectorType {
	case TypeNVME:
		return &connectorNVMe{common: common}, nil

	case TypeNVMEFC:
		return &connectorNVMeFC{common: common}, nil

	case TypeISCSI:
		return &connectorISCSI{common: common}, nil

	case TypeISCSIFC:
		return &connectorISCSIFC{common: common}, nil

	case TypeSDC:
		return &connectorSDC{common: common}, nil

	default:
		return nil, fmt.Errorf("Unknown storage connector type %q", connectorType)
	}
}

// GetSupportedVersions returns the versions for the given connector types,
// ignoring those that produce an error when version is being retrieved
// (e.g. due to missing required tools).
func GetSupportedVersions(connectorTypes []string) []string {
	versions := make([]string, 0, len(connectorTypes))

	for _, connectorType := range connectorTypes {
		connector, err := NewConnector(connectorType, "")
		if err != nil {
			continue
		}

		version, _ := connector.Version()
		if version != "" {
			versions = append(versions, version)
		}
	}

	return versions
}
