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

	// TypeNVME represents an NVMe storage connector.
	TypeNVME string = "nvme"

	// TypeSDC represents Dell SDC storage connector.
	TypeSDC string = "sdc"

	// TypeISCSI represents an iSCSI storage connector.
	TypeISCSI string = "iscsi"
)

const (
	// TransportTCP represents the TCP/IP transport layer.
	TransportTCP string = "tcp"

	// TransportFC represents the Fibre Channel transport layer.
	TransportFC string = "fc"
)

// Spec identifies a connector by its protocol type and transport layer.
type Spec struct {
	Type      string
	Transport string
}

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
	Transport() string
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
func NewConnector(connectorType string, transport string, serverUUID string) (Connector, error) {
	if transport == "" {
		transport = TransportTCP
	}

	switch connectorType {
	case TypeNVME:
		c := common{serverUUID: serverUUID, transport: transport}
		if transport == TransportFC {
			return &connectorNVMeFC{common: c}, nil
		}

		return &connectorNVMe{common: c}, nil

	case TypeISCSI:
		c := common{serverUUID: serverUUID, transport: transport}
		if transport == TransportFC {
			return &connectorISCSIFC{common: c}, nil
		}

		return &connectorISCSI{common: c}, nil

	case TypeSDC:
		return &connectorSDC{common: common{serverUUID: serverUUID}}, nil

	default:
		return nil, fmt.Errorf("Unknown storage connector type %q", connectorType)
	}
}

// GetSupportedVersions returns the versions for the given connector specs,
// ignoring those that produce an error when version is being retrieved
// (e.g. due to missing required tools).
func GetSupportedVersions(specs []Spec) []string {
	versions := make([]string, 0, len(specs))

	for _, spec := range specs {
		connector, err := NewConnector(spec.Type, spec.Transport, "")
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
