package connectors

import (
	"context"
)

const (
	// TypeUnknown represents an unknown storage connector.
	TypeUnknown string = "unknown"

	// TypeISCSI represents an iSCSI storage connector.
	TypeISCSI string = "iscsi"

	// TypeNVME represents an NVMe/TCP storage connector.
	TypeNVME string = "nvme"

	// TypeSDC represents Dell SDC storage connector.
	TypeSDC string = "sdc"
)

// Connector represents a storage connector that handles connections through
// appropriate storage subsystem.
type Connector interface {
	Type() string
	Version() (string, error)
	QualifiedName() (string, error)
	LoadModules() bool
	SessionID(targetQN string) (string, error)
	Connect(ctx context.Context, targetAddr string, targetQN string) error
	ConnectAll(ctx context.Context, targetAddr string) error
	Disconnect(targetQN string) error
	DisconnectAll() error
}

// NewConnector creates a new connector of the given type.
func NewConnector(connectorType string, serverUUID string) Connector {
	common := common{
		serverUUID: serverUUID,
	}

	switch connectorType {
	case TypeISCSI:
		return &connectorISCSI{
			common: common,
		}
	case TypeNVME:
		return &connectorNVMe{
			common: common,
		}
	default:
		// Return common connector if the type is unknown.
		// We want to avoid returning nil to prevent panics.
		return &common
	}
}

func GetSupportedVersions(supportedConnectors []string) []string {
	versions := make([]string, 0, len(supportedConnectors))

	// Iterate over the supported connectors, extracting version and loading
	// kernel module for each of them.
	for _, connectorType := range supportedConnectors {
		connector := NewConnector(connectorType, "")

		version, err := connector.Version()
		if err != nil {
			// Ignore the connector if the version cannot be retrieved.
			// This is due to missing tools.
			continue
		}

		versions = append(versions, version)
	}

	return versions
}
