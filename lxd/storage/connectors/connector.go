package connectors

import (
	"context"
)

const (
	// TypeUnknown represents an unknown storage connector.
	TypeUnknown string = "unknown"

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

// NewConnector instantiates a new connector of the given type.
// The caller needs to ensure connector type is validated before calling this
// function, as common (empty) connector is returned for unknown type.
func NewConnector(connectorType string, serverUUID string) Connector {
	common := common{
		serverUUID: serverUUID,
	}

	switch connectorType {
	case TypeNVME:
		return &connectorNVMe{
			common: common,
		}

	case TypeSDC:
		return &connectorSDC{
			common: common,
		}

	default:
		// Return common connector if the type is unknown. This removes
		// the need to check for nil or handle the error in the caller.
		return &common
	}
}

// GetSupportedVersions returns the versions for the given connector types
// ignoring those that produce an error when version is being retrieved
// (e.g. due to a missing required tools).
func GetSupportedVersions(connectorTypes []string) []string {
	versions := make([]string, 0, len(connectorTypes))

	// Iterate over the supported connectors, extracting version and loading
	// kernel module for each of them.
	for _, connectorType := range connectorTypes {
		version, err := NewConnector(connectorType, "").Version()
		if err != nil {
			continue
		}

		versions = append(versions, version)
	}

	return versions
}