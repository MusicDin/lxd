package connectors

import (
	"context"
	"fmt"
)

const (
	// TypeUnknown represents an unknown storage connector.
	TypeUnknown string = "unknown"

	// TypeISCSI represents an iSCSI storage connector.
	TypeISCSI string = "iscsi"

	// TypeNVME represents an NVMe/TCP storage connector.
	TypeNVME string = "nvme"
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
func NewConnector(connectorType string, serverUUID string) (Connector, error) {
	common := common{
		serverUUID: serverUUID,
	}

	switch connectorType {
	case TypeISCSI:
		return &connectorISCSI{
			common: common,
		}, nil
	case TypeNVME:
		return &connectorNVMe{
			common: common,
		}, nil
	default:
		return nil, fmt.Errorf("Invalid connector type %q", connectorType)
	}
}
