package connectors

import (
	"context"

	"github.com/canonical/lxd/lxd/storage/block"
)

type common struct {
	serverUUID string
}

// WaitDiskDeviceGone waits until the device at devicePath has disappeared.
// When deviceID is non-empty, the device is also considered gone if the
// identity of the block device currently at devicePath no longer matches
// deviceID, meaning the path was reused by a different device.
// It returns false if the context deadline is exceeded first.
func (c *common) WaitDiskDeviceGone(ctx context.Context, devicePath string, deviceID string) bool {
	return block.WaitDiskDeviceGoneByID(ctx, devicePath, deviceID)
}
