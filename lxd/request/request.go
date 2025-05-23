package request

import (
	"context"
	"net"

	"github.com/canonical/lxd/shared/api"
)

// CreateRequestor extracts the lifecycle event requestor data from the request context.
func CreateRequestor(ctx context.Context) *api.EventLifecycleRequestor {
	info, _ := GetCtxInfo(ctx)
	requestor := &api.EventLifecycleRequestor{}

	// Normal requestor.
	requestor.Address = info.SourceAddress
	requestor.Username = info.Username
	requestor.Protocol = info.Protocol

	// Forwarded requestor override.
	if info.ForwardedAddress != "" {
		requestor.Address = info.ForwardedAddress
	}

	if info.ForwardedUsername != "" {
		requestor.Username = info.ForwardedUsername
	}

	if info.ForwardedProtocol != "" {
		requestor.Protocol = info.ForwardedProtocol
	}

	return requestor
}

// SaveConnectionInContext can be set as the ConnContext field of a http.Server to set the connection
// in the request context for later use.
func SaveConnectionInContext(ctx context.Context, connection net.Conn) context.Context {
	return context.WithValue(ctx, CtxConn, connection)
}
