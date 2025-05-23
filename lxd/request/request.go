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
	requestor.Address = info.SourceAddress

	// Normal requestor.
	val, ok := ctx.Value(CtxUsername).(string)
	if ok {
		requestor.Username = val
	}

	val, ok = ctx.Value(CtxProtocol).(string)
	if ok {
		requestor.Protocol = val
	}

	// Forwarded requestor override.
	val, ok = ctx.Value(CtxForwardedUsername).(string)
	if ok {
		requestor.Username = val
	}

	val, ok = ctx.Value(CtxForwardedProtocol).(string)
	if ok {
		requestor.Protocol = val
	}

	val, ok = ctx.Value(CtxForwardedAddress).(string)
	if ok {
		requestor.Address = val
	}

	return requestor
}

// SaveConnectionInContext can be set as the ConnContext field of a http.Server to set the connection
// in the request context for later use.
func SaveConnectionInContext(ctx context.Context, connection net.Conn) context.Context {
	return context.WithValue(ctx, CtxConn, connection)
}
