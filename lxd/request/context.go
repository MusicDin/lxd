package request

import (
	"context"
	"fmt"
	"net/http"
)

// Info represents the request information that are stored in the request
// context, which is passed around.
type Info struct{}

// GetCtxValue gets a value of type T from the context using the given key.
func GetCtxValue[T any](ctx context.Context, key CtxKey) (T, error) {
	var empty T
	valueAny := ctx.Value(key)
	if valueAny == nil {
		return empty, fmt.Errorf("Failed to get expected value %q from context", key)
	}

	value, ok := valueAny.(T)
	if !ok {
		return empty, fmt.Errorf("Value for context key %q has incorrect type (expected %T, got %T)", key, empty, valueAny)
	}

	return value, nil
}

// SetCtxValue sets the given value in the request context with the given key.
func SetCtxValue(r *http.Request, key CtxKey, value any) {
	ctx := context.WithValue(r.Context(), key, value)
	*r = *r.WithContext(ctx)
}

// IsRequestContext checks if the given context is a request context.
// This is determined by checking the presence of the request information in the context.
func IsRequestContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}

	_, ok := GetCtxInfo(ctx)
	return ok
}

// GetCtxInfo gets the request information from the request context.
func GetCtxInfo(reqContext context.Context) (*Info, bool) {
	info, ok := reqContext.Value(CtxRequestInfo).(*Info)
	if !ok {
		return &Info{}, false
	}

	return info, true
}

// SetCtxInfo sets the request information in the request context.
func SetCtxInfo(r *http.Request, info *Info) {
	SetCtxValue(r, CtxRequestInfo, info)
}
