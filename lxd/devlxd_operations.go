package main

import (
	"net/http"
	"strconv"

	"github.com/gorilla/mux"

	"github.com/canonical/lxd/lxd/response"
	"github.com/canonical/lxd/shared/api"
)

var devLXDOperationsWaitEndpoint = devLXDAPIEndpoint{
	Path: "operations/{id}/wait",
	Get:  devLXDAPIEndpointAction{Handler: devLXDOperationsWaitGetHandler},
}

func devLXDOperationsWaitGetHandler(d *Daemon, r *http.Request) response.Response {
	inst, err := getInstanceFromContextAndCheckSecurityFlags(r.Context(), devLXDSecurityKey)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	// Allow access only to the projectName where current instance is running.
	projectName := inst.Project().Name
	opID := mux.Vars(r)["id"]

	// Determine the timeout based on the timeout query parameter and the request context's deadline.
	timeout := -1
	queryTimeout := r.FormValue("timeout")
	if queryTimeout != "" {
		timeout, err = strconv.Atoi(queryTimeout)
		if err != nil {
			return response.DevLXDErrorResponse(api.NewStatusError(http.StatusBadRequest, "Invalid timeout value"))
		}
	}

	// Wait for the operation to complete or timeout to be reached.
	url := api.NewURL().Path("1.0", "operations", opID).WithQuery("timeout", strconv.FormatInt(int64(timeout), 10)).WithQuery("project", projectName)
	req, err := NewRequestWithContext(r.Context(), http.MethodGet, url.String(), nil, "")
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	resp := operationWaitGet(d, req)
	op, err := RenderToOperation(req, resp)
	if err != nil {
		return response.DevLXDErrorResponse(err)
	}

	respOp := api.DevLXDOperation{
		ID:         op.ID,
		Status:     op.Status,
		StatusCode: op.StatusCode,
		Err:        op.Err,
	}

	// TODO: Filter allowed operations based on the security keys.

	return response.DevLXDResponse(http.StatusOK, respOp, "json")
}
