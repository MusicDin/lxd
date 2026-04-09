package main

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/canonical/lxd/lxd/auth"
	"github.com/canonical/lxd/lxd/instance/instancetype"
	"github.com/canonical/lxd/lxd/response"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/entity"
)

var instanceBitmapsCmd = APIEndpoint{
	Path:            "instances/{name}/bitmaps",
	MetricsType:     entity.TypeInstance,
	ProjectSpecific: true,

	Post: APIEndpointAction{Handler: instanceBitmapsPost, AccessHandler: allowPermission(entity.TypeInstance, auth.EntitlementCanEdit, "name")},
}

// swagger:operation POST /1.0/instances/{name}/bitmaps instances instance_bitmaps_post
//
//	Create instance bitmaps
//
//	Creates a dirty bitmap of the given name on every disk that the instance NBD export serves, which are the
//	writable non-shared block disks of the instance.
//
//	---
//	consumes:
//	  - application/json
//	produces:
//	  - application/json
//	parameters:
//	  - in: query
//	    name: project
//	    description: Project name
//	    type: string
//	    example: default
//	  - in: body
//	    name: bitmap
//	    description: Bitmap
//	    required: true
//	    schema:
//	      $ref: "#/definitions/StorageVolumeBitmapsPost"
//	responses:
//	  "200":
//	    $ref: "#/responses/EmptySyncResponse"
//	  "400":
//	    $ref: "#/responses/BadRequest"
//	  "403":
//	    $ref: "#/responses/Forbidden"
//	  "404":
//	    $ref: "#/responses/NotFound"
//	  "500":
//	    $ref: "#/responses/InternalServerError"
func instanceBitmapsPost(d *Daemon, r *http.Request) response.Response {
	s := d.State()

	inst, _, _, resp := forwardedInstanceResponseWithInstance(s, r)
	if resp != nil {
		return resp
	}

	if inst.Type() != instancetype.VM {
		return response.BadRequest(errors.New("Bitmaps are only supported by virtual machines"))
	}

	if !inst.IsRunning() {
		return response.BadRequest(errors.New("Creating bitmaps requires the instance to be running"))
	}

	req := api.StorageVolumeBitmapsPost{}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return response.BadRequest(err)
	}

	if req.Name == "" {
		return response.BadRequest(errors.New("Bitmap name is required"))
	}

	err = inst.CreateBitmap(nil, req)
	if err != nil {
		return response.SmartError(err)
	}

	return response.EmptySyncResponse
}
