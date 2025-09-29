package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/google/uuid"
	"github.com/gorilla/mux"

	"github.com/canonical/lxd/lxd/auth"
	"github.com/canonical/lxd/lxd/db"
	dbCluster "github.com/canonical/lxd/lxd/db/cluster"
	"github.com/canonical/lxd/lxd/request"
	"github.com/canonical/lxd/lxd/response"
	"github.com/canonical/lxd/lxd/util"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/entity"
	"github.com/canonical/lxd/shared/version"
)

var oidcSessionsCmd = APIEndpoint{
	Name:        "oidc_sessions",
	Path:        "auth/oidc-sessions",
	MetricsType: entity.TypeIdentity,
	Get: APIEndpointAction{
		Handler:       oidcSessionsGet,
		AccessHandler: allowAuthenticated,
	},
}

var oidcSessionCmd = APIEndpoint{
	Name:        "oidc_session",
	Path:        "auth/oidc-sessions/{id}",
	MetricsType: entity.TypeIdentity,
	Get: APIEndpointAction{
		Handler: oidcSessionGet,
		// Caller can view the session if they can view the identity.
		// All identities can view themselves.
		AccessHandler: oidcSessionAccessHandler(auth.EntitlementCanView),
	},
	Delete: APIEndpointAction{
		Handler: oidcSessionDelete,
		// Caller can delete the session if they can delete the identity.
		// All identities can delete themselves.
		AccessHandler: oidcSessionAccessHandler(auth.EntitlementCanDelete),
	},
}

const ctxOIDCSessionDetails request.CtxKey = "session-details"

func oidcSessionAccessHandler(entitlement auth.Entitlement) func(d *Daemon, r *http.Request) response.Response {
	return func(d *Daemon, r *http.Request) response.Response {
		s := d.State()
		sessionIDStr, err := url.PathUnescape(mux.Vars(r)["id"])
		if err != nil {
			return response.SmartError(err)
		}

		sessionID, err := uuid.Parse(sessionIDStr)
		if err != nil {
			return response.BadRequest(fmt.Errorf("Bad session ID: %w", err))
		}

		var session *dbCluster.OIDCSession
		err = s.DB.Cluster.Transaction(r.Context(), func(ctx context.Context, tx *db.ClusterTx) error {
			session, err = dbCluster.GetOIDCSessionByUUID(ctx, tx.Tx(), sessionID)
			return err
		})
		if err != nil {
			if api.StatusErrorCheck(err, http.StatusNotFound) {
				return response.NotFound(nil)
			}

			return response.SmartError(err)
		}

		err = s.Authorizer.CheckPermission(r.Context(), entity.IdentityURL(api.AuthenticationMethodOIDC, session.Email), entitlement)
		if err != nil {
			return response.SmartError(err)
		}

		request.SetContextValue(r, ctxOIDCSessionDetails, *session)
		return response.EmptySyncResponse
	}
}

// swagger:operation GET /1.0/auth/oidc-sessions oidc_sessions oidc_sessions_get
//
//	Get OIDC session URLs
//
//	Returns a list of OIDC sessions (URLs).
//
//	---
//	produces:
//	  - application/json
//	parameters:
//		- in: query
//		  name: email
//		  description: Email address of user
//		  type: string
//		  example: jane.doe@example.com
//	responses:
//	  "200":
//	    description: API endpoints
//	    schema:
//	      type: object
//	      description: Sync response
//	      properties:
//	        type:
//	          type: string
//	          description: Response type
//	          example: sync
//	        status:
//	          type: string
//	          description: Status description
//	          example: Success
//	        status_code:
//	          type: integer
//	          description: Status code
//	          example: 200
//	        metadata:
//	          type: array
//	          description: List of endpoints
//	          items:
//	            type: string
//	          example: |-
//	            [
//	              "/1.0/auth/oidc-sessions/01993cf9-7cf5-7ecb-8946-7736875a8322",
//	              "/1.0/auth/oidc-sessions/01993cf9-a97e-76ef-9382-4434fee8b469"
//	            ]
//	  "403":
//	    $ref: "#/responses/Forbidden"
//	  "500":
//	    $ref: "#/responses/InternalServerError"

// swagger:operation GET /1.0/auth/oidc-sessions?recursion=1 oidc_sessions oidc_sessions_get_recursion1
//
//	Get the OIDC sessions
//
//	Returns a list of OIDC sessions.
//
//	---
//	produces:
//	  - application/json
//	parameters:
//		- in: query
//		  name: email
//		  description: Email address of user
//		  type: string
//		  example: jane.doe@example.com
//	responses:
//	  "200":
//	    description: API endpoints
//	    schema:
//	      type: object
//	      description: Sync response
//	      properties:
//	        type:
//	          type: string
//	          description: Response type
//	          example: sync
//	        status:
//	          type: string
//	          description: Status description
//	          example: Success
//	        status_code:
//	          type: integer
//	          description: Status code
//	          example: 200
//	        metadata:
//	          type: array
//	          description: List of auth groups
//	          items:
//	            $ref: "#/definitions/OIDCSession"
//	  "403":
//	    $ref: "#/responses/Forbidden"
//	  "500":
//	    $ref: "#/responses/InternalServerError"
func oidcSessionsGet(d *Daemon, r *http.Request) response.Response {
	recurse := util.IsRecursionRequest(r)
	email := request.QueryParam(r, "email")

	s := d.State()

	// Caller can view a session if they can view the identity that holds the session.
	// All identities can view themselves.
	canViewIdentity, err := s.Authorizer.GetPermissionChecker(r.Context(), auth.EntitlementCanView, entity.TypeIdentity)
	if err != nil {
		return response.SmartError(err)
	}

	var sessions []dbCluster.OIDCSession
	err = s.DB.Cluster.Transaction(r.Context(), func(ctx context.Context, tx *db.ClusterTx) error {
		if email != "" {
			sessions, err = dbCluster.GetOIDCSessionsByEmail(ctx, tx.Tx(), email)
			return err
		}

		sessions, err = dbCluster.GetAllOIDCSessions(ctx, tx.Tx())
		return err
	})
	if err != nil {
		return response.SmartError(err)
	}

	apiSessions := make([]api.OIDCSession, 0, len(sessions))
	sessionURLs := make([]string, 0, len(sessions))
	for _, session := range sessions {
		if !canViewIdentity(entity.IdentityURL(api.AuthenticationMethodOIDC, session.Email)) {
			continue
		}

		if recurse {
			apiSessions = append(apiSessions, session.ToAPI())
		} else {
			sessionURLs = append(sessionURLs, api.NewURL().Path(version.APIVersion, "auth", "oidc-sessions", session.UUID.String()).String())
		}
	}

	if recurse {
		return response.SyncResponse(true, apiSessions)
	}

	return response.SyncResponse(true, sessionURLs)
}

// swagger:operation GET /1.0/auth/oidc-sessions/{id} oidc_sessions oidc_session_get
//
//	Get the OIDC session
//
//	Gets a specific OIDC session.
//
//	---
//	produces:
//	  - application/json
//	responses:
//	  "200":
//	    schema:
//	      type: object
//	      description: Sync response
//	      properties:
//	        type:
//	          type: string
//	          description: Response type
//	          example: sync
//	        status:
//	          type: string
//	          description: Status description
//	          example: Success
//	        status_code:
//	          type: integer
//	          description: Status code
//	          example: 200
//	        metadata:
//	          $ref: "#/definitions/OIDCSession"
//	  "403":
//	    $ref: "#/responses/Forbidden"
//	  "500":
//	    $ref: "#/responses/InternalServerError"
func oidcSessionGet(d *Daemon, r *http.Request) response.Response {
	dbSessionDetails, err := request.GetContextValue[dbCluster.OIDCSession](r.Context(), ctxOIDCSessionDetails)
	if err != nil {
		return response.SmartError(err)
	}

	return response.SyncResponse(true, dbSessionDetails.ToAPI())
}

// swagger:operation DELETE /1.0/auth/oidc-sessions/{id} oidc_sessions oidc_session_delete
//
//	Delete an OIDC session
//
//	Deletes the OIDC session
//
//	---
//	produces:
//	  - application/json
//	responses:
//	  "200":
//	    $ref: "#/responses/EmptySyncResponse"
//	  "400":
//	    $ref: "#/responses/BadRequest"
//	  "403":
//	    $ref: "#/responses/Forbidden"
//	  "500":
//	    $ref: "#/responses/InternalServerError"
func oidcSessionDelete(d *Daemon, r *http.Request) response.Response {
	dbSessionDetails, err := request.GetContextValue[dbCluster.OIDCSession](r.Context(), ctxOIDCSessionDetails)
	if err != nil {
		return response.SmartError(err)
	}

	s := d.State()
	err = s.DB.Cluster.Transaction(r.Context(), func(ctx context.Context, tx *db.ClusterTx) error {
		return dbCluster.DeleteOIDCSessionByUUID(ctx, tx.Tx(), dbSessionDetails.UUID)
	})
	if err != nil {
		return response.SmartError(err)
	}

	return response.EmptySyncResponse
}
