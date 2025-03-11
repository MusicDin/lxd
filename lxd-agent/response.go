package main

import (
	"net/http"

	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/logger"
)

type devLxdResponse struct {
	content any
	code    int
	ctype   string
}

func errorResponse(code int, msg string) *devLxdResponse {
	return &devLxdResponse{msg, code, "raw"}
}

func okResponse(ct any, ctype string) *devLxdResponse {
	logger.Infof("Success (%s): %v", ctype, ct)
	return &devLxdResponse{ct, http.StatusOK, ctype}
}

func smartResponse(err error) *devLxdResponse {
	logger.Errorf("An error occured: %v", err)

	if err == nil {
		return okResponse(nil, "")
	}

	statusCode, found := api.StatusErrorMatch(err)
	if found {
		return errorResponse(statusCode, err.Error())
	}

	return errorResponse(http.StatusInternalServerError, err.Error())
}
