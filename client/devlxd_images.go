package lxd

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"

	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/cancel"
	"github.com/canonical/lxd/shared/ioprogress"
	"github.com/canonical/lxd/shared/units"
	"github.com/canonical/lxd/shared/version"
)

// ExportImage exports the image with a given fingerprint from the host's LXD.
// Note that only cached and public images can be exported.
func (r *ProtocolDevLXD) ExportImage(fingerprint string, req ImageFileRequest) (*ImageFileResponse, error) {
	if req.MetaFile == nil {
		return nil, fmt.Errorf("The MetaFile field is required")
	}

	url := api.NewURL().Scheme(r.httpBaseURL.Scheme).Host(r.httpBaseURL.Host).Path(version.APIVersion, "images", fingerprint, "export").URL
	return devLXDDownloadImage(fingerprint, url.String(), r.httpUserAgent, r.DoHTTP, req)
}

// devLXDDownloadImage downloads the image with a given fingerprint from the host's LXD.
// XXX: This is the same as "lxdDownloadImage", but properly handles the devLXD response.
func devLXDDownloadImage(fingerprint string, uri string, userAgent string, do func(*http.Request) (*http.Response, error), req ImageFileRequest) (*ImageFileResponse, error) {
	// Prepare the response
	resp := ImageFileResponse{}

	// Prepare the download request
	request, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}

	if userAgent != "" {
		request.Header.Set("User-Agent", userAgent)
	}

	// Start the request
	response, doneCh, err := cancel.CancelableDownload(req.Canceler, do, request)
	if err != nil {
		return nil, err
	}

	defer func() { _ = response.Body.Close() }()
	defer close(doneCh)

	// Handle error response.
	if response.StatusCode != http.StatusOK {
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return nil, fmt.Errorf("Failed to read response body from %q: %v", response.Request.URL.String(), err)
		}

		// XXX: Devlxd image export does not consistently return the devLXD response.
		// Therefore, try to parse the api.Response first. If that fails, then
		// parse the devLXD response.
		apiResponse := api.Response{}
		err = json.Unmarshal(body, &apiResponse)
		if err != nil {
			// Report response body in error.
			return nil, api.NewStatusError(response.StatusCode, strings.TrimSpace(string(body)))
		}

		// Return apiResponse error.
		return nil, api.StatusErrorf(apiResponse.Code, apiResponse.Error)
	}

	ctype, ctypeParams, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil {
		ctype = "application/octet-stream"
	}

	// Handle the data
	body := response.Body
	if req.ProgressHandler != nil {
		reader := &ioprogress.ProgressReader{
			ReadCloser: response.Body,
			Tracker: &ioprogress.ProgressTracker{
				Length: response.ContentLength,
			},
		}

		if response.ContentLength > 0 {
			reader.Tracker.Handler = func(percent int64, speed int64) {
				req.ProgressHandler(ioprogress.ProgressData{Text: strconv.FormatInt(percent, 10) + "% (" + units.GetByteSizeString(speed, 2) + "/s)"})
			}
		} else {
			reader.Tracker.Handler = func(received int64, speed int64) {
				req.ProgressHandler(ioprogress.ProgressData{Text: units.GetByteSizeString(received, 2) + " (" + units.GetByteSizeString(speed, 2) + "/s)"})
			}
		}

		body = reader
	}

	// Hashing
	sha256 := sha256.New()

	// Deal with split images
	if ctype == "multipart/form-data" {
		if req.MetaFile == nil || req.RootfsFile == nil {
			return nil, fmt.Errorf("Multi-part image but only one target file provided")
		}

		// Parse the POST data
		mr := multipart.NewReader(body, ctypeParams["boundary"])

		// Get the metadata tarball
		part, err := mr.NextPart()
		if err != nil {
			return nil, err
		}

		if part.FormName() != "metadata" {
			return nil, fmt.Errorf("Invalid multipart image")
		}

		size, err := io.Copy(io.MultiWriter(req.MetaFile, sha256), part)
		if err != nil {
			return nil, err
		}

		resp.MetaSize = size
		resp.MetaName = part.FileName()

		// Get the rootfs tarball
		part, err = mr.NextPart()
		if err != nil {
			return nil, err
		}

		if !shared.ValueInSlice(part.FormName(), []string{"rootfs", "rootfs.img"}) {
			return nil, fmt.Errorf("Invalid multipart image")
		}

		size, err = io.Copy(io.MultiWriter(req.RootfsFile, sha256), part)
		if err != nil {
			return nil, err
		}

		resp.RootfsSize = size
		resp.RootfsName = part.FileName()

		// Check the hash
		hash := fmt.Sprintf("%x", sha256.Sum(nil))
		if !strings.HasPrefix(hash, fingerprint) {
			return nil, fmt.Errorf("Image fingerprint doesn't match. Got %s expected %s", hash, fingerprint)
		}

		return &resp, nil
	}

	// Deal with unified images
	_, cdParams, err := mime.ParseMediaType(response.Header.Get("Content-Disposition"))
	if err != nil {
		return nil, err
	}

	filename, ok := cdParams["filename"]
	if !ok {
		return nil, fmt.Errorf("No filename in Content-Disposition header")
	}

	size, err := io.Copy(io.MultiWriter(req.MetaFile, sha256), body)
	if err != nil {
		return nil, err
	}

	resp.MetaSize = size
	resp.MetaName = filename

	// Check the hash
	hash := fmt.Sprintf("%x", sha256.Sum(nil))
	if !strings.HasPrefix(hash, fingerprint) {
		return nil, fmt.Errorf("Image fingerprint doesn't match. Got %s expected %s", hash, fingerprint)
	}

	return &resp, nil
}
