package drivers

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"

	"github.com/canonical/lxd/lxd/locking"
	"github.com/canonical/lxd/lxd/resources"
	"github.com/canonical/lxd/lxd/util"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/logger"
	"github.com/canonical/lxd/shared/revert"
)

// loadISCSIModules loads the iSCSI kernel modules.
// Returns true if the modules can be loaded.
func (d *pure) loadISCSIModules() bool {
	return util.LoadModule("iscsi_tcp") == nil
}

// pureVolTypePrefixes maps volume type to storage volume name prefix.
// Use smallest possible prefixes since PureStorage volume names are limited to 63 characters.
var pureVolTypePrefixes = map[VolumeType]string{
	VolumeTypeContainer: "c",
	VolumeTypeVM:        "v",
	VolumeTypeImage:     "i",
	VolumeTypeCustom:    "u",
}

// pureContentTypeSuffixes maps volume's content type to storage volume name suffix.
var pureContentTypeSuffixes = map[ContentType]string{
	// Suffix used for block content type volumes.
	ContentTypeBlock: "b",

	// Suffix used for ISO content type volumes.
	ContentTypeISO: "i",
}

// pureError represents an error responses from PureStorage API.
type pureError struct {
	// List of errors returned by the PureStorage API.
	Errors []struct {
		Context string `json:"context"`
		Message string `json:"message"`
	} `json:"errors"`

	// StatusCode is not part of the response body but is used
	// to store the HTTP status code.
	StatusCode int `json:"-"`
}

// HTTPStatusCode returns the HTTP status code from the PureStorage API error.
func (p *pureError) HTTPStatusCode() int {
	return p.StatusCode
}

// AllErrors tries to return all kinds of errors from the PureStorage API in a nicely formatted way.
func (p *pureError) AllErrors() string {
	var errorStrings []string
	for _, err := range p.Errors {
		errorStrings = append(errorStrings, err.Message)
	}

	return strings.Join(errorStrings, ", ")
}

// Error returns the first error message from the PureStorage API error.
func (p *pureError) Error() string {
	if p == nil || len(p.Errors) == 0 {
		return ""
	}

	// Return the first error message without the trailing dot.
	return strings.TrimSuffix(p.Errors[0].Message, ".")
}

// Is returns true if the error status code is equal to the provided status code and the error message
// contains the provided substring.
func (p *pureError) Is(statusCode int, substring string) bool {
	if p.StatusCode != statusCode {
		return false
	}

	for _, err := range p.Errors {
		if strings.Contains(err.Message, substring) {
			return true
		}
	}

	return false
}

// IsNotFoundError returns true if the error status code is 400 (bad request)
// and the message contains "does not exist".
func (p *pureError) IsNotFoundError() bool {
	if p.StatusCode != http.StatusBadRequest {
		return false
	}

	for _, err := range p.Errors {
		if strings.Contains(err.Message, "does not exist") {
			return true
		}
	}

	return false
}

// pureResponse wraps the response from the PureStorage API. In most cases, the response
// contains a list of items, even if only one item is returned.
type pureResponse[T any] struct {
	Items []T `json:"items"`
}

// pureEntity represents a generic entity in PureStorage.
type pureEntity struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// pureSpace represents the usage data of PureStorage resource.
type pureSpace struct {
	// Total reserved space.
	// For volumes, this is the available space or quota.
	// For storage pools, this is the total reserved space (not the quota).
	TotalBytes int64 `json:"total_provisioned"`

	// Amount of logically written data that a volume or a snapshot references.
	// This value is compared against the quota, therefore, it should be used for
	// showing the actual used space. Although, the actual used space is most likely
	// less than this value due to the data reduction that is done by PureStorage.
	UsedBytes int64 `json:"virtual"`
}

// pureStorageArray represents a storage array in PureStorage.
type pureStorageArray struct {
	ID       string    `json:"id"`
	Name     string    `json:"name"`
	Capacity int64     `json:"capacity"`
	Space    pureSpace `json:"space"`
}

// pureStoragePool represents a storage pool (Pod) in PureStorage.
type pureStoragePool struct {
	ID          string       `json:"id"`
	Name        string       `json:"name"`
	Quota       int64        `json:"quota_limit"`
	IsDestroyed bool         `json:"destroyed"`
	Space       pureSpace    `json:"space"`
	Arrays      []pureEntity `json:"arrays"`
}

// pureVolume represents a volume in PureStorage.
type pureVolume struct {
	ID    string    `json:"id"`
	Name  string    `json:"name"`
	Space pureSpace `json:"space"`
}

// pureHost represents a host in PureStorage.
type pureHost struct {
	Name            string   `json:"name"`
	IQNs            []string `json:"iqns"`
	ConnectionCount int      `json:"connection_count"`
}

// pureTarget represents a target in PureStorage. e
// Expand this struct if more fields, such as NQN, are needed.
type pureTarget struct {
	IQN    *string `json:"iqn"`
	Portal *string `json:"portal"`
}

// pureClient holds the PureStorage HTTP client and an access token.
type pureClient struct {
	driver      *pure
	accessToken string
}

// newPowerFlexClient creates a new instance of the HTTP PureStorage client.
func newPureClient(driver *pure) *pureClient {
	return &pureClient{
		driver: driver,
	}
}

// createBodyReader creates a reader for the given request body contents.
func (p *pureClient) createBodyReader(contents map[string]any) (io.Reader, error) {
	body := &bytes.Buffer{}

	err := json.NewEncoder(body).Encode(contents)
	if err != nil {
		return nil, fmt.Errorf("Failed to write request body: %w", err)
	}

	return body, nil
}

// request issues a HTTP request against the PureStorage gateway. The request
func (p *pureClient) request(method string, path string, reqBody io.Reader, reqHeaders map[string]string, respBody any, respHeaders map[string]string) error {
	var url string

	// Construct the request URL.
	if strings.HasPrefix(path, "/api") {
		// If the provided path starts with "/api", simply append it to the gateway URL.
		url = fmt.Sprintf("%s%s", p.driver.config["pure.gateway"], path)
	} else {
		// Otherwise, prefix the path with "/api/<api_version>" and then append it to the gateway URL.
		// If API version is not known yet, retrieve and cache it first.
		if p.driver.apiVersion == "" {
			apiVersions, err := p.getAPIVersions()
			if err != nil {
				return fmt.Errorf("Failed to retrieve supported PureStorage API versions: %w", err)
			}

			// Use the latest available API version.
			p.driver.apiVersion = apiVersions[len(apiVersions)-1]
		}

		url = fmt.Sprintf("%s/api/%s%s", p.driver.config["pure.gateway"], p.driver.apiVersion, path)
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return fmt.Errorf("Failed to create request: %w", err)
	}

	// Set custom request headers.
	for k, v := range reqHeaders {
		req.Header.Add(k, v)
	}

	req.Header.Add("Accept", "application/json")
	if reqBody != nil {
		req.Header.Add("Content-Type", "application/json")
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: shared.IsFalse(p.driver.config["pure.gateway.verify"]),
			},
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("Failed to send request: %w", err)
	}

	defer resp.Body.Close()

	// Wrap unauthorized requests into an API status error.
	if resp.StatusCode == http.StatusUnauthorized {
		return api.StatusErrorf(http.StatusUnauthorized, "Unauthorized request")
	}

	// Overwrite the response data type if an error is detected.
	// Both HTTP status code and PowerFlex error code get mapped to the
	// custom error struct from the response body.
	if resp.StatusCode != http.StatusOK {
		respBody = &pureError{}
	}

	// Extract the response body if requested.
	if respBody != nil {
		err = json.NewDecoder(resp.Body).Decode(respBody)
		if err != nil {
			return fmt.Errorf("Failed to read response body from %q: %w", path, err)
		}
	}

	// Extract the response headers if requested.
	if respHeaders != nil {
		for k, v := range resp.Header {
			respHeaders[k] = strings.Join(v, ",")
		}
	}

	// Return the formatted error from the body
	pureErr, ok := respBody.(*pureError)
	if ok {
		pureErr.StatusCode = resp.StatusCode
		return pureErr
	}

	return nil
}

// requestAuthenticated issues an authenticated HTTP request against the PureStorage gateway. In case
// the access token is expired, the function will try to obtain a new one.
func (p *pureClient) requestAuthenticated(method string, path string, reqBody io.Reader, respBody any) error {
	retries := 1
	for {
		// Ensure we are logged into the PureStorage.
		err := p.login()
		if err != nil {
			return err
		}

		// Set access token as request header.
		reqHeaders := map[string]string{
			"X-Auth-Token": p.accessToken,
		}

		// Initiate request.
		err = p.request(method, path, reqBody, reqHeaders, respBody, nil)
		if err != nil {
			if api.StatusErrorCheck(err, http.StatusUnauthorized) && retries > 0 {
				// Access token seems to be expired.
				// Reset the token and try one more time.
				p.accessToken = ""
				retries--
				continue
			}

			// Either the error is not of type unauthorized or the maximum number of
			// retries has been exceeded.
			return err
		}

		return nil
	}
}

// getAPIVersion returns the list of API version that are supported by the PureStorage.
func (p *pureClient) getAPIVersions() ([]string, error) {
	var resp struct {
		APIVersions []string `json:"version"`
	}

	err := p.request(http.MethodGet, "/api/api_version", nil, nil, &resp, nil)
	if err != nil {
		return nil, fmt.Errorf("Failed retrieve available API versions from PureStorage: %w", err)
	}

	if len(resp.APIVersions) == 0 {
		return nil, fmt.Errorf("PureStorage does not support any API version")
	}

	return resp.APIVersions, nil
}

// login initiates an authentication request against the PureStorage using the API token. If successful,
// an access token is retrieved and stored within a client. The access token is then used for futher
// authentication.
func (p *pureClient) login() error {
	if p.accessToken != "" {
		// Token has been already obtained.
		return nil
	}

	reqHeaders := map[string]string{
		"Api-Token": p.driver.config["pure.api.token"],
	}

	respHeaders := make(map[string]string)

	err := p.request(http.MethodPost, "/login", nil, reqHeaders, nil, respHeaders)
	if err != nil {
		return fmt.Errorf("Failed to login: %w", err)
	}

	p.accessToken = respHeaders["X-Auth-Token"]
	if p.accessToken == "" {
		return errors.New("Failed to obtain access token")

	}
	return nil
}

// getStorageArray returns the storage pool behind poolID.
func (p *pureClient) getStorageArrays(arrayNames ...string) ([]pureStorageArray, error) {
	var resp pureResponse[pureStorageArray]
	err := p.requestAuthenticated(http.MethodGet, fmt.Sprintf("/arrays?names=%s", strings.Join(arrayNames, ",")), nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("Failed to get storage arrays: %w", err)
	}

	return resp.Items, nil
}

// getStorageArray returns the storage pool behind poolID.
func (p *pureClient) getStorageArray(arrayName string) (*pureStorageArray, error) {
	var resp pureResponse[pureStorageArray]
	err := p.requestAuthenticated(http.MethodGet, fmt.Sprintf("/arrays?names=%s", arrayName), nil, &resp)
	if err != nil {
		perr, ok := err.(*pureError)
		if ok && perr.IsNotFoundError() {
			return nil, api.StatusErrorf(http.StatusNotFound, "Storage array %q not found", arrayName)
		}

		return nil, fmt.Errorf("Failed to get storage array %q: %w", arrayName, err)
	}

	if len(resp.Items) == 0 {
		return nil, api.StatusErrorf(http.StatusNotFound, "Storage array %q not found", arrayName)
	}

	return &resp.Items[0], nil
}

// getStoragePool returns the storage pool with the given name.
func (p *pureClient) getStoragePool(poolName string) (*pureStoragePool, error) {
	var resp pureResponse[pureStoragePool]
	err := p.requestAuthenticated(http.MethodGet, fmt.Sprintf("/pods?names=%s", poolName), nil, &resp)
	if err != nil {
		perr, ok := err.(*pureError)
		if ok && perr.IsNotFoundError() {
			return nil, api.StatusErrorf(http.StatusNotFound, "Storage pool %q not found", poolName)
		}

		return nil, fmt.Errorf("Failed to get storage pool %q: %w", poolName, err)
	}

	if len(resp.Items) == 0 {
		return nil, api.StatusErrorf(http.StatusNotFound, "Storage pool %q not found", poolName)
	}

	return &resp.Items[0], nil
}

// createStoragePool creates a storage pool (PureStorage Pod) and returns it's ID.
func (p *pureClient) createStoragePool(poolName string) error {
	pool, err := p.getStoragePool(poolName)
	if err == nil && pool.IsDestroyed {
		// Storage pool exists in destroyed state, therefore, restore it.
		req, err := p.createBodyReader(map[string]any{
			"destroyed": false,
		})
		if err != nil {
			return err
		}

		err = p.requestAuthenticated(http.MethodPatch, fmt.Sprintf("/pods?names=%s", poolName), req, nil)
		if err != nil {
			return fmt.Errorf("Failed to restore storage pool %q: %w", poolName, err)
		}

		logger.Warn("Storage pool has been restored", logger.Ctx{"pool": poolName})
	} else {
		// Storage pool does not exist in destroyed state, therefore, try to create a new one.
		err = p.requestAuthenticated(http.MethodPost, fmt.Sprintf("/pods?names=%s", poolName), nil, nil)
		if err != nil {
			return fmt.Errorf("Failed to create storage pool %q: %w", poolName, err)
		}
	}

	return nil
}

// deleteStoragePool deletes a storage pool (PureStorage Pod).
func (p *pureClient) deleteStoragePool(poolName string) error {
	pool, err := p.getStoragePool(poolName)
	if err != nil {
		if api.StatusErrorCheck(err, http.StatusNotFound) {
			// Storage pool has been already removed.
			return nil
		}

		return err
	}

	// To delete the storage pool, we need to destroy it first by setting the destroyed property to true.
	// In addition, we want to destroy all of its contents to allow the pool to be deleted.
	// If the pool is already destroyed, we can skip this step.
	if !pool.IsDestroyed {
		req, err := p.createBodyReader(map[string]any{
			"destroyed": true,
		})
		if err != nil {
			return err
		}

		err = p.requestAuthenticated(http.MethodPatch, fmt.Sprintf("/pods?names=%s&destroy_contents=true", poolName), req, nil)
		if err != nil {
			perr, ok := err.(*pureError)
			if ok && perr.IsNotFoundError() {
				return nil
			}

			return fmt.Errorf("Failed to destroy storage pool %q: %w", poolName, err)
		}
	}

	// Eradicate the storage pool by permanently deleting it along all of its contents.
	err = p.requestAuthenticated(http.MethodDelete, fmt.Sprintf("/pods?names=%s&eradicate_contents=true", poolName), nil, nil)
	if err != nil {
		perr, ok := err.(*pureError)
		if ok {
			if perr.IsNotFoundError() {
				return nil
			}

			if perr.Is(http.StatusBadRequest, "Cannot eradicate pod") {
				// Eradication failed, therefore the pool remains in the destroyed state.
				// However, we still consider it as deleted because PureStorage SafeMode
				// may be enabled, which prevents immediate eradication of the pool.
				logger.Warn("Storage pool is left in destroyed state", logger.Ctx{"pool": poolName, "err": err})
				return nil
			}
		}

		return fmt.Errorf("Failed to delete storage pool %q: %w", poolName, err)
	}

	return nil
}

// getVolume returns the volume behind volumeID.
func (p *pureClient) getVolume(poolName string, volName string) (*pureVolume, error) {
	var resp pureResponse[pureVolume]

	err := p.requestAuthenticated(http.MethodGet, fmt.Sprintf("/volumes?names=%s::%s", poolName, volName), nil, &resp)
	if err != nil {
		perr, ok := err.(*pureError)
		if ok && perr.IsNotFoundError() {
			return nil, api.StatusErrorf(http.StatusNotFound, "Volume %q not found", volName)
		}

		return nil, fmt.Errorf("Failed to get volume %q: %w", volName, err)
	}

	if len(resp.Items) == 0 {
		return nil, api.StatusErrorf(http.StatusNotFound, "Volume %q not found", volName)
	}

	return &resp.Items[0], nil
}

// createVolume creates a new volume in the given storage pool. The volume is created with
// supplied size in bytes. Upon successful creation, volume's ID is returned.
func (p *pureClient) createVolume(poolName string, volName string, sizeBytes int64) error {
	req, err := p.createBodyReader(map[string]any{
		"provisioned": sizeBytes,
	})
	if err != nil {
		return err
	}

	// Prevent default protection groups to be applied on the new volume, which can
	// prevent us from eradicating the volume once deleted.
	err = p.requestAuthenticated(http.MethodPost, fmt.Sprintf("/volumes?names=%s::%s&with_default_protection=false", poolName, volName), req, nil)
	if err != nil {
		return fmt.Errorf("Failed to create volume %q in storage pool %q: %w", volName, poolName, err)
	}

	return nil
}

// deleteVolume deletes an exisiting volume in the given storage pool.
func (p *pureClient) deleteVolume(poolName string, volName string) error {
	req, err := p.createBodyReader(map[string]any{
		"destroyed": true,
	})
	if err != nil {
		return err
	}

	// To destroy the volume, we need to patch it by setting the destroyed to true.
	err = p.requestAuthenticated(http.MethodPatch, fmt.Sprintf("/volumes?names=%s::%s", poolName, volName), req, nil)
	if err != nil {
		return fmt.Errorf("Failed to destroy volume %q in storage pool %q: %w", volName, poolName, err)
	}

	// Afterwards, we can eradicate the volume. If this operation fails, the volume will remain
	// in the destroyed state.
	// TODO: Should we revert it from the destroyed state if eradication fails?
	err = p.requestAuthenticated(http.MethodDelete, fmt.Sprintf("/volumes?names=%s::%s", poolName, volName), nil, nil)
	if err != nil {
		return fmt.Errorf("Failed to delete volume %q in storage pool %q: %w", volName, poolName, err)
	}

	return nil
}

// resizeVolume resizes an existing volume.
func (p *pureClient) resizeVolume(poolName string, volName string, sizeBytes int64) error {
	req, err := p.createBodyReader(map[string]any{
		"provisioned": sizeBytes,
	})
	if err != nil {
		return err
	}

	err = p.requestAuthenticated(http.MethodPatch, fmt.Sprintf("/volumes?names=%s::%s", poolName, volName), req, nil)
	if err != nil {
		return fmt.Errorf("Failed to resize volume %q in storage pool %q: %w", volName, poolName, err)
	}

	return nil
}

// copyVolume copies a source volume into destination volume. If overwrite is set to true,
// the destination volume will be overwritten if it already exists.
func (p *pureClient) copyVolume(srcPoolName string, srcVolName string, dstPoolName string, dstVolName string, overwrite bool) error {
	req, err := p.createBodyReader(map[string]any{
		"source": map[string]string{
			"name": fmt.Sprintf("%s::%s", srcPoolName, srcVolName),
		},
	})
	if err != nil {
		return err
	}

	url := fmt.Sprintf("/volumes?names=%s::%s&overwrite=%v", dstPoolName, dstVolName, overwrite)

	if !overwrite {
		// Disable default protection groups when creating a new volume to avoid potential issues
		// when deleting the volume because protection group may prevent volume eridication.
		url = fmt.Sprintf("%s&with_default_protection=false", url)
	}

	err = p.requestAuthenticated(http.MethodPost, url, req, nil)
	if err != nil {
		return fmt.Errorf(`Failed to copy volume "%s/%s" to "%s/%s": %w`, srcPoolName, srcVolName, dstPoolName, dstVolName, err)
	}

	return nil
}

// getVolumeSnapshots retrieves all existing snapshot for the given storage volume.
func (p *pureClient) getVolumeSnapshots(poolName string, volName string) ([]pureVolume, error) {
	var resp pureResponse[pureVolume]

	err := p.requestAuthenticated(http.MethodGet, fmt.Sprintf("/volume-snapshots?names=%s::%s", poolName, volName), nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("Failed to retrieve snapshots for volume %q in storage pool %q: %w", volName, poolName, err)
	}

	return resp.Items, nil
}

// getVolumeSnapshot retrieves an existing snapshot for the given storage volume.
func (p *pureClient) getVolumeSnapshot(poolName string, volName string, snapshotName string) (*pureVolume, error) {
	var resp pureResponse[pureVolume]

	err := p.requestAuthenticated(http.MethodGet, fmt.Sprintf("/volume-snapshots?names=%s::%s.%s", poolName, volName, snapshotName), nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("Failed to retrieve snapshot %q for volume %q in storage pool %q: %w", snapshotName, volName, poolName, err)
	}

	if len(resp.Items) == 0 {
		return nil, api.StatusErrorf(http.StatusNotFound, "Snapshot %q not found", snapshotName)
	}

	return &resp.Items[0], nil
}

// createVolumeSnapshot creates a new snapshot for the given storage volume.
func (p *pureClient) createVolumeSnapshot(poolName string, volName string, snapshotName string) error {
	req, err := p.createBodyReader(map[string]any{
		"suffix": snapshotName,
	})
	if err != nil {
		return err
	}

	err = p.requestAuthenticated(http.MethodPost, fmt.Sprintf("/volume-snapshots?source_names=%s::%s", poolName, volName), req, nil)
	if err != nil {
		return fmt.Errorf("Failed to create snapshot %q for volume %q in storage pool %q: %w", snapshotName, volName, poolName, err)
	}

	return nil
}

// deleteVolumeSnapshot deletes an existing snapshot for the given storage volume.
func (p *pureClient) deleteVolumeSnapshot(poolName string, volName string, snapshotName string) error {
	req, err := p.createBodyReader(map[string]any{
		"suffix": snapshotName,
	})
	if err != nil {
		return err
	}

	err = p.requestAuthenticated(http.MethodDelete, fmt.Sprintf("/volume-snapshots?source_names=%s::%s.%s", poolName, volName, snapshotName), req, nil)
	if err != nil {
		return fmt.Errorf("Failed to delete snapshot %q for volume %q in storage pool %q: %w", snapshotName, volName, poolName, err)
	}

	return nil
}

// copyVolumeSnapshot copies a volume snapshot into destination volume. If destination volume
// already exists, it is overwritten.
func (p *pureClient) restoreVolumeSnapshot(srcPoolName string, srcVolName string, srcVolumeSnapshots string) error {
	return p.copyVolume(srcPoolName, fmt.Sprintf("%s.%s", srcVolName, srcVolumeSnapshots), srcPoolName, srcVolName, true)
}

// getHosts retrieves an existing PureStorage host.
func (p *pureClient) getHosts() ([]pureHost, error) {
	var resp pureResponse[pureHost]

	err := p.requestAuthenticated(http.MethodGet, fmt.Sprintf("/hosts"), nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("Failed to get hosts: %w", err)
	}

	return resp.Items, nil
}

// getHostByIQN retrieves an existing host that is configured with the given IQN.
func (p *pureClient) getHostByIQN(iqn string) (*pureHost, error) {
	hosts, err := p.getHosts()
	if err != nil {
		return nil, err
	}

	for _, host := range hosts {
		if slices.Contains(host.IQNs, iqn) {
			return &host, nil
		}
	}

	return nil, api.StatusErrorf(http.StatusNotFound, "Host with IQN %q not found", iqn)
}

// getHost retrieves an existing host with the given name.
func (p *pureClient) getHost(hostName string) (*pureHost, error) {
	var resp pureResponse[pureHost]

	err := p.requestAuthenticated(http.MethodGet, fmt.Sprintf("/hosts?names=%s", hostName), nil, &resp)
	if err != nil {
		perr, ok := err.(*pureError)
		if ok && perr.IsNotFoundError() {
			return nil, api.StatusErrorf(http.StatusNotFound, "Host %q not found", hostName)
		}

		return nil, fmt.Errorf("Failed to get host %q: %w", hostName, err)
	}

	if len(resp.Items) == 0 {
		return nil, api.StatusErrorf(http.StatusNotFound, "Host %q not found", hostName)
	}

	return &resp.Items[0], nil
}

// createHost creates a new host that can be associated with specific volumes.
func (p *pureClient) createHost(hostName string, iqns []string) error {
	req, err := p.createBodyReader(map[string]any{
		"iqns": iqns,
	})
	if err != nil {
		return err
	}

	err = p.requestAuthenticated(http.MethodPost, fmt.Sprintf("/hosts?names=%s", hostName), req, nil)
	if err != nil {
		perr, ok := err.(*pureError)
		if ok && perr.Is(http.StatusBadRequest, "Host already exists.") {
			return api.StatusErrorf(http.StatusConflict, "Host %q already exists", hostName)
		}

		return fmt.Errorf("Failed to create host %q: %w", hostName, err)
	}

	return nil
}

// updateHost updates an existing host.
func (p *pureClient) updateHost(hostName string, iqns []string) error {
	req, err := p.createBodyReader(map[string]any{
		"iqns": iqns,
	})
	if err != nil {
		return err
	}

	// To destroy the volume, we need to patch it by setting the destroyed to true.
	err = p.requestAuthenticated(http.MethodPatch, fmt.Sprintf("/hosts?names=%s", hostName), req, nil)
	if err != nil {
		return fmt.Errorf("Failed to update host %q: %w", hostName, err)
	}

	return nil
}

// connectHostToVolume creates a connection beween a host and volume. It returns true if the connection
// was created, and false if it already existed.
func (p *pureClient) connectHostToVolume(poolName string, volName string, hostName string) (bool, error) {
	err := p.requestAuthenticated(http.MethodPost, fmt.Sprintf("/connections?host_names=%s&volume_names=%s::%s", hostName, poolName, volName), nil, nil)
	if err != nil {
		perr, ok := err.(*pureError)
		if ok && perr.Is(http.StatusBadRequest, "Connection already exists.") {
			// Do not error out if connection already exists.
			return false, nil
		}

		return false, fmt.Errorf("Failed to connect volume %q with host %q: %w", volName, hostName, err)
	}

	return true, nil
}

// disconnectHostFromVolume deletes a connection beween a host and volume.
func (p *pureClient) disconnectHostFromVolume(poolName string, volName string, hostName string) error {
	err := p.requestAuthenticated(http.MethodDelete, fmt.Sprintf("/connections?host_names=%s&volume_names=%s::%s", hostName, poolName, volName), nil, nil)
	if err != nil {
		perr, ok := err.(*pureError)
		if ok && perr.IsNotFoundError() {
			return api.StatusErrorf(http.StatusNotFound, "Connection between host %q and volume %q not found", volName, hostName)
		}

		return fmt.Errorf("Failed to disconnect volume %q from host %q: %w", volName, hostName, err)
	}

	return nil
}

// deleteHost deletes an existing host.
func (p *pureClient) deleteHost(hostName string) error {
	err := p.requestAuthenticated(http.MethodDelete, fmt.Sprintf("/hosts?names=%s", hostName), nil, nil)
	if err != nil {
		return fmt.Errorf("Failed to delete host %q: %w", hostName, err)
	}

	return nil
}

// getISCSITarget retrieves the target that can be connected to using the provided initiator IQN.
func (p *pureClient) getISCSITarget() (*pureTarget, error) {
	var resp pureResponse[pureTarget]

	portal := p.driver.config["pure.iscsi.address"]

	// Check if the port is already set for the address.
	addrAndPort := strings.Split(portal, ":")
	if len(addrAndPort) == 1 {
		// Search on default iSCSI port 3260.
		portal = fmt.Sprintf("%s:3260", portal)
	} else if len(addrAndPort) > 2 {
		// Incorrect address.
		return nil, fmt.Errorf("Invalid iSCSI address %q", portal)
	}

	err := p.requestAuthenticated(http.MethodGet, fmt.Sprintf("/ports?filter=portal='%s'", portal), nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("Failed to retrieve iSCSI targets for address %q: %w", portal, err)
	}

	if len(resp.Items) == 0 {
		return nil, api.StatusErrorf(http.StatusNotFound, "No iSCSI target found on address %q", portal)
	}

	return &resp.Items[0], nil

}

// ensureISCSIHost returns a name of the host that is configured with a given IQN. If such host
// does not exist, a new one is created.
func (d *pure) ensureISCSIHost() (hostName string, cleanup revert.Hook, err error) {
	var hostname string

	revert := revert.New()
	defer revert.Fail()

	iqn, err := d.hostIQN()
	if err != nil {
		return "", nil, err
	}

	// Fetch the host by IQN.
	host, err := d.client().getHostByIQN(iqn)
	if err != nil {
		if !api.StatusErrorCheck(err, http.StatusNotFound) {
			return "", nil, err
		}

		// The host with a given IQN does not exist, therefore, create a new one.
		hostname, err = d.serverName()
		if err != nil {
			return "", nil, err
		}

		err = d.client().createHost(hostname, []string{iqn})
		if err != nil {
			if !api.StatusErrorCheck(err, http.StatusConflict) {
				return "", nil, err
			}

			// The host with the given name already exists, update it instead.
			err = d.client().updateHost(hostname, []string{iqn})
			if err != nil {
				return "", nil, err
			}
		} else {
			revert.Add(func() { _ = d.client().deleteHost(hostname) })
		}
	} else {
		hostname = host.Name
	}

	logger.Warn("Ensured iSCSI host exists", logger.Ctx{"hostname": hostname, "iqn": iqn})

	cleanup = revert.Clone().Fail
	revert.Success()
	return hostname, cleanup, nil
}

func iscsiHasConnection(iqn string) (bool, error) {
	// Base path for iSCSI sessions.
	basePath := "/sys/class/iscsi_session"

	// Retrieve list of existing iSCSI sessions.
	sessions, err := os.ReadDir(basePath)
	if err != nil {
		return false, fmt.Errorf("Failed getting a list of existing iSCSI sessions: %w", err)
	}

	for _, session := range sessions {
		// Get the target IQN of the iSCSI session.
		iqnBytes, err := os.ReadFile(filepath.Join(basePath, session.Name(), "targetname"))
		if err != nil {
			return false, fmt.Errorf("Failed getting the target IQN for session %q: %w", session, err)
		}

		sessionIQN := strings.TrimSpace(string(iqnBytes))

		logger.Warn("Found session", logger.Ctx{"session": session.Name(), "iqn": sessionIQN})

		if iqn == sessionIQN {
			// Already connected to the PureStorage array via iSCSI.
			return true, nil
		}
	}

	return false, nil
}

// iscsiConnect connects this host to the iSCSI subsystem configured in the storage pool.
// The connection can only be established after the first volume is mapped to this host.
// The operation is idempotent and returns nil if already connected to the subsystem.
func (d *pure) iscsiConnect() (revert.Hook, error) {
	revert := revert.New()
	defer revert.Fail()

	// Find the array's IQN if connecting for the first time.
	if d.iscsiTarget == nil {
		target, err := d.client().getISCSITarget()
		if err != nil {
			return nil, err
		}

		d.iscsiTarget = target
	}

	// Base path for iSCSI sessions.
	basePath := "/sys/class/iscsi_session"

	// Retrieve list of existing iSCSI sessions.
	sessions, err := os.ReadDir(basePath)
	if err != nil {
		return nil, fmt.Errorf("Failed getting a list of existing iSCSI sessions: %w", err)
	}

	// TODO: Temporary use just the first target.
	targetIQN := *d.iscsiTarget.IQN

	for _, session := range sessions {
		// Get the target IQN of the iSCSI session.
		iqnBytes, err := os.ReadFile(filepath.Join(basePath, session.Name(), "targetname"))
		if err != nil {
			return nil, fmt.Errorf("Failed getting the target IQN for session %q: %w", session, err)
		}

		sessionIQN := strings.TrimSpace(string(iqnBytes))
		sessionID := strings.TrimPrefix(session.Name(), "session")

		logger.Warn("Found session", logger.Ctx{"session": session.Name(), "id": sessionID, "iqn": sessionIQN})

		if targetIQN == sessionIQN {
			// Already connected to the PureStorage array via iSCSI.
			// Rescan the session to ensure new volumes are detected.
			_, err := shared.RunCommand("iscsiadm", "--mode", "session", "--sid", sessionID, "--rescan")
			if err != nil {
				return nil, err
			}

			cleanup := revert.Clone().Fail
			revert.Success()
			return cleanup, nil
		}
	}

	// Check if the host is already connected to the PureStorage array via iSCSI.
	// hasConn, err := iscsiHasConnection(d.targetIQN)
	// if err != nil {
	// 	// Rescan the session to ensure new volumes are detected.
	// 	_, err := shared.RunCommand("iscsiadm", "--mode", "session", "--sid", "--rescan")

	// 	return nil, err
	// }

	// if hasConn {
	// 	// Already connected to the PureStorage array via iSCSI.
	// 	cleanup := revert.Clone().Fail
	// 	revert.Success()
	// 	return cleanup, nil
	// }

	iscsiAddr := d.config["pure.iscsi.address"]

	// Attempt to login into discovered iSCSI targets.
	stdout, stderr, err := shared.RunCommandSplit(d.state.ShutdownCtx, nil, nil, "iscsiadm", "--mode", "node", "--targetname", targetIQN, "--portal", iscsiAddr, "--login", "--debug=8")
	if err != nil {
		logger.Warn("Output", logger.Ctx{"stdout": stdout, "error": err})
		return nil, fmt.Errorf("Failed to connect to PureStorage host %q via iSCSI: %s\n%v", iscsiAddr, stderr, err)
	}

	revert.Add(func() { _ = d.iscsiDisconnect(targetIQN) })

	cleanup := revert.Clone().Fail
	revert.Success()
	return cleanup, nil
}

// iscsiDisconnect disconnects this host from the given iSCSI target.
func (d *pure) iscsiDisconnect(targetIQN string) error {
	hasConn, err := iscsiHasConnection(targetIQN)
	if err != nil {
		return err
	}

	if hasConn {
		// Disconnect from the iSCSI target.
		_, err := shared.RunCommand("iscsiadm", "--mode", "node", "--targetname", targetIQN, "--logout")
		if err != nil {
			return fmt.Errorf("Failed disconnecting from PureStorage iSCSI target %q: %w", targetIQN, err)
		}
	}

	return nil
}

// iscsiDisconnect disconnects this host from all iSCSI sessions.
func (d *pure) iscsiDisconnectAll() error {
	_, err := shared.RunCommand("iscsiadm", "--mode", "session", "--logout")
	if err != nil {
		return fmt.Errorf("Failed disconnecting from iSCSI sessions: %w", err)
	}

	return nil
}

// mapVolume maps the given volume onto this host.
func (d *pure) mapVolume(vol Volume) (revert.Hook, error) {
	revert := revert.New()
	defer revert.Fail()

	volName, err := d.getVolumeName(vol)
	if err != nil {
		return nil, err
	}

	var hostname string

	switch d.config["pure.mode"] {
	case pureModeISCSI:
		unlock, err := locking.Lock(d.state.ShutdownCtx, "iscsi")
		if err != nil {
			return nil, err
		}

		defer unlock()

		// Ensure the host exists and is configured with the correct IQN.
		hostName, cleanup, err := d.ensureISCSIHost()
		if err != nil {
			return nil, err
		}

		hostname = hostName
		revert.Add(cleanup)
	}

	client := d.client()

	// Ensure the volume is connected to the host.
	connCreated, err := client.connectHostToVolume(vol.pool, volName, hostname)
	if err != nil {
		return nil, err
	}

	if connCreated {
		revert.Add(func() { _ = client.disconnectHostFromVolume(vol.pool, volName, hostname) })
	}

	if d.config["pure.mode"] == pureModeISCSI {
		// Connect to the array using iscsi. Connection can be established only
		// when at least one volume is connected to the host.
		cleanup, err := d.iscsiConnect()
		if err != nil {
			return nil, err
		}

		revert.Add(cleanup)
	}

	cleanup := revert.Clone().Fail
	revert.Success()
	return cleanup, nil
}

// unmapVolume unmaps the given volume from this host.
func (d *pure) unmapVolume(vol Volume) error {
	var host *pureHost
	var iqn string
	var err error

	volName, err := d.getVolumeName(vol)
	if err != nil {
		return err
	}

	switch d.config["pure.mode"] {
	case pureModeISCSI:
		iqn, err = d.hostIQN()
		if err != nil {
			return err
		}

		host, err = d.client().getHostByIQN(iqn)
		if err != nil {
			return err
		}

		unlock, err := locking.Lock(d.state.ShutdownCtx, "iscsi")
		if err != nil {
			return err
		}

		defer unlock()
	}

	// Disconnect the volume from the host and ignore error if connection does not exist.
	err = d.client().disconnectHostFromVolume(vol.pool, volName, host.Name)
	if err != nil && !api.StatusErrorCheck(err, http.StatusNotFound) {
		return err
	}

	// Wait until the volume has disappeared.
	volumePath, _, _ := d.getMappedDevPath(vol, false)
	if volumePath != "" {
		if d.config["pure.mode"] == pureModeISCSI {
			// TODO: This is more like a workaround, but I cannot find a better way to remove the device, since
			// iSCSI does not do that when volume is disconnected from the host.
			//
			// When volume is disconnected from the host, the device will remain on the system.
			//
			// To remove the device, we need to either logout from the iSCSI session or remove the device manually.
			// Logging out of the session is not desired as it would disconnect all the volumes from the array.
			// Therefore, we need to manually remove the device.
			split := strings.Split(filepath.Base(volumePath), "/")
			devName := split[len(split)-1]

			path := fmt.Sprintf("/sys/block/%s/device/delete", devName)
			if shared.PathExists(path) {
				err := os.WriteFile(path, []byte("1"), 0400)
				if err != nil {
					return fmt.Errorf("Failed to unmap volume %q: Failed to remove device %q: %w", vol.name, devName, err)
				}
			}
		}

		ctx, cancel := context.WithTimeout(d.state.ShutdownCtx, 10*time.Second)
		defer cancel()

		if !waitGone(ctx, volumePath) {
			return fmt.Errorf("Timeout exceeded waiting for PureStorage volume %q to disappear on path %q", vol.name, volumePath)
		}
	}

	if d.config["pure.mode"] == pureModeISCSI {
		// If this was the last volume being unmapped from this system, terminate iSCSI session
		// and remove the host from PureStorage.
		if host.ConnectionCount == 1 {
			err := d.iscsiDisconnect(*d.iscsiTarget.IQN)
			if err != nil {
				return err
			}

			// Remove cached iSCSI target.
			d.iscsiTarget = nil

			err = d.client().deleteHost(host.Name)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// getMappedDevPath returns the local device path for the given volume.
// Indicate with mapVolume if the volume should get mapped to the system if it isn't present.
func (d *pure) getMappedDevPath(vol Volume, mapVolume bool) (string, revert.Hook, error) {
	revert := revert.New()
	defer revert.Fail()

	defer func() {
		// Show mapped volumes from iSCSI:
		out, err := shared.RunCommandContext(d.state.ShutdownCtx, "iscsiadm", "--mode", "session", "--print", "3" /* 3 = print level */)
		logger.Debug("Active iSCSI session", logger.Ctx{"out": out, "error": err})
	}()

	if mapVolume {
		cleanup, err := d.mapVolume(vol)
		if err != nil {
			return "", nil, err
		}

		revert.Add(cleanup)
	}

	// findDevPathFunc has to be called in a loop with a set timeout to ensure
	// all the necessary directories and devices can be discovered.
	findDevPathFunc := func(diskPrefix string, volumeName string) (string, error) {
		var diskPaths []string

		// If there are no other disks on the system by id, the directory might not even be there.
		// Returns ENOENT in case the by-id/ directory does not exist.
		diskPaths, err := resources.GetDisksByID(diskPrefix)
		if err != nil {
			return "", err
		}

		for _, diskPath := range diskPaths {
			// Skip the disk if it is only a partition of the actual volume.
			if strings.Contains(diskPath, "-part") {
				continue
			}

			// Skip volumes that do not have volume's pool and name suffix.
			if !strings.HasSuffix(diskPath, volumeName) {
				continue
			}

			// The actual device might not already be created.
			// Returns ENOENT in case the device does not exist.
			devPath, err := filepath.EvalSymlinks(diskPath)
			if err != nil {
				return "", err
			}

			return devPath, nil
		}

		return "", nil
	}

	volName, err := d.getVolumeName(vol)
	if err != nil {
		return "", nil, err
	}

	volumeFullName := fmt.Sprintf("%s::%s", vol.pool, volName)
	timeout := time.Now().Add(60 * time.Second) // TODO: Adjust to 5 or 10 seconds!
	logger.Warn("Looking for device", logger.Ctx{"volume": volumeFullName, "timeout": "30s"})

	var volumeDevPath string

	// It might take a while to create the local disk.
	// Retry until it can be found.
	for {
		if time.Now().After(timeout) {
			diskPaths, _ := resources.GetDisksByID("scsi-")
			// TODO: Remove second part of the error (debug purpose).
			return "", nil, fmt.Errorf("Failed to locate device for volume %q: Timeout exceeded\nList of devices: \n%s", vol.name, strings.Join(diskPaths, "\n"))
		}

		var diskPrefix string
		switch d.config["pure.mode"] {
		case pureModeISCSI:
			diskPrefix = "scsi-"
		}

		devPath, err := findDevPathFunc(diskPrefix, volumeFullName)
		if err != nil {
			// Try again if on of the directories cannot be found.
			if errors.Is(err, unix.ENOENT) {
				continue
			}

			return "", nil, err
		}

		if devPath != "" {
			volumeDevPath = devPath
			break
		}

		// Exit if the volume wasn't explicitly mapped.
		// Doing a retry would run into the timeout when the device isn't mapped.
		if !mapVolume {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	if volumeDevPath == "" {
		return "", nil, fmt.Errorf("Failed to locate device for volume %q", vol.name)
	}

	logger.Warn("Located device for volume", logger.Ctx{"pool": vol.pool, "volume": vol.name, "device": volumeDevPath})

	cleanup := revert.Clone().Fail
	revert.Success()
	return volumeDevPath, cleanup, nil
}

// serverName returns the hostname of this host. It prefers the value from the daemons state
// in case LXD is clustered.
func (d *pure) serverName() (string, error) {
	if d.state.ServerName != "none" {
		return d.state.ServerName, nil
	}

	hostname, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("Failed to get hostname: %w", err)
	}

	return hostname, nil
}

// hostIQN returns the unique iSCSI Qualified Name (IQN) of the host. A custom one is generated
// from the servers UUID since getting the IQN from /etc/iscsi/initiatorname.iscsi would require
// the iscsiadm to be installed on the host.
func (d *pure) hostIQN() (string, error) {
	// filename := "/etc/iscsi/initiatorname.iscsi"
	filename := shared.HostPath("/etc/iscsi/initiatorname.iscsi")
	// filename := shared.VarPath("/iscsi/initiatorname.iscsi")

	if !shared.PathExists(filename) {
		// Ensure parent directories exist.
		err := os.MkdirAll(filepath.Dir(filename), 0755)
		if err != nil {
			return "", err
		}

		iqn := fmt.Sprintf("iqn.1996-07.com.example:01:%s", d.state.ServerUUID)

		// Create initiatorname.iscsi file with the generated IQN.
		err = os.WriteFile(filename, []byte("InitiatorName="+iqn), 0600)
		if err != nil {
			return "", err
		}

		return iqn, nil
	}

	// Read the existing IQN from the file.
	content, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}

	// Find the IQN line in the file.
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		iqn, ok := strings.CutPrefix(line, "InitiatorName=")
		if ok {
			return iqn, nil
		}
	}

	return "", fmt.Errorf("Failed to extract host IQN from %q", filename)
}

// getVolumeName returns the fully qualified name derived from the volume.
func (d *pure) getVolumeName(vol Volume) (string, error) {
	volUUID, err := uuid.Parse(vol.config["volatile.uuid"])
	if err != nil {
		return "", fmt.Errorf(`Failed parsing "volatile.uuid" from volume %q: %w`, vol.name, err)
	}

	binUUID, err := volUUID.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf(`Failed marshalling the "volatile.uuid" of volume %q to binary format: %w`, vol.name, err)
	}

	// The volume's name in base64 encoded format. PureStorage volume name cannot contain some
	// base64 characters, such as equal sign (=), plus (+), and slash (/). Therefore, use
	volName := base64.RawURLEncoding.EncodeToString(binUUID)

	// TODO: Snapshots do not support underscores in their names.
	volName = strings.ReplaceAll(volName, "_", "-")

	// Search for the volume type prefix, and if found, prepend it to the volume name.
	volumeTypePrefix, ok := pureVolTypePrefixes[vol.volType]
	if ok {
		// TODO: Dash/hyphen used instead of underscore.
		volName = fmt.Sprintf("%s-%s", volumeTypePrefix, volName)
	}

	// Search for the content type suffix, and if found, append it to the volume name.
	contentTypeSuffix, ok := pureContentTypeSuffixes[vol.contentType]
	if ok {
		// TODO: Dash/hyphen used instead of dot.
		volName = fmt.Sprintf("%s-%s", volName, contentTypeSuffix)
	}

	logger.Error("Volume name", logger.Ctx{"volName": volName, "volType": vol.volType, "contentType": vol.contentType})
	return volName, nil
}
