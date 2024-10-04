package drivers

import (
	"bytes"
	"context"
	"crypto/tls"
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

// TODO: This should be unified with PowerFlex for consistency (most likely, some of the new drivers will rely on that as well).
//
// pureVolTypePrefixes maps volume type to storage volume name prefix.
// Use smallest possible prefixes since PowerFlex volume names are limited to 31 characters.
var pureVolTypePrefixes = map[VolumeType]string{
	VolumeTypeContainer: "c",
	VolumeTypeVM:        "v",
	VolumeTypeImage:     "i",
	VolumeTypeCustom:    "u",
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

// pureID represents a generic ID response from the PureStorage API.
type pureID struct {
	ID string `json:"id"`
}

// pureHost represents a host in PureStorage.
type pureHost struct {
	Name            string   `json:"name"`
	IQNs            []string `json:"iqns"`
	ConnectionCount int      `json:"connection_count"`
}

// pureProtectionDomain represents a protection domain in PureStorage.
type pureProtectionDomain struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	SystemID string `json:"systemId"`
}

// pureStoragePool represents a storage pool (Pod) in PureStorage.
type pureStoragePool struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Destroyed bool   `json:"destroyed"`
	// ProtectionDomainID string `json:"protectionDomainId"`
}

// pureProtectionDomainStoragePool represents a relation between a storage pool and a protection domain
// in PureStorage.
type pureProtectionDomainStoragePool struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// pureVolume represents a volume in PureStorage.
type pureVolume struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	VolumeType string `json:"volumeType"` // volume/snapshot
	// VTreeID          string `json:"vtreeId"`
	// AncestorVolumeID string `json:"ancestorVolumeId"`
	// MappedSDCInfo    []struct {
	// 	SDCID    string `json:"sdcId"`
	// 	SDCName  string `json:"sdcName"`
	// 	NQN      string `json:"nqn"`
	// 	HostType string `json:"hostType"`
	// } `json:"mappedSdcInfo"`
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

// getVolumeID returns the volume ID for the given name.
// func (p *pureClient) getVolumeID(name string) (string, error) {
// 	body, err := p.createBodyReader(map[string]any{
// 		"name": name,
// 	})
// 	if err != nil {
// 		return "", err
// 	}

// 	var actualResponse string
// 	err = p.requestAuthenticated(http.MethodPost, "/api/types/Volume/instances/action/queryIdByKey", body, &actualResponse)
// 	if err != nil {
// 		powerFlexError, ok := err.(*powerFlexError)
// 		if ok {
// 			// API returns 500 if the volume does not exist.
// 			// To not confuse it with other 500 that might occur check the error code too.
// 			if powerFlexError.HTTPStatusCode() == http.StatusInternalServerError && powerFlexError.ErrorCode() == powerFlexCodeVolumeNotFound {
// 				return "", api.StatusErrorf(http.StatusNotFound, "PowerFlex volume not found: %q", name)
// 			}
// 		}

// 		return "", fmt.Errorf("Failed to get volume ID: %q: %w", name, err)
// 	}

// 	return actualResponse, nil
// }

// getStoragePool returns the storage pool behind poolID.
func (p *pureClient) getStoragePool(poolName string) (*pureStoragePool, error) {
	var actualResponse pureStoragePool
	err := p.requestAuthenticated(http.MethodGet, fmt.Sprintf("/pods?names=%s", poolName), nil, &actualResponse)
	if err != nil {
		perr, ok := err.(*pureError)
		if ok && perr.IsNotFoundError() {
			return nil, api.StatusErrorf(http.StatusNotFound, "Storage pool %q not found", poolName)
		}

		return nil, fmt.Errorf("Failed to get storage pool %q: %w", poolName, err)
	}

	return &actualResponse, nil
}

// createStoragePool creates a storage pool (PureStorage Pod) and returns it's ID.
func (p *pureClient) createStoragePool(poolName string) (string, error) {
	var resp pureResponse[pureID]

	err := p.requestAuthenticated(http.MethodPost, fmt.Sprintf("/pods?names=%s", poolName), nil, &resp)
	if err != nil {
		return "", fmt.Errorf("Failed to create storage pool %q: %w", poolName, err)
	}

	if len(resp.Items) == 0 || resp.Items[0].ID == "" {
		return "", fmt.Errorf(`Failed to create storage pool %q: Response does not contain field "id"`, poolName)
	}

	return resp.Items[0].ID, nil
}

// deleteStoragePool deletes a storage pool (PureStorage Pod).
func (p *pureClient) deleteStoragePool(poolName string) error {
	req, err := p.createBodyReader(map[string]any{
		"destroyed": true,
	})
	if err != nil {
		return err
	}

	// To destroy the storage pool, we need to first destroy it by setting the destroyed to true.
	// In addition, destroy all of its contents to allow the pool to be deleted.
	err = p.requestAuthenticated(http.MethodPatch, fmt.Sprintf("/pods?names=%s&destroy_contents=true", poolName), req, nil)
	if err != nil {
		perr, ok := err.(*pureError)
		if ok && perr.IsNotFoundError() {
			return api.StatusErrorf(http.StatusNotFound, "Storage pool %q not found", poolName)
		}

		return fmt.Errorf("Failed to destroy storage pool %q: %w", poolName, err)
	}

	// Afterwards, the storage pool and all of its contents can be deleted (eradicated).
	err = p.requestAuthenticated(http.MethodDelete, fmt.Sprintf("/pods?names=%s&eradicate_contents=true", poolName), nil, nil)
	if err != nil {
		perr, ok := err.(*pureError)
		if ok && perr.IsNotFoundError() {
			return api.StatusErrorf(http.StatusNotFound, "Storage pool %q not found", poolName)
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

	// TODO: Is this check required
	if len(resp.Items) == 0 {
		return nil, api.StatusErrorf(http.StatusNotFound, "Volume %q not found", volName)
	}

	return &resp.Items[0], nil
}

// createVolume creates a new volume in the given storage pool. The volume is created with supplied size in bytes.
// Upon successful creation, volume's ID is returned.
func (p *pureClient) createVolume(poolName string, volName string, sizeBytes int64) (string, error) {
	req, err := p.createBodyReader(map[string]any{
		"provisioned": sizeBytes,
	})
	if err != nil {
		return "", err
	}

	var resp pureResponse[pureID]

	err = p.requestAuthenticated(http.MethodPost, fmt.Sprintf("/volumes?names=%s::%s", poolName, volName), req, &resp)
	if err != nil {
		return "", fmt.Errorf("Failed to create volume %q in storage pool %q: %w", volName, poolName, err)
	}

	if len(resp.Items) == 0 {
		return "", fmt.Errorf("Failed to create volume %q in storage pool %q: Volume ID not found", volName, poolName)
	}

	return resp.Items[0].ID, nil
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

// iscsiDiscover returns PureStorage array IQN discovered on the provided iscsi host.
func (d *pure) iscsiDiscover() (string, error) {
	iscsiAddr := d.config["pure.iscsi.address"]

	// Ensure the iSCSI directory exists.
	// TODO: This is temporary workaround.
	err := os.MkdirAll("/run/lock/iscsi", 0755)
	if err != nil {
		return "", err
	}

	out, err := shared.RunCommand("iscsiadm", "--mode", "discovery", "--type", "sendtargets", "--portal", iscsiAddr)
	if err != nil {
		return "", fmt.Errorf("Failed to discover any iSCSI target on address %q: %w", iscsiAddr, err)
	}

	lines := strings.Split(out, "\n")
	iqns := make([]string, 0, len(lines))

	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		iqn := fields[1]
		if !slices.Contains(iqns, iqn) {
			iqns = append(iqns, iqn)
		}
	}

	if len(iqns) == 0 {
		return "", fmt.Errorf("Failed to discover any iSCSI target on address %q", iscsiAddr)
	}

	// TODO: Temporary return just a single IQN.
	return iqns[0], nil
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
	if d.targetIQN == "" {
		targetIQN, err := d.iscsiDiscover()
		if err != nil {
			return nil, err
		}

		d.targetIQN = targetIQN
	}

	// Base path for iSCSI sessions.
	basePath := "/sys/class/iscsi_session"

	// Retrieve list of existing iSCSI sessions.
	sessions, err := os.ReadDir(basePath)
	if err != nil {
		return nil, fmt.Errorf("Failed getting a list of existing iSCSI sessions: %w", err)
	}

	for _, session := range sessions {
		// Get the target IQN of the iSCSI session.
		iqnBytes, err := os.ReadFile(filepath.Join(basePath, session.Name(), "targetname"))
		if err != nil {
			return nil, fmt.Errorf("Failed getting the target IQN for session %q: %w", session, err)
		}

		sessionIQN := strings.TrimSpace(string(iqnBytes))
		sessionID := strings.TrimPrefix(session.Name(), "session")

		logger.Warn("Found session", logger.Ctx{"session": session.Name(), "id": sessionID, "iqn": sessionIQN})

		if d.targetIQN == sessionIQN {
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
	stdout, stderr, err := shared.RunCommandSplit(d.state.ShutdownCtx, nil, nil, "iscsiadm", "--mode", "node", "--targetname", d.targetIQN, "--portal", iscsiAddr, "--login", "--debug=8")
	if err != nil {
		logger.Warn("Output", logger.Ctx{"stdout": stdout, "error": err})
		return nil, fmt.Errorf("Failed to connect to PureStorage host %q via iSCSI: %s\n%v", iscsiAddr, stderr, err)
	}

	revert.Add(func() { _ = d.iscsiDisconnect(d.targetIQN) })

	cleanup := revert.Clone().Fail
	revert.Success()
	return cleanup, nil
}

// iscsiDisconnect disconnects this host from the given iSCSI target.
func (d *pure) iscsiDisconnect(iqn string) error {
	hasConn, err := iscsiHasConnection(d.targetIQN)
	if err != nil {
		return err
	}

	if hasConn {
		// Disconnect from the iSCSI target.
		_, err := shared.RunCommand("iscsiadm", "--mode", "node", "--targetname", iqn, "--logout")
		if err != nil {
			return fmt.Errorf("Failed disconnecting from PureStorage iSCSI target %q: %w", iqn, err)
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
	connCreated, err := client.connectHostToVolume(vol.pool, vol.name, hostname)
	if err != nil {
		return nil, err
	}

	if connCreated {
		revert.Add(func() { _ = client.disconnectHostFromVolume(vol.pool, vol.name, hostname) })
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
	err = d.client().disconnectHostFromVolume(vol.pool, vol.name, host.Name)
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
			err := d.iscsiDisconnect(d.targetIQN)
			if err != nil {
				return err
			}

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

	volumeFullName := fmt.Sprintf("%s::%s", vol.pool, vol.name)
	timeout := time.Now().Add(30 * time.Second) // TODO: Adjust to 5 or 10 seconds!
	logger.Warn("Looking for device", logger.Ctx{"volume": volumeFullName, "timeout": "30s"})

	var volumeDevPath string

	// It might take a while to create the local disk.
	// Retry until it can be found.
	for {
		if time.Now().After(timeout) {
			diskPaths, _ := resources.GetDisksByID("scsi-")
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

		time.Sleep(100 * time.Millisecond)
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
		if strings.HasPrefix(line, "InitiatorName=") {
			return strings.TrimPrefix(line, "InitiatorName="), nil
		}
	}

	return "", fmt.Errorf("Failed to extract host IQN from %q", filename)
}

// // resolvePool looks up the selected storage pool.
// // If only the pool is provided, it's expected to be the ID of the pool.
// // In case both pool and domain are set, the pool will get looked up
// // by name within the domain.
// func (d *pure) resolvePool() (*powerFlexStoragePool, error) {
// 	return nil, nil
// }

// // getPowerFlexVolumeName returns the fully qualified name derived from the volume.
// func (d *pure) getVolumeName(vol Volume) (string, error) {
// 	volUUID, err := uuid.Parse(vol.config["volatile.uuid"])
// 	if err != nil {
// 		return "", fmt.Errorf(`Failed parsing "volatile.uuid" from volume %q: %w`, vol.name, err)
// 	}

// 	binUUID, err := volUUID.MarshalBinary()
// 	if err != nil {
// 		return "", fmt.Errorf(`Failed marshalling the "volatile.uuid" of volume %q to binary format: %w`, vol.name, err)
// 	}

// 	// The volume's name in base64 encoded format.
// 	volName := base64.StdEncoding.EncodeToString(binUUID)

// 	var suffix string
// 	if vol.contentType == ContentTypeBlock {
// 		suffix = powerFlexBlockVolSuffix
// 	} else if vol.contentType == ContentTypeISO {
// 		suffix = powerFlexISOVolSuffix
// 	}

// 	// Use storage volume prefix from powerFlexVolTypePrefixes depending on type.
// 	// If the volume's type is unknown, don't put any prefix to accommodate the volume name size constraint.
// 	volumeTypePrefix, ok := powerFlexVolTypePrefixes[vol.volType]
// 	if ok {
// 		volumeTypePrefix = fmt.Sprintf("%s_", volumeTypePrefix)
// 	}

// 	return fmt.Sprintf("%s%s%s", volumeTypePrefix, volName, suffix), nil
// }
