package drivers

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
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

// pureAPIVersion is the Pure Storage API version used by LXD.
// The 2.21 version is the first version that supports NVMe/TCP.
const pureAPIVersion = "2.21"

// pureServiceNameMapping maps Pure Storage mode in LXD to the corresponding Pure Storage
// service name.
var pureServiceNameMapping = map[string]string{
	pureModeISCSI: "iscsi",
}

// pureVolTypePrefixes maps volume type to storage volume name prefix.
// Use smallest possible prefixes since Pure Storage volume names are limited to 63 characters.
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

// pureSnapshotPrefix is a prefix used for Pure Storage snapshots to avoid name conflicts
// when creating temporary volume from the snapshot.
var pureSnapshotPrefix = "s"

// pureError represents an error responses from Pure Storage API.
type pureError struct {
	// List of errors returned by the Pure Storage API.
	Errors []struct {
		Context string `json:"context"`
		Message string `json:"message"`
	} `json:"errors"`

	// StatusCode is not part of the response body but is used
	// to store the HTTP status code.
	StatusCode int `json:"-"`
}

// Error returns the first error message from the Pure Storage API error.
func (p *pureError) Error() string {
	if p == nil || len(p.Errors) == 0 {
		return ""
	}

	// Return the first error message without the trailing dot.
	return strings.TrimSuffix(p.Errors[0].Message, ".")
}

// isPureErrorOf checks if the given error is of type pureError, has the specified status code,
// and its error messages contain any of the provided substrings. Note that the error message
// comparison is case-insensitive.
func isPureErrorOf(err error, statusCode int, substrings ...string) bool {
	perr, ok := err.(*pureError)
	if !ok {
		return false
	}

	if perr.StatusCode != statusCode {
		return false
	}

	if len(substrings) == 0 {
		// Error matches the given status code and no substrings are provided.
		return true
	}

	// Check if any error message contains a provided substring.
	// Perform case-insensitive matching by converting both the
	// error message and the substring to lowercase.
	for _, err := range perr.Errors {
		errMsg := strings.ToLower(err.Message)

		for _, substring := range substrings {
			if strings.Contains(errMsg, strings.ToLower(substring)) {
				return true
			}
		}
	}

	return false
}

// pureIsNotFoundError returns true if the error is of type pureError, its status code is 400 (bad request),
// and the error message contains a substring indicating the resource was not found.
func isPureErrorNotFound(err error) bool {
	return isPureErrorOf(err, http.StatusBadRequest, "Not found", "Does not exist", "No such volume or snapshot")
}

// pureResponse wraps the response from the Pure Storage API. In most cases, the response
// contains a list of items, even if only one item is returned.
type pureResponse[T any] struct {
	Items []T `json:"items"`
}

// purePort represents a network interface in Pure Storage.
type pureNetworkInterface struct {
	Name     string `json:"name"`
	Ethernet struct {
		Address string `json:"address,omitempty"`
	} `json:"eth,omitempty"`
}

// pureStoragePool represents a storage pool (pod) in Pure Storage.
type pureStoragePool struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	IsDestroyed bool   `json:"destroyed"`
}

// pureVolume represents a volume in Pure Storage.
type pureVolume struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	IsDestroyed bool   `json:"destroyed"`
}

// pureHost represents a host in Pure Storage.
type pureHost struct {
	Name            string   `json:"name"`
	IQNs            []string `json:"iqns"`
	ConnectionCount int      `json:"connection_count"`
}

// purePort represents a port in Pure Storage.
type purePort struct {
	Name string `json:"name"`
	IQN  string `json:"iqn,omitempty"`
	NQN  string `json:"nqn,omitempty"`
}

// pureClient holds the Pure Storage HTTP client and an access token.
type pureClient struct {
	driver      *pure
	accessToken string
}

// newPureClient creates a new instance of the HTTP Pure Storage client.
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

// request issues a HTTP request against the Pure Storage gateway.
func (p *pureClient) request(method string, url url.URL, reqBody io.Reader, reqHeaders map[string]string, respBody any, respHeaders map[string]string) error {
	// Extract scheme and host from the gateway URL.
	scheme, host, found := strings.Cut(p.driver.config["pure.gateway"], "://")
	if !found {
		return fmt.Errorf("Invalid Pure Storage gateway URL: %q", p.driver.config["pure.gateway"])
	}

	// Set request URL scheme and host.
	url.Scheme = scheme
	url.Host = host

	// Prefixes the given path with the API version in the format "/api/<version>/<path>".
	// If the path is "/api/api_version", the API version is not included as this path
	// is used to retrieve supported API versions.
	if url.Path != "/api/api_version" {
		// If API version is not known yet, retrieve and cache it first.
		if p.driver.apiVersion == "" {
			apiVersions, err := p.getAPIVersions()
			if err != nil {
				return fmt.Errorf("Failed to retrieve supported Pure Storage API versions: %w", err)
			}

			// Ensure the required API version is supported by Pure Storage array.
			if !slices.Contains(apiVersions, pureAPIVersion) {
				return fmt.Errorf("Required API version %q is not supported by Pure Storage array", pureAPIVersion)
			}

			// Set API version to the driver to avoid checking the API version
			// for each subsequent request.
			p.driver.apiVersion = pureAPIVersion
		}

		// Prefix current path with the API version.
		url.Path = path.Join("api", p.driver.apiVersion, url.Path)
	}

	req, err := http.NewRequest(method, url.String(), reqBody)
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

	// The unauthorized error is reported when an invalid (or expired) access token is provided.
	// Wrap unauthorized requests into an API status error to allow easier checking for expired
	// token in the requestAuthenticated function.
	if resp.StatusCode == http.StatusUnauthorized {
		return api.StatusErrorf(http.StatusUnauthorized, "Unauthorized request")
	}

	// Overwrite the response data type if an error is detected.
	if resp.StatusCode != http.StatusOK {
		respBody = &pureError{}
	}

	// Extract the response body if requested.
	if respBody != nil {
		err = json.NewDecoder(resp.Body).Decode(respBody)
		if err != nil {
			return fmt.Errorf("Failed to read response body from %q: %w", url.String(), err)
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

// requestAuthenticated issues an authenticated HTTP request against the Pure Storage gateway.
// In case the access token is expired, the function will try to obtain a new one.
func (p *pureClient) requestAuthenticated(method string, url url.URL, reqBody io.Reader, respBody any) error {
	// If request fails with an unauthorized error, the request will be retried after
	// requesting a new access token.
	retries := 1

	for {
		// Ensure we are logged into the Pure Storage.
		err := p.login()
		if err != nil {
			return err
		}

		// Set access token as request header.
		reqHeaders := map[string]string{
			"X-Auth-Token": p.accessToken,
		}

		// Initiate request.
		err = p.request(method, url, reqBody, reqHeaders, respBody, nil)
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

// getAPIVersion returns the list of API versions that are supported by the Pure Storage.
func (p *pureClient) getAPIVersions() ([]string, error) {
	var resp struct {
		APIVersions []string `json:"version"`
	}

	url := api.NewURL().Path("api", "api_version")
	err := p.request(http.MethodGet, url.URL, nil, nil, &resp, nil)
	if err != nil {
		return nil, fmt.Errorf("Failed to retrieve available API versions from Pure Storage: %w", err)
	}

	if len(resp.APIVersions) == 0 {
		return nil, fmt.Errorf("Pure Storage does not support any API versions")
	}

	return resp.APIVersions, nil
}

// login initiates an authentication request against the Pure Storage using the API token. If successful,
// an access token is retrieved and stored within a client. The access token is then used for further
// authentication.
func (p *pureClient) login() error {
	if p.accessToken != "" {
		// Token has been already obtained.
		return nil
	}

	reqHeaders := map[string]string{
		"api-token": p.driver.config["pure.api.token"],
	}

	respHeaders := make(map[string]string)

	url := api.NewURL().Path("login")
	err := p.request(http.MethodPost, url.URL, nil, reqHeaders, nil, respHeaders)
	if err != nil {
		return fmt.Errorf("Failed to login: %w", err)
	}

	p.accessToken = respHeaders["X-Auth-Token"]
	if p.accessToken == "" {
		return errors.New("Failed to obtain access token")
	}

	return nil
}

// getNetworkInterfaces retrieves a valid Pure Storage network interfaces, which
// means the interface has an IP address configured and is enabled. The result
// can be filtered by a specific service name, where an empty string represents
// no filtering.
func (p *pureClient) getNetworkInterfaces(service string) ([]pureNetworkInterface, error) {
	var resp pureResponse[pureNetworkInterface]

	// Retrieve enabled network interfaces that have an IP address configured.
	url := api.NewURL().Path("network-interfaces").WithQuery("filter", "enabled='true'").WithQuery("filter", "eth.address")
	if service != "" {
		url = url.WithQuery("filter", "services='"+service+"'")
	}

	err := p.requestAuthenticated(http.MethodGet, url.URL, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("Failed to retrieve Pure Storage network interfaces: %w", err)
	}

	return resp.Items, nil
}

// getStoragePool returns the storage pool with the given name.
func (p *pureClient) getStoragePool(poolName string) (*pureStoragePool, error) {
	var resp pureResponse[pureStoragePool]

	url := api.NewURL().Path("pods").WithQuery("names", poolName)
	err := p.requestAuthenticated(http.MethodGet, url.URL, nil, &resp)
	if err != nil {
		if isPureErrorNotFound(err) {
			return nil, api.StatusErrorf(http.StatusNotFound, "Storage pool %q not found", poolName)
		}

		return nil, fmt.Errorf("Failed to get storage pool %q: %w", poolName, err)
	}

	if len(resp.Items) == 0 {
		return nil, api.StatusErrorf(http.StatusNotFound, "Storage pool %q not found", poolName)
	}

	return &resp.Items[0], nil
}

// createStoragePool creates a storage pool (Pure Storage pod).
func (p *pureClient) createStoragePool(poolName string, size int64) error {
	reqBody := make(map[string]any)
	if size > 0 {
		reqBody["quota_limit"] = size
	}

	pool, err := p.getStoragePool(poolName)
	if err == nil && pool.IsDestroyed {
		// Storage pool exists in destroyed state, therefore, restore it.
		reqBody["destroyed"] = false

		req, err := p.createBodyReader(reqBody)
		if err != nil {
			return err
		}

		url := api.NewURL().Path("pods").WithQuery("names", poolName)
		err = p.requestAuthenticated(http.MethodPatch, url.URL, req, nil)
		if err != nil {
			return fmt.Errorf("Failed to restore storage pool %q: %w", poolName, err)
		}

		logger.Info("Storage pool has been restored", logger.Ctx{"pool": poolName})
		return nil
	}

	req, err := p.createBodyReader(reqBody)
	if err != nil {
		return err
	}

	// Storage pool does not exist in destroyed state, therefore, try to create a new one.
	url := api.NewURL().Path("pods").WithQuery("names", poolName)
	err = p.requestAuthenticated(http.MethodPost, url.URL, req, nil)
	if err != nil {
		return fmt.Errorf("Failed to create storage pool %q: %w", poolName, err)
	}

	return nil
}

// deleteStoragePool deletes a storage pool (Pure Storage pod).
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

		url := api.NewURL().Path("pods").WithQuery("names", poolName).WithQuery("destroy_contents", "true")
		err = p.requestAuthenticated(http.MethodPatch, url.URL, req, nil)
		if err != nil {
			if isPureErrorNotFound(err) {
				return nil
			}

			return fmt.Errorf("Failed to destroy storage pool %q: %w", poolName, err)
		}
	}

	// Eradicate the storage pool by permanently deleting it along all of its contents.
	url := api.NewURL().Path("pods").WithQuery("names", poolName).WithQuery("eradicate_contents", "true")
	err = p.requestAuthenticated(http.MethodDelete, url.URL, nil, nil)
	if err != nil {
		if isPureErrorNotFound(err) {
			return nil
		}

		if isPureErrorOf(err, http.StatusBadRequest, "Cannot eradicate pod") {
			// Eradication failed, therefore the pool remains in the destroyed state.
			// However, we still consider it as deleted because Pure Storage SafeMode
			// may be enabled, which prevents immediate eradication of the pool.
			logger.Warn("Storage pool is left in destroyed state", logger.Ctx{"pool": poolName, "err": err})
			return nil
		}

		return fmt.Errorf("Failed to delete storage pool %q: %w", poolName, err)
	}

	return nil
}

// getHosts retrieves an existing Pure Storage host.
func (p *pureClient) getHosts() ([]pureHost, error) {
	var resp pureResponse[pureHost]

	url := api.NewURL().Path("hosts")
	err := p.requestAuthenticated(http.MethodGet, url.URL, nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("Failed to get hosts: %w", err)
	}

	return resp.Items, nil
}

// getCurrentHost retrieves the Pure Storage host linked to the current LXD host.
// The Pure Storage host is considered a match if it includes the fully qualified
// name of the LXD host that is determined by the configured mode.
func (p *pureClient) getCurrentHost() (*pureHost, error) {
	qn, err := p.driver.hostQN()
	if err != nil {
		return nil, err
	}

	hosts, err := p.getHosts()
	if err != nil {
		return nil, err
	}

	for _, host := range hosts {
		if slices.Contains(host.IQNs, qn) {
			return &host, nil
		}
	}

	return nil, api.StatusErrorf(http.StatusNotFound, "Host with qualified name %q not found", qn)
}

// createHost creates a new host with provided initiator qualified names that can be associated
// with specific volumes.
func (p *pureClient) createHost(hostName string, qns []string) error {
	body := make(map[string]any, 1)
	mode := p.driver.config["pure.mode"]

	switch mode {
	case pureModeISCSI:
		body["iqns"] = qns
	default:
		return fmt.Errorf("Unsupported Pure Storage mode %q", mode)
	}

	req, err := p.createBodyReader(body)
	if err != nil {
		return err
	}

	url := api.NewURL().Path("hosts").WithQuery("names", hostName)
	err = p.requestAuthenticated(http.MethodPost, url.URL, req, nil)
	if err != nil {
		if isPureErrorOf(err, http.StatusBadRequest, "Host already exists.") {
			return api.StatusErrorf(http.StatusConflict, "Host %q already exists", hostName)
		}

		return fmt.Errorf("Failed to create host %q: %w", hostName, err)
	}

	return nil
}

// updateHost updates an existing host.
func (p *pureClient) updateHost(hostName string, qns []string) error {
	body := make(map[string]any, 1)
	mode := p.driver.config["pure.mode"]

	switch mode {
	case pureModeISCSI:
		body["iqns"] = qns
	default:
		return fmt.Errorf("Unsupported Pure Storage mode %q", mode)
	}

	req, err := p.createBodyReader(body)
	if err != nil {
		return err
	}

	// To destroy the volume, we need to patch it by setting the destroyed to true.
	url := api.NewURL().Path("hosts").WithQuery("names", hostName)
	err = p.requestAuthenticated(http.MethodPatch, url.URL, req, nil)
	if err != nil {
		return fmt.Errorf("Failed to update host %q: %w", hostName, err)
	}

	return nil
}

// deleteHost deletes an existing host.
func (p *pureClient) deleteHost(hostName string) error {
	url := api.NewURL().Path("hosts").WithQuery("names", hostName)
	err := p.requestAuthenticated(http.MethodDelete, url.URL, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed to delete host %q: %w", hostName, err)
	}

	return nil
}

// connectHostToVolume creates a connection between a host and volume. It returns true if the connection
// was created, and false if it already existed.
func (p *pureClient) connectHostToVolume(poolName string, volName string, hostName string) (bool, error) {
	url := api.NewURL().Path("connections").WithQuery("host_names", hostName).WithQuery("volume_names", poolName+"::"+volName)

	err := p.requestAuthenticated(http.MethodPost, url.URL, nil, nil)
	if err != nil {
		if isPureErrorOf(err, http.StatusBadRequest, "Connection already exists.") {
			// Do not error out if connection already exists.
			return false, nil
		}

		return false, fmt.Errorf("Failed to connect volume %q with host %q: %w", volName, hostName, err)
	}

	return true, nil
}

// disconnectHostFromVolume deletes a connection between a host and volume.
func (p *pureClient) disconnectHostFromVolume(poolName string, volName string, hostName string) error {
	url := api.NewURL().Path("connections").WithQuery("host_names", hostName).WithQuery("volume_names", poolName+"::"+volName)

	err := p.requestAuthenticated(http.MethodDelete, url.URL, nil, nil)
	if err != nil {
		if isPureErrorNotFound(err) {
			return api.StatusErrorf(http.StatusNotFound, "Connection between host %q and volume %q not found", volName, hostName)
		}

		return fmt.Errorf("Failed to disconnect volume %q from host %q: %w", volName, hostName, err)
	}

	return nil
}

// getTarget retrieves the qualified name and addresses of Pure Storage target for the configured mode.
func (p *pureClient) getTarget() (targetQN string, targetAddrs []string, err error) {
	mode := p.driver.config["pure.mode"]

	// Get Pure Storage service name based on the configured mode.
	service, ok := pureServiceNameMapping[mode]
	if !ok {
		return "", nil, fmt.Errorf("Failed to determine service name for Pure Storage mode %q", mode)
	}

	// Retrieve the list of Pure Storage network interfaces.
	interfaces, err := p.getNetworkInterfaces(service)
	if err != nil {
		return "", nil, err
	}

	if len(interfaces) == 0 {
		return "", nil, api.StatusErrorf(http.StatusNotFound, "Enabled network interface with %q service not found", service)
	}

	targetAddrs = make([]string, 0, len(interfaces))
	for _, iface := range interfaces {
		targetAddrs = append(targetAddrs, iface.Ethernet.Address)
	}

	// Get the qualified name of the target by iterating over the available
	// ports until the one with the qualified name is found. All ports have
	// the same IQN, but it may happen that IQN is not reported for a
	// specific port, for example, if the port is misconfigured.
	var nq string
	for _, iface := range interfaces {
		var resp pureResponse[purePort]

		url := api.NewURL().Path("ports").WithQuery("filter", "name='"+iface.Name+"'")
		err = p.requestAuthenticated(http.MethodGet, url.URL, nil, &resp)
		if err != nil {
			return "", nil, fmt.Errorf("Failed to retrieve Pure Storage targets: %w", err)
		}

		if len(resp.Items) == 0 {
			continue
		}

		port := resp.Items[0]

		if mode == pureModeISCSI {
			nq = port.IQN
		}

		if nq != "" {
			break
		}
	}

	if nq == "" {
		return "", nil, api.StatusErrorf(http.StatusNotFound, "Qualified name for %q target not found", mode)
	}

	return nq, targetAddrs, nil
}

// ensureHost returns a name of the host that is configured with a given IQN. If such host
// does not exist, a new one is created, where host's name equals to the server name with a
// mode included as a suffix because Pure Storage does not allow mixing IQNs, NQNs, and WWNs
// on a single host.
func (d *pure) ensureHost() (hostName string, cleanup revert.Hook, err error) {
	var hostname string

	revert := revert.New()
	defer revert.Fail()

	// Get the qualified name of the host.
	qn, err := d.hostQN()
	if err != nil {
		return "", nil, err
	}

	// Fetch an existing Pure Storage host.
	host, err := d.client().getCurrentHost()
	if err != nil {
		if !api.StatusErrorCheck(err, http.StatusNotFound) {
			return "", nil, err
		}

		// The Pure Storage host with a qualified name of the current LXD host does not exist.
		// Therefore, create a new one and name it after the server name.
		serverName, err := d.serverName()
		if err != nil {
			return "", nil, err
		}

		// Append the mode to the server name because Pure Storage does not allow mixing
		// NQNs, IQNs, and WWNs for a single host.
		hostname = serverName + "-" + d.config["pure.mode"]

		err = d.client().createHost(hostname, []string{qn})
		if err != nil {
			if !api.StatusErrorCheck(err, http.StatusConflict) {
				return "", nil, err
			}

			// The host with the given name already exists, update it instead.
			err = d.client().updateHost(hostname, []string{qn})
			if err != nil {
				return "", nil, err
			}
		} else {
			revert.Add(func() { _ = d.client().deleteHost(hostname) })
		}
	} else {
		// Hostname already exists with the given IQN.
		hostname = host.Name
	}

	cleanup = revert.Clone().Fail
	revert.Success()
	return hostname, cleanup, nil
}

// connect connects this host with the PureStorge array. Note that the connection can only
// be established when at least one volume is mapped with the corresponding Pure Storage host.
// The operation is idempotent and returns nil if already connected to the subsystem.
func (d *pure) connect() (cleanup revert.Hook, err error) {
	revert := revert.New()
	defer revert.Fail()

	// Find the array's qualified name for the configured mode.
	targetQN, targetAddrs, err := d.client().getTarget()
	if err != nil {
		return nil, err
	}

	var connectFunc func(ctx context.Context, addr string) error

	switch d.config["pure.mode"] {
	case pureModeISCSI:
		// Try to find an existing iSCSI session.
		sessionID, err := iscsiFindSession(targetQN)
		if err != nil {
			return nil, err
		}

		if sessionID != "" {
			// Already connected to the Pure Storage array via iSCSI.
			// Rescan the session to ensure new volumes are detected.
			_, err := shared.RunCommand("iscsiadm", "--mode", "session", "--sid", sessionID, "--rescan")
			if err != nil {
				return nil, err
			}

			cleanup := revert.Clone().Fail
			revert.Success()
			return cleanup, nil
		}

		// Function that connects to the iSCSI target on a given address.
		connectFunc = func(ctx context.Context, addr string) error {
			// Discover iSCSI target.
			_, _, err := shared.RunCommandSplit(ctx, nil, nil, "iscsiadm", "--mode", "discovery", "--type", "sendtargets", "--portal", addr)
			if err != nil {
				return fmt.Errorf("Failed to discover Pure Storage targets on %q via iSCSI: %w", addr, err)
			}

			// Attempt to login into discovered iSCSI target.
			_, _, err = shared.RunCommandSplit(ctx, nil, nil, "iscsiadm", "--mode", "node", "--targetname", targetQN, "--portal", addr, "--login")
			if err != nil {
				return fmt.Errorf("Failed to connect to Pure Storage array %q via iSCSI: %w", addr, err)
			}

			return nil
		}

	default:
		return nil, fmt.Errorf("Unsupported Pure Storage mode %q", d.config["pure.mode"])
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(targetAddrs))

	// Do not defer context cancellation to avoid stopping connection attempts if
	// the function is left before all connection attempts are done.
	timeoutCtx, cancel := context.WithTimeout(d.state.ShutdownCtx, 30*time.Second)

	// Connect to all target addresses.
	for _, addr := range targetAddrs {
		wg.Add(1)

		go func() {
			defer wg.Done()
			errChan <- connectFunc(timeoutCtx, addr)
		}()
	}

	// Ensure error channel is closed once all routines have finished.
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Revert successful connections in case of an unexpected error.
	revert.Add(func() {
		// Cancel the context to immediately stop all connection attempts.
		cancel()

		// Wait until all connection attempts have finished.
		wg.Wait()

		// Revert any potential successful connections.
		_ = d.disconnect(targetQN)
	})

	// The minimum number of successful connections depends on the number
	// of available target addresses. If there is only one target address,
	// multipath cannot be enabled, therefore only one successful connection
	// is required. If there are multiple target addresses, we expect at least
	// two successful connections in order to configure multipath. While one
	// connection would be enough, we want to let the user know that multipath
	// is not possible and expect the environment to be fixed.
	minConns := 1
	if len(targetAddrs) > 1 {
		minConns = 2
	}

	// Number of finished connection attempts.
	doneConns := 0

	// Only one successful connection is required to succeed. Therefore, continue
	// once the first connection is established, or exit if all connections fail.
	for {
		err := <-errChan
		doneConns++

		if err == nil {
			minConns--
			if minConns > 0 {
				continue
			}

			// We have enough connections.
			break
		}

		if doneConns == len(targetAddrs) {
			// All connection attempts are done, but not enough
			// have succeeded.
			return nil, err
		}
	}

	cleanup = revert.Clone().Fail
	revert.Success()
	return cleanup, nil
}

// disconnect disconnects this host from the given target with a given qualified name.
func (d *pure) disconnect(targetQN string) error {
	switch d.config["pure.mode"] {
	case pureModeISCSI:
		sessionID, err := iscsiFindSession(targetQN)
		if err != nil {
			return err
		}

		if sessionID != "" {
			// Disconnect from the iSCSI target.
			_, err := shared.RunCommand("iscsiadm", "--mode", "node", "--targetname", targetQN, "--logout")
			if err != nil {
				return fmt.Errorf("Failed disconnecting from Pure Storage iSCSI target %q: %w", targetQN, err)
			}
		}
	default:
		return fmt.Errorf("Unsupported Pure Storage mode %q", d.config["pure.mode"])
	}

	return nil
}

// mapVolume maps the given volume onto this host.
func (d *pure) mapVolume(vol Volume) (cleanup revert.Hook, err error) {
	revert := revert.New()
	defer revert.Fail()

	volName, err := d.getVolumeName(vol)
	if err != nil {
		return nil, err
	}

	unlock, err := locking.Lock(d.state.ShutdownCtx, d.config["pure.mode"])
	if err != nil {
		return nil, err
	}

	defer unlock()

	// Ensure the host exists and is configured with the correct QN.
	hostname, cleanup, err := d.ensureHost()
	if err != nil {
		return nil, err
	}

	revert.Add(cleanup)

	// Ensure the volume is connected to the host.
	connCreated, err := d.client().connectHostToVolume(vol.pool, volName, hostname)
	if err != nil {
		return nil, err
	}

	if connCreated {
		revert.Add(func() { _ = d.client().disconnectHostFromVolume(vol.pool, volName, hostname) })
	}

	// Connect to the array.
	connCleanup, err := d.connect()
	if err != nil {
		return nil, err
	}

	revert.Add(connCleanup)

	cleanup = revert.Clone().Fail
	revert.Success()
	return cleanup, nil
}

// unmapVolume unmaps the given volume from this host.
func (d *pure) unmapVolume(vol Volume) error {
	volName, err := d.getVolumeName(vol)
	if err != nil {
		return err
	}

	unlock, err := locking.Lock(d.state.ShutdownCtx, d.config["pure.mode"])
	if err != nil {
		return err
	}

	defer unlock()

	host, err := d.client().getCurrentHost()
	if err != nil {
		return err
	}

	// Disconnect the volume from the host and ignore error if connection does not exist.
	err = d.client().disconnectHostFromVolume(vol.pool, volName, host.Name)
	if err != nil && !api.StatusErrorCheck(err, http.StatusNotFound) {
		return err
	}

	volumePath, _, _ := d.getMappedDevPath(vol, false)
	if volumePath != "" {
		if d.config["pure.mode"] == pureModeISCSI {
			// When volume is disconnected from the host, the device will remain on the system.
			//
			// To remove the device, we need to either logout from the session or remove the
			// device manually. Logging out of the session is not desired as it would disconnect
			// from all connected volumes. Therefore, we need to manually remove the device.
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

		// Wait until the volume has disappeared.
		ctx, cancel := context.WithTimeout(d.state.ShutdownCtx, 10*time.Second)
		defer cancel()

		if !waitGone(ctx, volumePath) {
			return fmt.Errorf("Timeout exceeded waiting for Pure Storage volume %q to disappear on path %q", vol.name, volumePath)
		}
	}

	// If this was the last volume being unmapped from this system, terminate iSCSI session
	// and remove the host from Pure Storage.
	if host.ConnectionCount <= 1 {
		targetQN, _, err := d.client().getTarget()
		if err != nil {
			return err
		}

		// Disconnect from the target.
		err = d.disconnect(targetQN)
		if err != nil {
			return err
		}

		// Remove the host from Pure Storage.
		err = d.client().deleteHost(host.Name)
		if err != nil {
			return err
		}
	}

	return nil
}

// getMappedDevPath returns the local device path for the given volume.
// Indicate with mapVolume if the volume should get mapped to the system if it isn't present.
func (d *pure) getMappedDevPath(vol Volume, mapVolume bool) (string, revert.Hook, error) {
	revert := revert.New()
	defer revert.Fail()

	if mapVolume {
		cleanup, err := d.mapVolume(vol)
		if err != nil {
			return "", nil, err
		}

		revert.Add(cleanup)
	}

	// findDevPathFunc has to be called in a loop with a set timeout to ensure
	// all the necessary directories and devices can be discovered.
	findDevPathFunc := func(diskPrefix string, diskSuffix string) (string, error) {
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
			if !strings.HasSuffix(diskPath, diskSuffix) {
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

	var volumeDevPath string
	var diskPrefix string
	var diskSuffix string

	volName, err := d.getVolumeName(vol)
	if err != nil {
		return "", nil, err
	}

	pureVol, err := d.client().getVolume(vol.pool, volName)
	if err != nil {
		return "", nil, err
	}

	switch d.config["pure.mode"] {
	case pureModeISCSI:
		diskPrefix = "scsi-"
		diskSuffix = strings.ToLower(pureVol.Serial)
	default:
		return "", nil, fmt.Errorf("Unsupported Pure Storage mode %q", d.config["pure.mode"])
	}

	ctx, cancel := context.WithTimeout(d.state.ShutdownCtx, 30*time.Second)
	defer cancel()

	// It might take a while to create the local disk.
	// Retry until it can be found.
	for {
		if ctx.Err() != nil {
			return "", nil, fmt.Errorf("Failed to locate device for volume %q: %v", vol.name, ctx.Err())
		}

		devPath, err := findDevPathFunc(diskPrefix, diskSuffix)
		if err != nil {
			// Try again if one of the directories cannot be found.
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

		time.Sleep(500 * time.Millisecond)
	}

	if volumeDevPath == "" {
		return "", nil, fmt.Errorf("Failed to locate device for volume %q", vol.name)
	}

	cleanup := revert.Clone().Fail
	revert.Success()
	return volumeDevPath, cleanup, nil
}

// iscsiFindSession returns the iSCSI session ID corresponding to the given IQN.
// If the session is not found, an empty string is returned.
func iscsiFindSession(iqn string) (string, error) {
	// Base path for iSCSI sessions.
	basePath := "/sys/class/iscsi_session"

	// Retrieve list of existing iSCSI sessions.
	sessions, err := os.ReadDir(basePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// No active sessions.
			return "", nil
		}

		return "", fmt.Errorf("Failed getting a list of existing iSCSI sessions: %w", err)
	}

	for _, session := range sessions {
		// Get the target IQN of the iSCSI session.
		iqnBytes, err := os.ReadFile(filepath.Join(basePath, session.Name(), "targetname"))
		if err != nil {
			return "", fmt.Errorf("Failed getting the target IQN for session %q: %w", session, err)
		}

		sessionIQN := strings.TrimSpace(string(iqnBytes))
		sessionID := strings.TrimPrefix(session.Name(), "session")

		if iqn == sessionIQN {
			// Already connected to the Pure Storage array via iSCSI.
			return sessionID, nil
		}
	}

	return "", nil
}

// hostQN returns the qualified name for the current host based on the configured mode.
func (d *pure) hostQN() (string, error) {
	switch d.config["pure.mode"] {
	case pureModeISCSI:
		// Get the unique iSCSI Qualified Name (IQN) of the host. The iscsiadm
		// does not allow providing the IQN directly, so we need to extract it
		// from the /etc/iscsi/initiatorname.iscsi file on the host.
		filename := shared.HostPath("/etc/iscsi/initiatorname.iscsi")
		if !shared.PathExists(filename) {
			return "", fmt.Errorf("Failed to extract host IQN: File %q does not exist", filename)
		}

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

		return "", fmt.Errorf(`Failed to extract host IQN: File %q does not contain "InitiatorName"`, filename)
	default:
		return "", fmt.Errorf("Unsupported Pure Storage mode %q", d.config["pure.mode"])
	}
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

// getVolumeName returns the fully qualified name derived from the volume's UUID.
func (d *pure) getVolumeName(vol Volume) (string, error) {
	volUUID, err := uuid.Parse(vol.config["volatile.uuid"])
	if err != nil {
		return "", fmt.Errorf(`Failed parsing "volatile.uuid" from volume %q: %w`, vol.name, err)
	}

	// Remove hypens from the UUID to create a volume name.
	volName := strings.ReplaceAll(volUUID.String(), "-", "")

	// Search for the volume type prefix, and if found, prepend it to the volume name.
	volumeTypePrefix, ok := pureVolTypePrefixes[vol.volType]
	if ok {
		volName = fmt.Sprintf("%s-%s", volumeTypePrefix, volName)
	}

	// Search for the content type suffix, and if found, append it to the volume name.
	contentTypeSuffix, ok := pureContentTypeSuffixes[vol.contentType]
	if ok {
		volName = fmt.Sprintf("%s-%s", volName, contentTypeSuffix)
	}

	// If volume is snapshot, prepend snapshot prefix to its name.
	if vol.IsSnapshot() {
		volName = fmt.Sprintf("%s%s", pureSnapshotPrefix, volName)
	}

	return volName, nil
}

// loadISCSIModules loads the iSCSI kernel modules.
// Returns true if the modules can be loaded.
func (d *pure) loadISCSIModules() bool {
	return util.LoadModule("iscsi_tcp") == nil
}
