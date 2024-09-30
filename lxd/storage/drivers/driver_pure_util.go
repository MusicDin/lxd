package drivers

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/canonical/lxd/lxd/util"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
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

// pureError contains arbitrary error responses from PureStorage. The maps values can be of various types.
// Reading of the actual values is performed by specific receiver functions which are implemented on the
// type itself.
type pureError map[string]any

// Error tries to return all kinds of errors from the PureStorage API in a nicely formatted way.
func (p *pureError) Error() string {
	var errorStrings []string

	for k, v := range *p {
		errorStrings = append(errorStrings, fmt.Sprintf("%s: %v", k, v))
	}

	return strings.Join(errorStrings, ", ")
}

// ErrorCode extracts the errorCode value from a PureStorage response.
func (p *pureError) ErrorCode() float64 {
	// In case the errorCode value is returned from the PureStorage API,
	// the respective integer value gets unmarshalled as float64.
	// See https://pkg.go.dev/encoding/json#Unmarshal for JSON numbers.
	code, ok := (*p)["errorCode"].(float64)
	if !ok {
		return 0
	}

	// TODO: Convert float number into integer

	return code
}

// HTTPStatusCode extracts the httpStatusCode value from a PureStorage response.
func (p *pureError) HTTPStatusCode() float64 {
	// In case the httpStatusCode value is returned from the PureStorage API,
	// the respective integer value gets unmarshalled as float64.
	// See https://pkg.go.dev/encoding/json#Unmarshal for JSON numbers.
	code, ok := (*p)["httpStatusCode"].(float64)
	if !ok {
		return 0
	}

	return code
}

// pureProtectionDomain represents a protection domain in PureStorage.
type pureProtectionDomain struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	SystemID string `json:"systemId"`
}

// pureStoragePool represents a storage pool (Pod) in PureStorage.
type pureStoragePool struct {
	ID   string `json:"id"`
	Name string `json:"name"`
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
func (p *pureClient) request(method string, path string, body io.Reader, headers map[string]string, response any) error {
	url := fmt.Sprintf("%s%s", p.driver.config["pure.gateway"], path)
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return fmt.Errorf("Failed to create request: %w", err)
	}

	// Set custom headers.
	for k, v := range headers {
		req.Header.Add(k, v)
	}

	req.Header.Add("Accept", "application/json")
	if body != nil {
		req.Header.Add("Content-Type", "application/json")
	}

	if p.accessToken != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", p.accessToken))
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

	// TODO: Verify this comment holds (second line)!
	// Exit right away if not authorized.
	// We cannot parse the returned body since it's not in JSON format.
	if resp.StatusCode == http.StatusUnauthorized && resp.Header.Get("Content-Type") != "application/json" {
		return api.StatusErrorf(http.StatusUnauthorized, "Unauthorized request")
	}

	// Overwrite the response data type if an error is detected.
	// Both HTTP status code and PowerFlex error code get mapped to the
	// custom error struct from the response body.
	if resp.StatusCode != http.StatusOK {
		response = &pureError{}
	}

	if response != nil {
		decoder := json.NewDecoder(resp.Body)
		err = decoder.Decode(response)
		if err != nil {
			return fmt.Errorf("Failed to read response body: %s: %w", path, err)
		}
	}

	// Return the formatted error from the body
	pureErr, ok := response.(*pureError)
	if ok {
		return pureErr
	}

	return nil
}

// requestAuthenticated issues an authenticated HTTP request against the PureStorage gateway. In case
// the access token is expired, the function will try to obtain a new one.
func (p *pureClient) requestAuthenticated(method string, path string, body io.Reader, response any) error {
	retries := 1
	for {
		// Ensure we are logged into the PureStorage.
		err := p.login()
		if err != nil {
			return err
		}

		// Set access token as request header.
		headers := map[string]string{
			"X-Auth-Token": p.accessToken,
		}

		// Initiate request.
		err = p.request(method, path, body, headers, response)
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

// login initiates an authentication request against the PureStorage using the API token. If successful,
// an access token is retrieved and stored within a client. The access token is then used for futher
// authentication.
func (p *pureClient) login() error {
	if p.accessToken != "" {
		// Token has been already obtained.
		return nil
	}

	headers := map[string]string{
		"api-token": p.driver.config["pure.api_token"],
	}

	var actualResponse struct {
		AccessToken string `json:"x-auth-token"`
	}

	err := p.request(http.MethodPost, "/login", nil, headers, &actualResponse)
	if err != nil {
		return fmt.Errorf("Failed to login: %w", err)
	}

	p.accessToken = actualResponse.AccessToken
	return nil
}

// getVolumeID returns the volume ID for the given name.
func (p *pureClient) getVolumeID(name string) (string, error) {
	body, err := p.createBodyReader(map[string]any{
		"name": name,
	})
	if err != nil {
		return "", err
	}

	var actualResponse string
	err = p.requestAuthenticated(http.MethodPost, "/api/types/Volume/instances/action/queryIdByKey", body, &actualResponse)
	if err != nil {
		powerFlexError, ok := err.(*powerFlexError)
		if ok {
			// API returns 500 if the volume does not exist.
			// To not confuse it with other 500 that might occur check the error code too.
			if powerFlexError.HTTPStatusCode() == http.StatusInternalServerError && powerFlexError.ErrorCode() == powerFlexCodeVolumeNotFound {
				return "", api.StatusErrorf(http.StatusNotFound, "PowerFlex volume not found: %q", name)
			}
		}

		return "", fmt.Errorf("Failed to get volume ID: %q: %w", name, err)
	}

	return actualResponse, nil
}

// getStoragePool returns the storage pool behind poolID.
func (p *pureClient) getStoragePool(poolID string) (*powerFlexStoragePool, error) {
	var actualResponse powerFlexStoragePool
	err := p.requestAuthenticated(http.MethodGet, fmt.Sprintf("/api/instances/StoragePool::%s", poolID), nil, &actualResponse)
	if err != nil {
		return nil, fmt.Errorf("Failed to get storage pool: %q: %w", poolID, err)
	}

	return &actualResponse, nil
}

// resolvePool looks up the selected storage pool.
// If only the pool is provided, it's expected to be the ID of the pool.
// In case both pool and domain are set, the pool will get looked up
// by name within the domain.
func (d *pure) resolvePool() (*powerFlexStoragePool, error) {
	return d.client().getStoragePool(d.config["powerflex.pool"])
}

// getPowerFlexVolumeName returns the fully qualified name derived from the volume.
func (d *pure) getVolumeName(vol Volume) (string, error) {
	volUUID, err := uuid.Parse(vol.config["volatile.uuid"])
	if err != nil {
		return "", fmt.Errorf(`Failed parsing "volatile.uuid" from volume %q: %w`, vol.name, err)
	}

	binUUID, err := volUUID.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf(`Failed marshalling the "volatile.uuid" of volume %q to binary format: %w`, vol.name, err)
	}

	// The volume's name in base64 encoded format.
	volName := base64.StdEncoding.EncodeToString(binUUID)

	var suffix string
	if vol.contentType == ContentTypeBlock {
		suffix = powerFlexBlockVolSuffix
	} else if vol.contentType == ContentTypeISO {
		suffix = powerFlexISOVolSuffix
	}

	// Use storage volume prefix from powerFlexVolTypePrefixes depending on type.
	// If the volume's type is unknown, don't put any prefix to accommodate the volume name size constraint.
	volumeTypePrefix, ok := powerFlexVolTypePrefixes[vol.volType]
	if ok {
		volumeTypePrefix = fmt.Sprintf("%s_", volumeTypePrefix)
	}

	return fmt.Sprintf("%s%s%s", volumeTypePrefix, volName, suffix), nil
}
