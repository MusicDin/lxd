package drivers

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/canonical/lxd/lxd/util"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/logger"
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

// ErrorCode extracts the errorCode value from a PureStorage response.
// func (p *pureError) ErrorCode() float64 {
// 	// In case the errorCode value is returned from the PureStorage API,
// 	// the respective integer value gets unmarshalled as float64.
// 	// See https://pkg.go.dev/encoding/json#Unmarshal for JSON numbers.
// 	code, ok := (*p)["errorCode"].(float64)
// 	if !ok {
// 		return 0
// 	}

// 	// TODO: Convert float number into integer

// 	return code
// }

// // HTTPStatusCode extracts the httpStatusCode value from a PureStorage response.
// func (p *pureError) HTTPStatusCode() float64 {
// 	// In case the httpStatusCode value is returned from the PureStorage API,
// 	// the respective integer value gets unmarshalled as float64.
// 	// See https://pkg.go.dev/encoding/json#Unmarshal for JSON numbers.
// 	code, ok := (*p)["httpStatusCode"].(float64)
// 	if !ok {
// 		return 0
// 	}

// 	return code
// }

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

	// Overwrite the response data type if an error is detected.
	// Both HTTP status code and PowerFlex error code get mapped to the
	// custom error struct from the response body.
	if resp.StatusCode != http.StatusOK {
		respBody = &pureError{}
	}

	// Extract the response body if requested.
	if respBody != nil {
		decoder := json.NewDecoder(resp.Body)
		err = decoder.Decode(respBody)
		if err != nil {
			return fmt.Errorf("Failed to read response body: %s: %w", path, err)
		}
	}

	// Extract the response headers if requested.
	if respHeaders != nil {
		for k, v := range resp.Header {
			respHeaders[k] = strings.Join(v, ",")
			logger.Warn("Response header", logger.Ctx{"key": k, "value": respHeaders[k]})
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
func (p *pureClient) getStoragePool(name string) (*pureStoragePool, error) {
	var actualResponse pureStoragePool
	err := p.requestAuthenticated(http.MethodGet, fmt.Sprintf("/pods?names=%s", name), nil, &actualResponse)
	if err != nil {
		return nil, fmt.Errorf("Failed to get storage pool %q: %w", name, err)
	}

	return &actualResponse, nil
}

// createStoragePool creates a storage pool (PureStorage Pod) and returns it's ID.
func (p *pureClient) createStoragePool(name string) (string, error) {
	var resp struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}

	err := p.requestAuthenticated(http.MethodPost, fmt.Sprintf("/pods?names=%s", name), nil, &resp)
	if err != nil {
		return "", fmt.Errorf("Failed to create storage pool %q: %w", name, err)
	}

	if len(resp.Items) == 0 || resp.Items[0].ID == "" {
		return "", fmt.Errorf(`Failed to create storage pool %q: Response does not contain field "id"`, name)
	}

	return resp.Items[0].ID, nil
}

// resolvePool looks up the selected storage pool.
// If only the pool is provided, it's expected to be the ID of the pool.
// In case both pool and domain are set, the pool will get looked up
// by name within the domain.
func (d *pure) resolvePool() (*powerFlexStoragePool, error) {
	return nil, nil
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
