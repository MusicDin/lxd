package clients

import (
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/canonical/lxd/lxd/storage/connectors"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/logger"
)

var powerStoreSessions = make(map[string]powerStoreSession)
var powerStoreSessionsLock = &sync.RWMutex{}

const (
	powerStoreAuthCookieName = "auth_cookie"
	powerStoreCSRFHeaderName = "dell-emc-token"

	// PowerStoreQueryResponseLimit is the maximum number of items PowerStore can return in
	// a single query response.
	PowerStoreQueryResponseLimit = 2000
)

// PowerStoreInitiatorType is an enumeration of initiator port type in
// PowerStore API.
type PowerStoreInitiatorType string

const (
	// InitiatorPortTypeEnumISCSI is an enumeration value indicating iSCSI
	// initiator port type.
	InitiatorPortTypeEnumISCSI PowerStoreInitiatorType = "iSCSI"

	// InitiatorPortTypeEnumFC is an enumeration value indicating fibre channel
	// initiator port type.
	InitiatorPortTypeEnumFC PowerStoreInitiatorType = "FC"

	// InitiatorPortTypeEnumNVMe is an enumeration value indicating NVMe
	// initiator port type.
	InitiatorPortTypeEnumNVMe PowerStoreInitiatorType = "NVMe"
)

type powerStoreErrorMessage struct {
	Message string `json:"message_l10n"`
}

// powerStoreError contains arbitrary error responses from PowerStore.
type powerStoreError struct {
	StatusCode int                      `json:"-"`
	Messages   []powerStoreErrorMessage `json:"messages,omitempty"`
}

func newPowerStoreError(resp *http.Response) error {
	if resp.StatusCode == http.StatusUnauthorized {
		return api.NewStatusError(http.StatusUnauthorized, "Unauthorized request")
	}

	psErr := &powerStoreError{
		StatusCode: resp.StatusCode,
	}

	if resp.Header.Get("Content-Type") != "application/json" || resp.Header.Get("Content-Length") == "0" {
		return psErr
	}

	err := json.NewDecoder(resp.Body).Decode(&psErr)
	if err != nil {
		return fmt.Errorf("Failed unmarshalling HTTP error response body: %w", err)
	}

	return psErr
}

// Error attempts to return all kinds of errors from the PowerStore API in
// a nicely formatted way.
func (e *powerStoreError) Error() string {
	msg := "PowerStore API error"
	if e.StatusCode != 0 {
		msg += " " + strconv.Itoa(e.StatusCode)
	}

	for _, em := range e.Messages {
		if em.Message != "" {
			msg += ": " + em.Message
		}
	}

	return msg
}

// isPowerStoreError checks if the error is of type powerStoreError and matches the status code.
func isPowerStoreError(err error, statusCode int, substrings ...string) bool {
	perr, ok := err.(*powerStoreError)
	if !ok {
		return false
	}

	if perr.StatusCode != statusCode {
		return false
	}

	if len(substrings) == 0 {
		return true
	}

	errMsg := strings.ToLower(perr.Error())
	for _, substring := range substrings {
		if strings.Contains(errMsg, strings.ToLower(substring)) {
			return true
		}
	}

	return false
}

// powerStoreSession describes PowerStore login session.
type powerStoreSession struct {
	ID              string
	IdleTimeout     time.Duration
	LastInteraction time.Time
	AuthToken       string
	CSRFToken       string
}

// IsValid inform if the token associated with the login session is not
// expired.
func (s *powerStoreSession) IsValid() bool {
	if s == nil {
		return false
	}

	return time.Now().Before(s.LastInteraction.Add(s.IdleTimeout))
}

// Interacted informs the login session object that interaction occurred and
// last interaction time should be updated.
func (s *powerStoreSession) Interacted() {
	s.LastInteraction = time.Now()
}

// PowerStoreResourceID is any resource that just contains ID. This type is often used
// a substitute when only ID of some resource should be retrieved or used.
type PowerStoreResourceID struct {
	ID string `json:"id"`
}

// PowerStoreInitiator describes an initiator resource in PowerStore API.
type PowerStoreInitiator struct {
	// ID       string `json:"id,omitempty"`
	HostID   string `json:"host_id,omitempty"`
	PortName string `json:"port_name,omitempty"`
	PortType string `json:"port_type,omitempty"`
}

func (PowerStoreInitiator) selector() string {
	return "host_id,port_name,port_type"
}

// PowerStoreHost describes a host resource in PowerStore API.
type PowerStoreHost struct {
	ID               string                         `json:"id,omitempty"`
	Name             string                         `json:"name,omitempty"`
	Description      string                         `json:"description,omitempty"`
	Initiators       []*PowerStoreHostInitiator     `json:"initiators,omitempty"`
	OsType           string                         `json:"os_type,omitempty"`
	HostConnectivity string                         `json:"host_connectivity,omitempty"`
	MappedVolumes    []*PowerStoreHostVolumeMapping `json:"mapped_hosts,omitempty"`
}

func (PowerStoreHost) selector() string {
	return "id,name,description,initiators(id,port_name,port_type),os_type,host_connectivity,mapped_hosts(id,host_id,volume_id)"
}

// PowerStoreHostInitiator describes an initiator resource of some host in
// PowerStore API.
type PowerStoreHostInitiator struct {
	// ID       string                  `json:"id,omitempty"`
	PortName string                  `json:"port_name,omitempty"`
	PortType PowerStoreInitiatorType `json:"port_type,omitempty"`
}

// PowerStoreVolume describes a volume resource in PowerStore API.
type PowerStoreVolume struct {
	ID            string                        `json:"id,omitempty"`
	Name          string                        `json:"name,omitempty"`
	Description   string                        `json:"description,omitempty"`
	Type          string                        `json:"type,omitempty"`
	State         string                        `json:"state,omitempty"`
	Size          int64                         `json:"size,omitempty"`
	LogicalUsed   int64                         `json:"logical_used,omitempty"`
	WWN           string                        `json:"wwn,omitempty"`
	AppType       string                        `json:"app_type,omitempty"`
	AppTypeOther  string                        `json:"app_type_other,omitempty"`
	VolumeGroups  []PowerStoreResourceID        `json:"volume_groups,omitempty"`
	MappedVolumes []PowerStoreHostVolumeMapping `json:"mapped_volumes,omitempty"`
}

func (PowerStoreVolume) selector() string {
	return "id,name,description,type,state,size,logical_used,wwn,app_type,app_type_other,volume_groups(id),mapped_volumes(id,host_id,volume_id)"
}

// PowerStoreHostVolumeMapping describes a mapping between host and volume in
// PowerStore API.
type PowerStoreHostVolumeMapping struct {
	// ID       string `json:"id,omitempty"`
	HostID   string `json:"host_id,omitempty"`
	VolumeID string `json:"volume_id,omitempty"`
}

// PowerStoreApplianceMetrics describes an appliance metric resource in
// PowerStore API.
type PowerStoreApplianceMetrics struct {
	ID                     string  `json:"id,omitempty"`
	Name                   string  `json:"name,omitempty"`
	AvgLatency             float64 `json:"avg_latency,omitempty"`
	TotalIops              float64 `json:"total_iops,omitempty"`
	TotalBandwidth         float64 `json:"total_bandwidth,omitempty"`
	LastLogicalTotalSpace  int64   `json:"last_logical_total_space,omitempty"`
	LastLogicalUsedSpace   int64   `json:"last_logical_used_space,omitempty"`
	LastPhysicalTotalSpace int64   `json:"last_physical_total_space,omitempty"`
	LastPhysicalUsedSpace  int64   `json:"last_physical_used_space,omitempty"`
}

func (PowerStoreApplianceMetrics) selector() string {
	return "id,name,avg_latency,total_iops,total_bandwidth,last_logical_total_space,last_logical_used_space,last_physical_total_space,last_physical_used_space"
}

// PowerStoreClient holds the PowerStore HTTP API client.
type PowerStoreClient struct {
	url           string
	skipTLSVerify bool
	username      string
	password      string

	resourceNamePrefix string

	session *powerStoreSession
	logger  logger.Logger
}

// NewPowerStoreClient creates a new instance of the PowerStore HTTP API client.
func NewPowerStoreClient(logger logger.Logger, url string, username string, password string, skipTLSVerify bool, volNamePrefix string) *PowerStoreClient {
	return &PowerStoreClient{
		url:                url,
		skipTLSVerify:      skipTLSVerify,
		username:           username,
		password:           password,
		resourceNamePrefix: volNamePrefix,
		logger:             logger,
	}
}

// sessionKey generates a hashtable key for a session.
func (c *PowerStoreClient) sessionKey() string {
	return c.url + c.username + c.password
}

// Session retrieves the current session.
func (c *PowerStoreClient) Session() *powerStoreSession {
	if c.session != nil {
		return c.session
	}

	key := c.sessionKey()

	powerStoreSessionsLock.RLock()
	defer powerStoreSessionsLock.RUnlock()

	session, ok := powerStoreSessions[key]
	if ok {
		s := session
		c.session = &s
		return c.session
	}

	return nil
}

// SetSession sets the current session.
func (c *PowerStoreClient) SetSession(session powerStoreSession) {
	key := c.sessionKey()

	powerStoreSessionsLock.Lock()
	defer powerStoreSessionsLock.Unlock()

	powerStoreSessions[key] = session
	c.session = &session
}

// InvalidateSession invalidates the current session.
func (c *PowerStoreClient) InvalidateSession() {
	key := c.sessionKey()

	powerStoreSessionsLock.Lock()
	defer powerStoreSessionsLock.Unlock()

	delete(powerStoreSessions, key)
	c.session = nil
}

// login initiates request() using PowerStore username and password.
// If successful, the session key is retrieved and stored within client structure.
// Once stored, the session key is reused for further requests.
func (c *PowerStoreClient) login() (*powerStoreSession, error) {
	session := c.Session()
	if session != nil {
		return session, nil
	}

	url := api.NewURL().Path("/api/rest/login_session")
	url = url.WithQuery("select", "id,user,role_ids,idle_timeout,is_password_change_required,is_built_in_user")

	// Base64 encode username and password for basic authentication.
	reqHeaders := map[string]string{
		"Authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte(c.username+":"+c.password)),
	}

	respHeaders := make(map[string]string)
	respBody := []struct {
		ID                       string   `json:"id"`
		User                     string   `json:"user"`
		RoleIDs                  []string `json:"role_ids"`
		IdleTimeout              int64    `json:"idle_timeout"`
		IsPasswordChangeRequired bool     `json:"is_password_change_required"`
		IsBuiltInUser            bool     `json:"is_built_in_user"`
	}{}

	err := c.request(http.MethodGet, url.URL, nil, reqHeaders, &respBody, respHeaders)
	if err != nil {
		return nil, fmt.Errorf("Failed logging into PowerStore: %w", err)
	}

	if len(respBody) < 1 {
		return nil, errors.New("Failed logging into PowerStore: Login response is missing session information")
	}

	sessionInfo := respBody[0]
	if sessionInfo.IsPasswordChangeRequired {
		return nil, errors.New("Failed logging into PowerStore: Password change required")
	}

	c.logger.Warn("Logged into PowerStore")

	// Parse auth cookie.
	resp := &http.Response{Header: http.Header{}}
	for k, v := range respHeaders {
		resp.Header[k] = []string{v}
	}

	// Parse CSRF token from response headers.
	csrfDuration := time.Duration(sessionInfo.IdleTimeout) * time.Second
	csrf := resp.Header.Get(powerStoreCSRFHeaderName)
	if csrf == "" {
		return nil, errors.New("Failed logging into PowerStore: Login response missing CSRF token")
	}

	var authCookie *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name != powerStoreAuthCookieName {
			continue
		}

		authCookie = c
		break
	}

	if authCookie == nil {
		return nil, errors.New("Starting PowerStore session: Missing PowerStore authorization cookie")
	}

	// Cache new session.
	session = &powerStoreSession{
		ID:              sessionInfo.ID,
		AuthToken:       authCookie.Value,
		CSRFToken:       csrf,
		IdleTimeout:     csrfDuration - 30*time.Second, // Subtract to add safety margin for a potential time skew.
		LastInteraction: time.Now(),
	}

	c.SetSession(*session)
	return session, nil
}

// request issues a HTTP request against the PowerStore gateway.
func (c *PowerStoreClient) request(method string, url url.URL, reqBody map[string]any, reqHeaders map[string]string, respBody any, respHeaders map[string]string) error {
	gw := c.url
	if !strings.Contains(gw, "://") {
		return fmt.Errorf("Invalid PowerStore URL %q: Missing protocol", gw)
	}

	gwURL, err := url.Parse(gw)
	if err != nil {
		return fmt.Errorf("Failed parsing PowerStore URL %q: %w", gw, err)
	}

	url.Scheme = gwURL.Scheme
	url.Host = gwURL.Host

	// Prepand gateway path with the request path in case PowerStore is served on a sub-path.
	url.Path = path.Join(gwURL.Path, url.Path)

	var reqBodyReader io.Reader

	if reqBody != nil {
		reqBodyReader, err = createBodyReader(reqBody)
		if err != nil {
			return err
		}
	}

	req, err := http.NewRequest(method, url.String(), reqBodyReader)
	if err != nil {
		return fmt.Errorf("Failed creating request: %w", err)
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
				InsecureSkipVerify: c.skipTLSVerify,
			},
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("Failed sending request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return newPowerStoreError(resp)
	}

	if respBody != nil {
		err := json.NewDecoder(resp.Body).Decode(respBody)
		if err != nil {
			return fmt.Errorf("Failed reading response body from %q: %w", url.String(), err)
		}
	}

	// Extract the response headers if requested.
	if respHeaders != nil {
		for k, v := range resp.Header {
			respHeaders[k] = strings.Join(v, ",")
		}
	}

	return nil
}

// requestAuthenticated issues an authenticated HTTP request against the PowerStore gateway.
// In case the access token is expired, the function will try to obtain a new one.
func (c *PowerStoreClient) requestAuthenticated(method string, url url.URL, reqBody map[string]any, respBody any, respHeaders map[string]string) error {
	// If request fails with an unauthorized error, the request will be retried after
	// requesting a new access token.
	retries := 1

	for {
		// Ensure we are logged into the PowerStore.
		session, err := c.login()
		if err != nil {
			return err
		}

		// Set access token as request header.
		reqHeaders := map[string]string{
			"Cookie":                 powerStoreAuthCookieName + "=" + session.AuthToken,
			powerStoreCSRFHeaderName: session.CSRFToken,
		}

		// Initiate request.
		err = c.request(method, url, reqBody, reqHeaders, respBody, respHeaders)
		if err != nil {
			if api.StatusErrorCheck(err, http.StatusUnauthorized) && retries > 0 {
				// The failure seems to be due to an expired session.
				// Invalidate session and try again.
				c.InvalidateSession()
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

// withPaginationQuery adds pagination parameters to the provided URL query.
func withPaginationQuery(url url.URL, offset uint64, limit int) url.URL {
	if limit <= 0 {
		limit = PowerStoreQueryResponseLimit
	}

	q := url.Query()
	q.Set("offset", strconv.FormatUint(offset, 10))
	q.Set("limit", strconv.Itoa(limit))
	url.RawQuery = q.Encode()
	return url
}

// parsePaginationOffset checks the provided headers and reports the new page offset and whether
// there are more items available to be retrieved.
func parsePaginationOffset(headers map[string]string) (newOffset uint64, hasMore bool, err error) {
	if headers == nil {
		return 0, false, nil
	}

	// valid Content-Range HTTP headers returned by PowerStore have a form:
	// - firstOffset '-' lastOffset '/' totalItems
	// - '*' '/' totalItems
	header := headers["Content-Range"]
	if header == "" {
		return 0, false, nil
	}

	errInvalidHeader := func() (uint64, bool, error) {
		return 0, false, fmt.Errorf("Invalid format of Content-Range header: %q", header)
	}

	rangeStr, totalItemsStr, ok := strings.Cut(header, "/")
	if !ok {
		return errInvalidHeader()
	}

	if rangeStr == "*" {
		return 0, false, nil
	}

	_, lastOffsetStr, ok := strings.Cut(rangeStr, "-")
	if !ok {
		return errInvalidHeader()
	}

	lastOffset, err := strconv.ParseUint(lastOffsetStr, 10, 64)
	if err != nil {
		return errInvalidHeader()
	}

	totalItems, err := strconv.ParseUint(totalItemsStr, 10, 64)
	if err != nil {
		return errInvalidHeader()
	}

	newOffset = lastOffset + 1
	return newOffset, totalItems > newOffset, nil
}

// func (c *PowerStoreClient) getHosts(queryFilters map[string]string, limit int) ([]PowerStoreHost, error) {
// 	url := api.NewURL().Path("api", "rest", "host")
// 	url = url.WithQuery("select", PowerStoreHost{}.selector())

// 	for k, v := range queryFilters {
// 		url = url.WithQuery(k, v)
// 	}

// 	var offset uint64
// 	var hosts []PowerStoreHost

// 	for {
// 		respBody := []PowerStoreHost{}
// 		respHeaders := make(map[string]string)

// 		pageURL := withPaginationQuery(url.URL, offset, limit)
// 		err := c.requestAuthenticated(http.MethodGet, pageURL, nil, &respBody, respHeaders)
// 		if err != nil {
// 			return nil, fmt.Errorf("Failed retrieving PowerStore hosts: %w", err)
// 		}

// 		nextOffset, hasMoreItems, err := parsePaginationOffset(respHeaders)
// 		if err != nil {
// 			return nil, fmt.Errorf("Failed retrieving PowerStore hosts: %w", err)
// 		}

// 		hosts = append(hosts, respBody...)
// 		offset = nextOffset

// 		if !hasMoreItems {
// 			break
// 		}
// 	}

// 	return hosts, nil
// }

// GetCurrentHost retrieves the PowerStore host linked to the current LXD host.
// The PowerStore host is considered a match if it includes the fully qualified
// name of the LXD host that is determined by the configured mode.
func (c *PowerStoreClient) GetCurrentHost(connectorType string, qn string) (*PowerStoreHost, error) {
	c.logger.Warn("Getting current host", logger.Ctx{"connector_type": connectorType, "qn": qn})

	var portType PowerStoreInitiatorType
	switch connectorType {
	case connectors.TypeISCSI:
		portType = InitiatorPortTypeEnumISCSI
	case connectors.TypeNVME:
		portType = InitiatorPortTypeEnumNVMe
	default:
		return nil, fmt.Errorf("Unsupported connector type: %q", connectorType)
	}

	// Find initiator with the provided port type (connector type) and name (qualified name),
	// and retrieve the ID of the host it belongs to.
	var initiator PowerStoreInitiator

	url := api.NewURL().Path("api", "rest", "initiator")
	url = url.WithQuery("select", initiator.selector())
	url = url.WithQuery("port_type", "eq."+string(portType))
	url = url.WithQuery("port_name", "eq."+qn)

	var initiators []PowerStoreInitiator
	err := c.requestAuthenticated(http.MethodGet, url.URL, nil, &initiators, nil)
	if err != nil {
		return nil, fmt.Errorf("Failed retrieving PowerStore host initiator: %w", err)
	}

	switch len(initiators) {
	case 0:
		return nil, api.StatusErrorf(http.StatusNotFound, "Host initiator with port name %q and type %q not found", qn, portType)
	case 1:
		initiator = initiators[0]
	default:
		return nil, fmt.Errorf("Multiple host initiators found with port name %q and type %q", qn, portType)
	}

	// Retrieve the actual host.
	var host PowerStoreHost
	url = api.NewURL().Path("api", "rest", "host", initiator.HostID)
	url = url.WithQuery("select", host.selector())

	err = c.requestAuthenticated(http.MethodGet, url.URL, nil, &host, nil)
	if err != nil {
		if isPowerStoreError(err, http.StatusNotFound) {
			return nil, api.StatusErrorf(http.StatusNotFound, "Host with initiator port name %q and type %q not found", qn, portType)
		}

		return nil, fmt.Errorf("Failed retrieving PowerStore host: %w", err)
	}

	return &host, nil
}

// GetHost retrieves host using its name.
func (c *PowerStoreClient) GetHost(hostName string) (*PowerStoreHost, error) {
	c.logger.Warn("Retrieving host", logger.Ctx{"host_name": hostName})
	var host PowerStoreHost

	url := api.NewURL().Path("api", "rest", "host", "name:"+hostName)
	url = url.WithQuery("select", host.selector())

	err := c.requestAuthenticated(http.MethodGet, url.URL, nil, &host, nil)
	if err != nil {
		if isPowerStoreError(err, http.StatusNotFound) {
			return nil, api.StatusErrorf(http.StatusNotFound, "Host with name %q not found", hostName)
		}

		return nil, fmt.Errorf("Failed retrieving PowerStore host: %w", err)
	}

	return &host, nil
}

// CreateHost creates new host.
func (c *PowerStoreClient) CreateHost(hostName string, connectorType string, qn string) (err error) {
	c.logger.Warn("Creating host", logger.Ctx{"hostname": hostName, "connector_type": connectorType, "qn": qn})
	url := api.NewURL().Path("api", "rest", "host")

	var portType PowerStoreInitiatorType
	switch connectorType {
	case connectors.TypeISCSI:
		portType = InitiatorPortTypeEnumISCSI
	case connectors.TypeNVME:
		portType = InitiatorPortTypeEnumNVMe
	default:
		return fmt.Errorf("Unsupported connector type: %q", connectorType)
	}

	req := map[string]any{
		"name":    hostName,
		"os_type": "Linux", // Required by PowerStore API.
		"initiators": []map[string]any{
			{
				"port_name": qn,
				"port_type": portType,
			},
		},
	}

	err = c.requestAuthenticated(http.MethodPost, url.URL, req, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed creating PowerStore host: %w", err)
	}

	return nil
}

// DeleteHost deletes host using its name.
func (c *PowerStoreClient) DeleteHost(hostName string) error {
	c.logger.Warn("Deleting host", logger.Ctx{"host_name": hostName})
	url := api.NewURL().Path("api", "rest", "host", "name:"+hostName)

	err := c.requestAuthenticated(http.MethodDelete, url.URL, nil, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed deleting PowerStore host: %w", err)
	}

	return nil
}

// AttachVolumeToHost attaches (maps) volume to host, returning true if the volume was freshly
// attached to the host, and false if the volume was already attached to the host.
func (c *PowerStoreClient) AttachVolumeToHost(volumeName string, hostName string) (bool, error) {
	c.logger.Warn("Attaching volume to host", logger.Ctx{"host_name": hostName, "volume_name": volumeName})

	// Check if the volume is already attached to the host.
	host, err := c.GetHost(hostName)
	if err != nil {
		return false, err
	}

	vol, err := c.GetVolume(volumeName)
	if err != nil {
		return false, err
	}

	for _, mapping := range host.MappedVolumes {
		if mapping.VolumeID == vol.ID {
			// The volume is already attached to the host.
			return false, nil
		}
	}

	// Attach the volume to the host.
	url := api.NewURL().Path("api", "rest", "volume", "name:"+volumeName, "attach")

	req := map[string]any{
		"host_id": "name:" + hostName,
	}

	err = c.requestAuthenticated(http.MethodPost, url.URL, req, nil, nil)
	if err != nil {
		return false, fmt.Errorf("Failed attaching PowerStore volume to the host: %w", err)
	}

	return true, nil
}

// DetachVolumeFromHost detaches (unmaps) volume from host.
func (c *PowerStoreClient) DetachVolumeFromHost(volumeName string, hostName string) error {
	c.logger.Warn("Detaching volume from host", logger.Ctx{"host_name": hostName, "volume_name": volumeName})
	url := api.NewURL().Path("api", "rest", "volume", "name:"+volumeName, "detach")

	req := map[string]any{
		"host_id": "name:" + hostName,
	}

	err := c.requestAuthenticated(http.MethodPost, url.URL, req, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed detaching PowerStore volume from the host: %w", err)
	}

	return nil
}

func (c *PowerStoreClient) getVolumes(queryFilter map[string]string) ([]PowerStoreVolume, error) {
	url := api.NewURL().Path("api", "rest", "volume")
	url = url.WithQuery("select", PowerStoreVolume{}.selector())

	for k, v := range queryFilter {
		url = url.WithQuery(k, v)
	}

	var offset uint64
	volumes := []PowerStoreVolume{}

	for {
		respBody := []PowerStoreVolume{}
		respHeaders := make(map[string]string)

		pageURL := withPaginationQuery(url.URL, offset, PowerStoreQueryResponseLimit)
		err := c.requestAuthenticated(http.MethodGet, pageURL, nil, &respBody, respHeaders)
		if err != nil {
			return nil, err
		}

		nextOffset, hasMoreItems, err := parsePaginationOffset(respHeaders)
		if err != nil {
			return nil, err
		}

		volumes = append(volumes, respBody...)
		offset = nextOffset

		if !hasMoreItems {
			break
		}
	}

	return volumes, nil
}

// GetVolumes retrieves list of volume associated with the storage pool.
func (c *PowerStoreClient) GetVolumes() ([]PowerStoreVolume, error) {
	c.logger.Warn("Getting volumes")
	filter := map[string]string{
		"name": "ilike." + c.resourceNamePrefix + "*",
		"or":   "(type.eq.Primary,type.eq.Clone)",
	}

	vols, err := c.getVolumes(filter)
	if err != nil {
		return nil, fmt.Errorf("Failed retrieving PowerStore volumes: %w", err)
	}

	return vols, nil
}

// GetVolume retrieves volume using its name.
func (c *PowerStoreClient) GetVolume(volumeName string) (*PowerStoreVolume, error) {
	var resp PowerStoreVolume

	url := api.NewURL().Path("api", "rest", "volume", "name:"+volumeName)
	url = url.WithQuery("or", "(type.eq.Primary,type.eq.Clone)")
	url = url.WithQuery("select", resp.selector())

	err := c.requestAuthenticated(http.MethodGet, url.URL, nil, &resp, nil)
	if err != nil {
		if isPowerStoreError(err, http.StatusNotFound) {
			return nil, api.StatusErrorf(http.StatusNotFound, "Volume with name %q not found", volumeName)
		}

		return nil, fmt.Errorf("Failed retrieving PowerStore volume with name %q: %w", volumeName, err)
	}

	return &resp, nil
}

// CreateVolume creates a new volume.
func (c *PowerStoreClient) CreateVolume(volumeName string, sizeBytes int64) error {
	c.logger.Warn("Creating volume", logger.Ctx{"volume_name": volumeName, "size_bytes": sizeBytes})
	url := api.NewURL().Path("api", "rest", "volume")

	req := map[string]any{
		"name": volumeName,
		"size": sizeBytes,
	}

	err := c.requestAuthenticated(http.MethodPost, url.URL, req, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed creating PowerStore volume: %w", err)
	}

	return nil
}

// DeleteVolume deletes volume using its name.
func (c *PowerStoreClient) DeleteVolume(volumeName string) error {
	c.logger.Warn("Deleting volume", logger.Ctx{"volume_name": volumeName})
	url := api.NewURL().Path("api", "rest", "volume", "name:"+volumeName)

	err := c.requestAuthenticated(http.MethodDelete, url.URL, nil, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed deleting PowerStore volume: %w", err)
	}

	return nil
}

// ResizeVolume creates a new volume.
func (c *PowerStoreClient) ResizeVolume(volumeName string, newSize int64) error {
	c.logger.Warn("Resizing volume", logger.Ctx{"volume_name": volumeName, "new_size": newSize})
	url := api.NewURL().Path("api", "rest", "volume", "name:"+volumeName)

	req := map[string]any{
		"size": newSize,
	}

	err := c.requestAuthenticated(http.MethodPatch, url.URL, req, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed resizing PowerStore volume: %w", err)
	}

	return nil
}

// CloneVolume clones the volume or the volume snapshot with the provided name to a new volume.
func (c *PowerStoreClient) CloneVolume(volumeName string, dstVolumeName string) error {
	c.logger.Warn("Cloning volume", logger.Ctx{"src_volume_name": volumeName, "dst_volume_name": dstVolumeName})
	url := api.NewURL().Path("api", "rest", "volume", "name:"+volumeName, "clone")

	req := map[string]any{
		"name":        dstVolumeName,
		"description": `LXD Volume Clone from "` + dstVolumeName + `"`,
	}

	err := c.requestAuthenticated(http.MethodPost, url.URL, req, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed cloning PowerStore volume: %w", err)
	}

	return nil
}

// RestoreVolume restores the volume form the volume snapshot.
func (c *PowerStoreClient) RestoreVolume(volumeName string, snapshotName string) error {
	c.logger.Warn("Restoring volume", logger.Ctx{"snapshot_name": snapshotName, "volume_name": volumeName})
	url := api.NewURL().Path("api", "rest", "volume", "name:"+volumeName, "restore")

	req := map[string]any{
		"from_snap_id": "name:" + snapshotName,
	}

	err := c.requestAuthenticated(http.MethodPost, url.URL, req, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed restoring PowerStore volume from snapshot: %w", err)
	}

	return nil
}

// RefreshVolume refreshes the volume form the volume or the volume snapshot.
func (c *PowerStoreClient) RefreshVolume(volumeName string, srcVolumeOrSnapshotName string) error {
	c.logger.Warn("Refreshing volume", logger.Ctx{"src_volume_name": srcVolumeOrSnapshotName, "dst_volume_name": volumeName})
	url := api.NewURL().Path("api", "rest", "volume", "name:"+volumeName, "refresh")

	req := map[string]any{
		"from_object_id": "name:" + srcVolumeOrSnapshotName,
	}

	err := c.requestAuthenticated(http.MethodPost, url.URL, req, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed refreshing PowerStore volume: %w", err)
	}

	return nil
}

// GetVolumeSnapshots retrieves list of snapshots associated with the provided volume.
func (c *PowerStoreClient) GetVolumeSnapshots(volumeName string) ([]PowerStoreVolume, error) {
	vol, err := c.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}

	c.logger.Warn("Getting volume snapshots", logger.Ctx{"volume_name": volumeName})
	filter := map[string]string{
		"name":                        "ilike." + c.resourceNamePrefix + "*",
		"type":                        "eq.Snapshot",
		"protection_data->>parent_id": "eq." + vol.ID,
	}

	snapshots, err := c.getVolumes(filter)
	if err != nil {
		return nil, fmt.Errorf("Failed retrieving PowerStore snapshots for volume %q: %w", volumeName, err)
	}

	return snapshots, nil
}

// GetVolumeSnapshot retrieves a snapshot of a volume.
func (c *PowerStoreClient) GetVolumeSnapshot(volumeName string, snapshotName string) (*PowerStoreVolume, error) {
	var resp PowerStoreVolume

	vol, err := c.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}

	url := api.NewURL().Path("api", "rest", "volume", "name:"+snapshotName)
	url = url.WithQuery("type", "eq.Snapshot")
	url = url.WithQuery("protection_data->>parent_id", "eq."+vol.ID)
	url = url.WithQuery("select", resp.selector())

	err = c.requestAuthenticated(http.MethodGet, url.URL, nil, &resp, nil)
	if err != nil {
		if isPowerStoreError(err, http.StatusNotFound) {
			return nil, api.StatusErrorf(http.StatusNotFound, "Volume snapshot with name %q not found", snapshotName)
		}

		return nil, fmt.Errorf("Failed retrieving PowerStore volume snapshot with name %q: %w", snapshotName, err)
	}

	return &resp, nil
}

// CreateVolumeSnapshot creates a new snapshot of a volume.
func (c *PowerStoreClient) CreateVolumeSnapshot(volumeName string, snapshotName string) error {
	c.logger.Warn("Creating volume snapshot", logger.Ctx{"volume_name": volumeName, "snapshot_name": snapshotName})
	url := api.NewURL().Path("api", "rest", "volume", "name:"+volumeName, "snapshot")

	req := map[string]any{
		"name":        snapshotName,
		"description": "LXD Volume Snapshot of " + snapshotName,
	}

	err := c.requestAuthenticated(http.MethodPost, url.URL, req, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed creating PowerStore volume snapshot with name %q: %w", snapshotName, err)
	}

	return nil
}

// DeleteVolumeSnapshot deletes a snapshot of a volume.
func (c *PowerStoreClient) DeleteVolumeSnapshot(volumeName string, snapshotName string) error {
	c.logger.Warn("Deleting volume snapshot", logger.Ctx{"volume_name": volumeName, "snapshot_name": snapshotName})
	url := api.NewURL().Path("api", "rest", "volume", "name:"+volumeName, "snapshot", "name:"+snapshotName)

	err := c.requestAuthenticated(http.MethodDelete, url.URL, nil, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed deleting PowerStore volume snapshot with name %q: %w", snapshotName, err)
	}

	return nil
}

// GetApplianceMetrics retrieves appliance metrics.
func (c *PowerStoreClient) GetApplianceMetrics() ([]PowerStoreApplianceMetrics, error) {
	c.logger.Warn("Getting appliance metrics")
	url := api.NewURL().Path("api", "rest", "appliance_list_cma_view")
	url = url.WithQuery("select", PowerStoreApplianceMetrics{}.selector())

	var offset uint64
	var metrics []PowerStoreApplianceMetrics

	for {
		respBody := []PowerStoreApplianceMetrics{}
		respHeaders := make(map[string]string)

		pageURL := withPaginationQuery(url.URL, offset, -1)
		err := c.requestAuthenticated(http.MethodGet, pageURL, nil, &respBody, respHeaders)
		if err != nil {
			return nil, fmt.Errorf("Failed retrieving metrics of PowerStore appliances: %w", err)
		}

		nextOffset, hasMoreItems, err := parsePaginationOffset(respHeaders)
		if err != nil {
			return nil, fmt.Errorf("Failed retrieving metrics of PowerStore appliances: %w", err)
		}

		metrics = append(metrics, respBody...)
		offset = nextOffset

		if !hasMoreItems {
			break
		}
	}

	return metrics, nil
}
