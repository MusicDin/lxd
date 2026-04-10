package clients

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
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

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

// OSTypeEnum is an enumeration of operating system type in PowerStore API.
type OSTypeEnum string

const (
	// OSTypeEnumLinux is an enumeration value indicating Linux operating system.
	OSTypeEnumLinux OSTypeEnum = "Linux"
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

// powerStoreError contains arbitrary error responses from PowerStore.
type powerStoreError struct {
	httpStatusCode int
	details        errorResponseResource
	decoderErr     error
}

func newPowerStoreError(resp *http.Response) error {
	if resp.StatusCode == http.StatusUnauthorized {
		return api.NewStatusError(http.StatusUnauthorized, "Unauthorized request")
	}

	psErr := &powerStoreError{
		httpStatusCode: resp.StatusCode,
	}

	if resp.Header.Get("Content-Type") != "application/json" || resp.Header.Get("Content-Length") == "0" {
		return psErr
	}

	err := json.NewDecoder(resp.Body).Decode(&psErr.details)
	if err != nil {
		psErr.decoderErr = fmt.Errorf("Failed unmarshalling HTTP error response body: %w", err)
	}

	return psErr
}

// HTTPStatusCode attempts to extract the HTTP status code value from
// a PowerStore response. If the error is not associated with some HTTP error
// code function returns zero.
func (e *powerStoreError) HTTPStatusCode() int {
	return e.httpStatusCode
}

// ErrorCode attempts to extract the PowerStore error code value. If the error
// do not contains the PowerStore error code code function returns an empty
// string.
func (e *powerStoreError) ErrorCode() string {
	for _, em := range e.details.Messages {
		if em != nil && em.Code != "" {
			return em.Code
		}
	}
	return ""
}

// Error attempts to return all kinds of errors from the PowerStore API in
// a nicely formatted way.
func (e *powerStoreError) Error() string {
	msg := "PowerStore API error"
	if e.httpStatusCode != 0 {
		msg = fmt.Sprintf("%s %d response", msg, e.httpStatusCode)
	}

	details, err := json.Marshal(e.details)
	if err == nil && len(details) > 0 && !bytes.Equal(details, []byte("{}")) && !bytes.Equal(details, []byte("null")) {
		msg = fmt.Sprintf("%s; details: %s", msg, details)
	}

	if e.decoderErr != nil {
		msg = fmt.Sprintf("%s; response decoding error: %s", msg, e.decoderErr.Error())
	}

	return msg
}

type errorResponseResource struct {
	Messages []*errorMessageResource `json:"messages,omitempty"`
}

type errorMessageResource struct {
	Severity    string                          `json:"severity"`
	Code        string                          `json:"code"`
	MessageL10n string                          `json:"message_l10n"`
	Arguments   []*errorMessageArgumentResource `json:"arguments,omitempty"`
}

type errorMessageArgumentResource struct {
	Delimiter string                   `json:"delimiter,omitempty"`
	Messages  []*errorInstanceResource `json:"messages,omitempty"`
}

type errorInstanceResource struct {
	Severity    string   `json:"severity"`
	Code        string   `json:"code"`
	MessageL10n string   `json:"message_l10n"`
	Arguments   []string `json:"arguments,omitempty"`
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
	ID       string `json:"id,omitempty"`
	HostID   string `json:"host_id,omitempty"`
	PortName string `json:"port_name,omitempty"`
	PortType string `json:"port_type,omitempty"`
}

// PowerStoreHost describes a host resource in PowerStore API.
type PowerStoreHost struct {
	ID               string                         `json:"id,omitempty"`
	Name             string                         `json:"name,omitempty"`
	Description      string                         `json:"description,omitempty"`
	Initiators       []*PowerStoreHostInitiator     `json:"initiators,omitempty"`
	OsType           OSTypeEnum                     `json:"os_type,omitempty"`
	HostConnectivity string                         `json:"host_connectivity,omitempty"`
	MappedVolumes    []*PowerStoreHostVolumeMapping `json:"mapped_hosts,omitempty"`
}

// PowerStoreHostInitiator describes an initiator resource of some host in
// PowerStore API.
type PowerStoreHostInitiator struct {
	ID       string                  `json:"id,omitempty"`
	PortName string                  `json:"port_name,omitempty"`
	Type     PowerStoreInitiatorType `json:"port_type,omitempty"`
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

// PowerStoreHostVolumeMapping describes a mapping between host and volume in
// PowerStore API.
type PowerStoreHostVolumeMapping struct {
	ID       string `json:"id,omitempty"`
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

// PowerStoreClient holds the PowerStore HTTP API client.
type PowerStoreClient struct {
	logger        logger.Logger
	url           string
	skipTLSVerify bool
	username      string
	password      string

	// tokenCache       *tokencache.TokenCache[powerStoreSession]

	session *powerStoreSession

	volumeNamePrefix string
	// hostNamePrefix       string
}

// NewPowerStoreClient creates a new instance of the PowerStore HTTP API client.
func NewPowerStoreClient(logger logger.Logger, url string, username string, password string, skipTLSVerify bool, volNamePrefix string) *PowerStoreClient {
	return &PowerStoreClient{
		logger:        logger,
		url:           url,
		skipTLSVerify: skipTLSVerify,
		username:      username,
		password:      password,
		// tokenCache:       powerStoreTokenCache,
		volumeNamePrefix: volNamePrefix,
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
func (c *PowerStoreClient) login(ctx context.Context) (*powerStoreSession, error) {
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

	err := c.request(ctx, http.MethodGet, url.URL, nil, reqHeaders, &respBody, respHeaders)
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

	// TODO: Remove DEBUG
	// DEBUG: Start
	logCtx := make(map[string]any, len(respHeaders))
	for k, v := range respHeaders {
		logCtx[k] = v
	}

	c.logger.Warn("RESPONSE HEADERS", logCtx)
	// DEBUG: End

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
func (c *PowerStoreClient) request(ctx context.Context, method string, url url.URL, reqBody map[string]any, reqHeaders map[string]string, respBody any, respHeaders map[string]string) error {
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

	req, err := http.NewRequestWithContext(ctx, method, url.String(), reqBodyReader)
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
func (c *PowerStoreClient) requestAuthenticated(ctx context.Context, method string, url url.URL, reqBody map[string]any, respBody any, respHeaders map[string]string) error {
	// If request fails with an unauthorized error, the request will be retried after
	// requesting a new access token.
	retries := 1

	for {
		// Ensure we are logged into the PowerStore.
		session, err := c.login(ctx)
		if err != nil {
			return err
		}

		// Set access token as request header.
		reqHeaders := map[string]string{
			"Cookie":                 powerStoreAuthCookieName + "=" + session.AuthToken,
			powerStoreCSRFHeaderName: session.CSRFToken,
		}

		// Initiate request.
		err = c.request(ctx, method, url, reqBody, reqHeaders, respBody, respHeaders)
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

// GetApplianceMetrics retrieves appliance metrics.
func (c *PowerStoreClient) GetApplianceMetrics(ctx context.Context) ([]PowerStoreApplianceMetrics, error) {
	url := api.NewURL().Path("api", "rest", "appliance_list_cma_view")
	url = url.WithQuery("select", "id,name,avg_latency,total_iops,total_bandwidth,last_logical_total_space,last_logical_used_space,last_physical_total_space,last_physical_used_space")

	var offset uint64
	var metrics []PowerStoreApplianceMetrics

	for {
		respBody := []PowerStoreApplianceMetrics{}
		respHeaders := make(map[string]string)

		pageURL := withPaginationQuery(url.URL, offset, -1)
		err := c.requestAuthenticated(ctx, http.MethodGet, pageURL, nil, &respBody, respHeaders)
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

func (c *PowerStoreClient) getHosts(ctx context.Context, queryFilters map[string]string, limit int) ([]PowerStoreHost, error) {
	url := api.NewURL().Path("api", "rest", "host")
	url = url.WithQuery("select", "id,name,description,initiators(id,port_name,port_type),os_type,host_connectivity,mapped_hosts(id,host_id,volume_id)")

	for k, v := range queryFilters {
		url = url.WithQuery(k, v)
	}

	var offset uint64
	var hosts []PowerStoreHost

	for {
		respBody := []PowerStoreHost{}
		respHeaders := make(map[string]string)

		pageURL := withPaginationQuery(url.URL, offset, limit)
		err := c.requestAuthenticated(ctx, http.MethodGet, pageURL, nil, &respBody, respHeaders)
		if err != nil {
			return nil, fmt.Errorf("Failed retrieving PowerStore hosts: %w", err)
		}

		nextOffset, hasMoreItems, err := parsePaginationOffset(respHeaders)
		if err != nil {
			return nil, fmt.Errorf("Failed retrieving PowerStore hosts: %w", err)
		}

		hosts = append(hosts, respBody...)
		offset = nextOffset

		if !hasMoreItems {
			break
		}
	}

	return hosts, nil
}

// GetCurrentHost retrieves the HPE Alletra Storage host linked to the current LXD host.
// The Alletra Storage host is considered a match if it includes the fully qualified
// name of the LXD host that is determined by the configured mode.
func (c *PowerStoreClient) GetCurrentHost(ctx context.Context, connectorType string, qn string) (*PowerStoreHost, error) {
	filters := map[string]string{}

	hosts, err := c.getHosts(ctx, filters, -1)
	if err != nil {
		return nil, err
	}

	for _, host := range hosts {
		for _, initiator := range host.Initiators {
			if initiator.PortName == qn {
				return &host, nil
			}
		}
	}

	return nil, api.StatusErrorf(http.StatusNotFound, "Host with qualified name %q (%q) not found", qn, connectorType)
}

// GetHostByID retrieves host using its ID.
func (c *PowerStoreClient) GetHostByID(ctx context.Context, id string) (*PowerStoreHost, error) {
	filters := map[string]string{
		"id": "eq." + id,
	}

	hosts, err := c.getHosts(ctx, filters, 1)
	if err != nil {
		return nil, err
	}

	if len(hosts) == 0 {
		return nil, api.StatusErrorf(http.StatusNotFound, "Host with ID %q not found", id)
	}

	return &hosts[0], nil
}

// GetHostByName retrieves host using its name.
func (c *PowerStoreClient) GetHostByName(ctx context.Context, name string) (*PowerStoreHost, error) {
	filters := map[string]string{
		"name": "eq." + name,
	}

	hosts, err := c.getHosts(ctx, filters, 1)
	if err != nil {
		return nil, err
	}

	if len(hosts) == 0 {
		return nil, api.StatusErrorf(http.StatusNotFound, "Host with name %q not found", name)
	}

	return &hosts[0], nil
}

// CreateHost creates new host.
func (c *PowerStoreClient) CreateHost(ctx context.Context, connectorType string, hostname string, qn string) (hostID string, err error) {
	url := api.NewURL().Path("api", "rest", "host")

	req := map[string]any{
		"name": hostname,
	}

	resp := PowerStoreResourceID{}
	err = c.requestAuthenticated(ctx, http.MethodPost, url.URL, req, &resp, nil)
	if err != nil {
		return "", fmt.Errorf("Failed creating PowerStore host: %w", err)
	}

	return resp.ID, nil
}

// DeleteHost deletes host using its ID.
func (c *PowerStoreClient) DeleteHost(ctx context.Context, hostID string) error {
	url := api.NewURL().Path("api", "rest", "host", hostID)

	err := c.requestAuthenticated(ctx, http.MethodDelete, url.URL, nil, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed deleting PowerStore host: %w", err)
	}

	return nil
}

// AddHostInitiator adds initiator to host using its ID.
func (c *PowerStoreClient) AddHostInitiator(ctx context.Context, hostID string, initiator *PowerStoreHostInitiator) error {
	url := api.NewURL().Path("api", "rest", "host", hostID)
	req := map[string]any{
		"add_initiators": []PowerStoreHostInitiator{*initiator},
	}

	err := c.requestAuthenticated(ctx, http.MethodPatch, url.URL, req, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed adding initiator to PowerStore host: %w", err)
	}

	return nil
}

// DeleteHostInitiator removes initiator matching port name from host using its ID.
func (c *PowerStoreClient) DeleteHostInitiator(ctx context.Context, hostID string, initiator *PowerStoreHostInitiator) error {
	url := api.NewURL().Path("api", "rest", "host", hostID)

	req := map[string]any{
		"remove_initiators": []string{initiator.PortName},
	}

	err := c.requestAuthenticated(ctx, http.MethodPatch, url.URL, req, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed removing initiator from PowerStore host: %w", err)
	}

	return nil
}

// AttachHostToVolume attaches (maps) host to volume, returning true if the volume was freshly
// attached to the host, and false if the volume was already attached to the host.
func (c *PowerStoreClient) AttachHostToVolume(ctx context.Context, hostID string, volumeID string) (bool, error) {
	// TODO: Remove DEBUG
	c.logger.Warn("Attaching host to volume", logger.Ctx{"host_id": hostID, "volume_id": volumeID})

	url := api.NewURL().Path("api", "rest", "host", hostID, "attach")

	req := map[string]any{
		"volume_id": volumeID,
	}

	err := c.requestAuthenticated(ctx, http.MethodPost, url.URL, req, nil, nil)
	if err != nil {
		return false, fmt.Errorf("Failed attaching PowerStore host to a volume: %w", err)
	}

	return true, nil
}

// DetachHostFromVolume detaches (unmaps) host from volume.
func (c *PowerStoreClient) DetachHostFromVolume(ctx context.Context, hostID string, volumeID string) error {
	// TODO: Remove DEBUG
	c.logger.Warn("Detaching host from volume", logger.Ctx{"host_id": hostID, "volume_id": volumeID})

	url := api.NewURL().Path("api", "rest", "host", hostID, "detach")

	req := map[string]any{
		"volume_id": volumeID,
	}

	err := c.requestAuthenticated(ctx, http.MethodPost, url.URL, req, nil, nil)
	if err != nil {
		return fmt.Errorf("Detaching PowerStore host from a volume: %w", err)
	}

	return nil
}

func (c *PowerStoreClient) getInitiatorsByQuery(ctx context.Context, queryFilters map[string]string, limit int) ([]PowerStoreInitiator, error) {
	url := api.NewURL().Path("api", "rest", "initiator")
	url = url.WithQuery("select", "id,host_id,port_name,port_type")

	for k, v := range queryFilters {
		url = url.WithQuery(k, v)
	}

	var offset uint64
	initiators := []PowerStoreInitiator{}

	for {
		respBody := []PowerStoreInitiator{}
		respHeaders := make(map[string]string)

		pageURL := withPaginationQuery(url.URL, offset, limit)
		err := c.requestAuthenticated(ctx, http.MethodGet, pageURL, nil, &respBody, respHeaders)
		if err != nil {
			return nil, fmt.Errorf("Failed retrieving PowerStore initiators: %w", err)
		}

		nextOffset, hasMoreItems, err := parsePaginationOffset(respHeaders)
		if err != nil {
			return nil, fmt.Errorf("Failed retrieving PowerStore initiators: %w", err)
		}

		initiators = append(initiators, respBody...)
		offset = nextOffset

		if !hasMoreItems {
			break
		}
	}

	return initiators, nil
}

// GetHostByInitiator retrieves host that have initiator matching port name and type.
func (c *PowerStoreClient) GetHostByInitiator(ctx context.Context, initiator *PowerStoreHostInitiator) (*PowerStoreHost, error) {
	filters := map[string]string{
		"port_name": "eq." + initiator.PortName,
		"port_type": "eq." + string(initiator.Type),
	}

	initiators, err := c.getInitiatorsByQuery(ctx, filters, 1)
	if err != nil {
		return nil, err
	}

	if len(initiators) == 0 {
		return nil, api.StatusErrorf(http.StatusNotFound, "Host with initiator port %q and type %q not found", initiator.PortName, initiator.Type)
	}

	return c.GetHostByID(ctx, initiators[0].HostID)
}

func (c *PowerStoreClient) getVolumes(ctx context.Context, queryFilter map[string]string, limit int) ([]PowerStoreVolume, error) {
	url := api.NewURL().Path("api", "rest", "volume")
	url = url.WithQuery("select", "id,name,description,type,state,size,logical_used,wwn,app_type,app_type_other,volume_groups(id),mapped_volumes(id,host_id,volume_id)")

	for k, v := range queryFilter {
		url = url.WithQuery(k, v)
	}

	var offset uint64
	volumes := []PowerStoreVolume{}

	for {
		respBody := []PowerStoreVolume{}
		respHeaders := make(map[string]string)

		pageURL := withPaginationQuery(url.URL, offset, limit)
		err := c.requestAuthenticated(ctx, http.MethodGet, pageURL, nil, &respBody, respHeaders)
		if err != nil {
			return nil, err
		}

		volumes = append(volumes, respBody...)
		if limit > 0 && len(volumes) >= limit {
			break
		}

		nextOffset, hasMoreItems, err := parsePaginationOffset(respHeaders)
		if err != nil {
			return nil, err
		}

		offset = nextOffset

		if !hasMoreItems {
			break
		}
	}

	return volumes, nil
}

// GetVolumes retrieves list of volume associated with the storage pool.
func (c *PowerStoreClient) GetVolumes(ctx context.Context) ([]PowerStoreVolume, error) {
	filter := map[string]string{
		"name": "ilike." + c.volumeNamePrefix + "*",
		"or":   "(type.eq.Primary,type.eq.Clone)",
	}

	vols, err := c.getVolumes(ctx, filter, -1)
	if err != nil {
		return nil, fmt.Errorf("Failed retrieving PowerStore volumes: %w", err)
	}

	return vols, nil
}

// GetVolumeByID retrieves volume using its ID.
func (c *PowerStoreClient) GetVolumeByID(ctx context.Context, id string) (*PowerStoreVolume, error) {
	filter := map[string]string{
		"id": "eq." + id,
		"or": "(type.eq.Primary,type.eq.Clone)",
	}

	vols, err := c.getVolumes(ctx, filter, 1)
	if err != nil {
		return nil, fmt.Errorf("Failed retrieving PowerStore volume with ID %q: %w", id, err)
	}

	if len(vols) == 0 {
		return nil, api.StatusErrorf(http.StatusNotFound, "Volume with ID %q not found", id)
	}

	return &vols[0], nil
}

// GetVolumeByName retrieves volume using its name.
func (c *PowerStoreClient) GetVolumeByName(ctx context.Context, name string) (*PowerStoreVolume, error) {
	filter := map[string]string{
		"name": "eq." + name,
		"or":   "(type.eq.Primary,type.eq.Clone)",
	}

	vols, err := c.getVolumes(ctx, filter, 1)
	if err != nil {
		return nil, fmt.Errorf("Failed retrieving PowerStore volume with name %q: %w", name, err)
	}

	if len(vols) == 0 {
		return nil, api.StatusErrorf(http.StatusNotFound, "Volume with name %q not found", name)
	}

	return &vols[0], nil
}

// GetVolumeID returns the volume ID for the given name.
func (c *PowerStoreClient) GetVolumeID(ctx context.Context, volumeName string) (volumeID string, err error) {
	vol, err := c.GetVolumeByName(ctx, volumeName)
	if err != nil {
		return "", err
	}

	return vol.ID, nil
}

// CreateVolume creates a new volume.
func (c *PowerStoreClient) CreateVolume(ctx context.Context, volumeName string, sizeBytes int64) (id string, err error) {
	url := api.NewURL().Path("api", "rest", "volume")

	req := map[string]any{
		"name":        volumeName,
		"description": "LXD Volume: " + volumeName,
		"size":        sizeBytes,
	}

	var resp PowerStoreResourceID
	err = c.requestAuthenticated(ctx, http.MethodPost, url.URL, req, &resp, nil)
	if err != nil {
		return "", fmt.Errorf("Failed creating PowerStore volume: %w", err)
	}

	return resp.ID, nil
}

// DeleteVolume deletes volume using its ID.
func (c *PowerStoreClient) DeleteVolume(ctx context.Context, volumeID string) error {
	url := api.NewURL().Path("api", "rest", "volume", volumeID)

	err := c.requestAuthenticated(ctx, http.MethodDelete, url.URL, nil, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed deleting PowerStore volume: %w", err)
	}

	return nil
}

// ResizeVolume creates a new volume.
func (c *PowerStoreClient) ResizeVolume(ctx context.Context, volumeID string, newSize int64) error {
	url := api.NewURL().Path("api", "rest", "volume", volumeID)

	req := map[string]any{
		"size": newSize,
	}

	err := c.requestAuthenticated(ctx, http.MethodPatch, url.URL, req, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed resizing PowerStore volume: %w", err)
	}

	return nil
}

// CloneVolume clones the volume or the volume snapshot with the provided ID to a new volume.
func (c *PowerStoreClient) CloneVolume(ctx context.Context, srcVolumeID string, dstVolumeName string) (dstVolumeID string, err error) {
	url := api.NewURL().Path("api", "rest", "volume", srcVolumeID, "clone")

	req := map[string]any{
		"name":        dstVolumeName,
		"description": `LXD Volume Clone from "` + dstVolumeName + `"`,
	}

	var resp PowerStoreResourceID
	err = c.requestAuthenticated(ctx, http.MethodPost, url.URL, req, &resp, nil)
	if err != nil {
		return "", fmt.Errorf("Failed cloning PowerStore volume: %w", err)
	}

	return resp.ID, nil
}

// RestoreVolume restores the volume form the volume snapshot.
func (c *PowerStoreClient) RestoreVolume(ctx context.Context, srcVolumeSnapshotID string, dstVolumeID string) error {
	url := api.NewURL().Path("api", "rest", "volume", dstVolumeID, "restore")

	req := map[string]any{
		"from_snap_id": srcVolumeSnapshotID,
	}

	err := c.requestAuthenticated(ctx, http.MethodPost, url.URL, req, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed restoring PowerStore volume from snapshot: %w", err)
	}

	return nil
}

// RefreshVolume refreshes the volume form the volume or the volume snapshot.
func (c *PowerStoreClient) RefreshVolume(ctx context.Context, srcVolumeID string, dstVolumeID string) error {
	url := api.NewURL().Path("api", "rest", "volume", dstVolumeID, "refresh")

	req := map[string]any{
		"from_object_id": srcVolumeID,
	}

	err := c.requestAuthenticated(ctx, http.MethodPost, url.URL, req, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed refreshing PowerStore volume: %w", err)
	}

	return nil
}

// GetVolumeSnapshots retrieves list of volume snapshots associated with the provided volume.
func (c *PowerStoreClient) GetVolumeSnapshots(ctx context.Context, volumeID string) ([]PowerStoreVolume, error) {
	filter := map[string]string{
		// "name": "ilike." + c.volumeNamePrefix + "*", // TODO: We need to filter LXD snapshots.
		"type":                        "eq.Snapshot",
		"protection_data->>parent_id": "eq." + volumeID,
	}

	snapshots, err := c.getVolumes(ctx, filter, -1)
	if err != nil {
		return nil, fmt.Errorf("Failed retrieving PowerStore volume snapshots: %w", err)
	}

	return snapshots, nil
}

// GetVolumeSnapshotByID retrieves volume snapshot using its ID.
func (c *PowerStoreClient) GetVolumeSnapshotByID(ctx context.Context, snapshotID string) (*PowerStoreVolume, error) {
	filter := map[string]string{
		// "protection_data->>parent_id": "eq." + volumeID, // TODO: Should we search snapshot for a specific parent as well?
		"type": "eq.Snapshot",
		"id":   "eq." + snapshotID,
	}

	snapshots, err := c.getVolumes(ctx, filter, 1)
	if err != nil {
		return nil, fmt.Errorf("Failed retrieving PowerStore volume snapshot with ID %q: %w", snapshotID, err)
	}

	if len(snapshots) == 0 {
		return nil, api.StatusErrorf(http.StatusNotFound, "Volume snapshot with ID %q not found", snapshotID)
	}

	return &snapshots[0], nil
}

// GetVolumeSnapshotByName retrieves volume snapshot using its name.
func (c *PowerStoreClient) GetVolumeSnapshotByName(ctx context.Context, name string) (*PowerStoreVolume, error) {
	filter := map[string]string{
		// "protection_data->>parent_id": "eq." + volumeID, // TODO: Should we search snapshot for a specific parent as well?
		"type": "eq.Snapshot",
		"name": "eq." + name,
	}

	snapshots, err := c.getVolumes(ctx, filter, 1)
	if err != nil {
		return nil, fmt.Errorf("Failed retrieving PowerStore volume snapshot with name %q: %w", name, err)
	}

	if len(snapshots) == 0 {
		return nil, api.StatusErrorf(http.StatusNotFound, "Volume snapshot with name %q not found", name)
	}

	return &snapshots[0], nil
}

// CreateVolumeSnapshot creates a new snapshot of a volume.
func (c *PowerStoreClient) CreateVolumeSnapshot(ctx context.Context, volumeID string, snapshotName string) (snapshotID string, err error) {
	url := api.NewURL().Path("api", "rest", "volume", volumeID, "snapshot")

	req := map[string]any{
		"name":        snapshotName,
		"description": "LXD Volume Snapshot of " + snapshotName,
	}

	var resp PowerStoreResourceID

	err = c.requestAuthenticated(ctx, http.MethodPost, url.URL, req, &resp, nil)
	if err != nil {
		return "", fmt.Errorf("Failed creating PowerStore volume snapshot with name %q: %w", snapshotName, err)
	}

	return resp.ID, nil
}

// DeleteVolumeSnapshot deletes a snapshot of a volume.
func (c *PowerStoreClient) DeleteVolumeSnapshot(ctx context.Context, volumeID string, snapshotID string) error {
	url := api.NewURL().Path("api", "rest", "volume", volumeID, "snapshot", snapshotID)

	err := c.requestAuthenticated(ctx, http.MethodDelete, url.URL, nil, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed deleting PowerStore volume snapshot with ID %q: %w", snapshotID, err)
	}

	return nil
}

// RemoveVolumeGroupMembers removes volumes from the volume group.
func (c *PowerStoreClient) RemoveVolumeGroupMembers(ctx context.Context, volumeGroupID string, volumeIDs []string) error {
	url := api.NewURL().Path("api", "rest", "volume_group", volumeGroupID, "remove_members")

	req := map[string]any{
		"volume_ids": volumeIDs,
	}

	err := c.requestAuthenticated(ctx, http.MethodPost, url.URL, req, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed removing members from PowerStore volume group: %w", err)
	}

	return nil
}
