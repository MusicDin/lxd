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
	"maps"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/canonical/lxd/lxd/storage/connectors"
	"github.com/canonical/lxd/lxd/storage/drivers/tokencache"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/logger"
)

var powerStoreSessions = make(map[string]powerStoreSession)
var powerStoreSessionsLock = &sync.RWMutex{}

// SessionKey() generates a hashtable key for a session.
func (p *PowerStoreClient) SessionKey() string {
	return p.url + p.username + p.password
}

// SessionKey() gets a session key from the cache.
func (p *PowerStoreClient) Session() *powerStoreSession {
	if p.session != nil {
		return p.session
	}

	key := p.SessionKey()

	powerStoreSessionsLock.RLock()
	defer powerStoreSessionsLock.RUnlock()

	session, ok := powerStoreSessions[key]
	if ok {
		s := session
		p.session = &s
		return p.session
	}

	return nil
}

// SetSession() adds a session key to the cache.
func (p *PowerStoreClient) SetSession(session powerStoreSession) {
	key := p.SessionKey()

	powerStoreSessionsLock.Lock()
	defer powerStoreSessionsLock.Unlock()

	powerStoreSessions[key] = session
	p.session = &session
}

// InvalidateSession() removes a session key from the cache.
func (p *PowerStoreClient) InvalidateSession() {
	key := p.SessionKey()

	powerStoreSessionsLock.Lock()
	defer powerStoreSessionsLock.Unlock()

	delete(powerStoreSessions, key)
	p.session = nil
}

// powerStoreTokenCache stores shared PowerStore login sessions.
var powerStoreTokenCache = tokencache.New[powerStoreSession]("powerstore")

const (
	powerStoreAuthCookieName = "auth_cookie"
	powerStoreCSRFHeaderName = "DELL-EMC-TOKEN"

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

// // pagination encapsulates query request pagination data.
// type pagination struct {
// 	Page         int
// 	ItemsPerPage int
// }

// // Offset computes offset value for the provided pagination state.
// func (p pagination) Offset() int {
// 	page := max(0, p.Page)
// 	limit := p.Limit()
// 	return page * limit
// }

// // Limit computes limit value for the provided pagination state.
// func (p pagination) Limit() int {
// 	return min(max(0, p.ItemsPerPage), PowerStoreQueryResponseLimit)
// }

// // query is a container for PowerStore query request parameters.
// type query map[string]string

// // Clone clones the provided query. If the query is nil it returns
// // an initialized empty query.
// func (q query) Clone() query {
// 	if q == nil {
// 		return query{}
// 	}

// 	return maps.Clone(q)
// }

// // Set sets the provided value under the specified key returning the new query.
// func (q query) Set(key, val string) query {
// 	q = q.Clone()
// 	q[key] = val
// 	return q
// }

// // Paginate adds pagination parameters returning a new query.
// func (q query) Paginate(pagination pagination) query {
// 	q = q.Clone()
// 	q["offset"] = strconv.Itoa(pagination.Offset())
// 	q["limit"] = strconv.Itoa(pagination.Limit())
// 	return q
// }

// // URLParameters transforms query into URL parameters.
// func (q query) URLParameters() url.Values {
// 	params := url.Values{}
// 	for key, val := range q {
// 		params.Set(key, val)
// 	}

// 	return params
// }

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
	MappedHosts      []*PowerStoreHostVolumeMapping `json:"mapped_hosts,omitempty"`
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

// request issues a HTTP request against the PowerStore gateway.
func (p *PowerStoreClient) request(ctx context.Context, method string, url url.URL, reqBody map[string]any, reqHeaders map[string]string, respBody any, respHeaders map[string]string) error {
	gw := p.url
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
				InsecureSkipVerify: p.skipTLSVerify,
			},
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("Failed sending request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
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

	// Parse CSRF token from response headers.
	csrf := respHeaders[powerStoreCSRFHeaderName]
	if csrf == "" {
		return nil, errors.New("Failed logging into PowerStore: Login response missing CSRF token")
	}

	csrfDuration := time.Duration(sessionInfo.IdleTimeout) * time.Second

	// Parse auth cookie.
	resp := &http.Response{Header: http.Header{}}
	for k, v := range respHeaders {
		resp.Header[k] = []string{v}
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

// requestAuthenticated issues an authenticated HTTP request against the PowerStore gateway.
// In case the access token is expired, the function will try to obtain a new one.
func (p *PowerStoreClient) requestAuthenticated(ctx context.Context, method string, url url.URL, reqBody map[string]any, respBody any, respHeaders map[string]string) error {
	// If request fails with an unauthorized error, the request will be retried after
	// requesting a new access token.
	retries := 1

	for {
		// Ensure we are logged into the Pure Storage.
		session, err := p.login(ctx)
		if err != nil {
			return err
		}

		// Set access token as request header.
		reqHeaders := map[string]string{
			"Cookie":                 powerStoreAuthCookieName + "=" + session.AuthToken,
			powerStoreCSRFHeaderName: session.CSRFToken,
		}

		// Initiate request.
		err = p.request(ctx, method, url, reqBody, reqHeaders, respBody, respHeaders)
		if err != nil {
			if api.StatusErrorCheck(err, http.StatusUnauthorized) && retries > 0 {
				// The failure seems to be due to an expired session.
				// Invalidate session and try again.
				p.InvalidateSession()
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

// func (c *PowerStoreClient) doAuthenticatedHTTPRequest(ctx context.Context, method string, path string, requestData, responseData any, requestEditors ...func(*http.Request) error) (*http.Response, error) {
// 	session, err := c.getOrCreateLoginSession(ctx, sessionKey)
// 	if err != nil {
// 		return nil, err
// 	}

// 	requestEditors = append([]func(*http.Request) error{c.withLoginSession(session)}, requestEditors...)
// 	resp, err := c.doUnauthenticatedHTTPRequest(ctx, method, path, requestData, responseData, requestEditors...)
// 	if resp != nil && resp.StatusCode == http.StatusUnauthorized {
// 		// there is something wrong with the session token, remove it
// 		c.forceLoginSessionRemoval(sessionKey, session)
// 	}

// 	return resp, err
// }

// func (c *PowerStoreClient) doUnauthenticatedHTTPRequest(ctx context.Context, method string, path string, requestData, responseData any, requestEditors ...func(*http.Request) error) (*http.Response, error) {
// 	body, err := c.marshalHTTPRequestBody(requestData)
// 	if err != nil {
// 		return nil, fmt.Errorf("Marshal HTTP request body: %s: %w", path, err)
// 	}

// 	url := c.url + path
// 	req, err := http.NewRequestWithContext(ctx, method, url, body)
// 	if err != nil {
// 		return nil, fmt.Errorf("Create request: %w", err)
// 	}

// 	req.Header.Add("Accept", "application/json")
// 	if body != nil {
// 		req.Header.Add("Content-Type", "application/json")
// 	}

// 	for _, edit := range requestEditors {
// 		err := edit(req)
// 		if err != nil {
// 			return nil, err
// 		}
// 	}

// 	client := &http.Client{
// 		Transport: &http.Transport{
// 			TLSClientConfig: &tls.Config{
// 				InsecureSkipVerify: c.skipTLSVerify,
// 			},
// 		},
// 	}

// 	resp, err := client.Do(req)
// 	if err != nil {
// 		return nil, fmt.Errorf("Send request: %w", err)
// 	}

// 	defer resp.Body.Close()

// 	if resp.StatusCode > 299 {
// 		return resp, newPowerStoreError(resp)
// 	}

// 	if responseData != nil {
// 		err := json.NewDecoder(resp.Body).Decode(responseData)
// 		if err != nil {
// 			return resp, fmt.Errorf("Unmarshal HTTP response body: %s: %w", path, err)
// 		}
// 	}
// 	return resp, nil
// }

// func (c *PowerStoreClient) withBasicAuthorization(username, password string) func(req *http.Request) error {
// 	return func(req *http.Request) error {
// 		token := base64.StdEncoding.EncodeToString(fmt.Appendf(nil, "%s:%s", username, password))
// 		req.Header.Set("Authorization", "Basic "+token)
// 		return nil
// 	}
// }

// func (c *PowerStoreClient) withLoginSession(ls *powerStoreSession) func(req *http.Request) error {
// 	return func(req *http.Request) error {
// 		req.Header.Add("Cookie", fmt.Sprintf("%s=%s", powerStoreAuthCookieName, ls.AuthToken))
// 		req.Header.Set(powerStoreCSRFHeaderName, ls.CSRFToken)
// 		return nil
// 	}
// }

// func (c *PowerStoreClient) withQuery(query query) func(req *http.Request) error {
// 	return func(req *http.Request) error {
// 		if len(query) == 0 {
// 			req.URL.RawQuery = ""
// 			return nil
// 		}

// 		req.URL.RawQuery = query.URLParameters().Encode()
// 		return nil
// 	}
// }

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

// GetCurrentHost retrieves the HPE Alletra Storage host linked to the current LXD host.
// The Alletra Storage host is considered a match if it includes the fully qualified
// name of the LXD host that is determined by the configured mode.
func (p *PowerStoreClient) GetCurrentHost(connectorType string, qn string) (*PowerStoreHost, error) {
	hosts, hasMore, err := p.getHosts(context.TODO())
	if err != nil {
		return nil, err
	}

	if hasMore {
		p.logger.Warn("Not all hosts have been retrieved!")
	}

	for _, host := range hosts {
		if connectorType == connectors.TypeISCSI {
			for _, iscsiPath := range host.ISCSIPaths {
				if iscsiPath.Name == qn {
					return &host, nil
				}
			}
		}

		if connectorType == connectors.TypeNVME {
			for _, nvmePath := range host.NVMETCPPaths {
				if nvmePath.NQN == qn {
					return &host, nil
				}
			}
		}
	}

	return nil, api.StatusErrorf(http.StatusNotFound, "Host with qualified name %q not found", qn)
}

func (c *PowerStoreClient) getHosts(ctx context.Context, queryFilters map[string]string) ([]PowerStoreHost, error) {
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

		pageURL := withPaginationQuery(url.URL, offset, -1)
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

// GetHostByID retrieves host using its ID.
func (c *PowerStoreClient) GetHostByID(ctx context.Context, id string) (*PowerStoreHost, error) {
	hosts, err := c.getHosts(ctx, map[string]string{"id": "eq." + id})
	if err != nil {
		return nil, err
	}

	switch len(hosts) {
	case 0:
		return nil, api.StatusErrorf(http.StatusNotFound, "Host with ID %q not found", id)
	case 1:
		return &hosts[0], nil
	default:
		return nil, api.StatusErrorf(http.StatusInternalServerError, "Multiple hosts with ID %q found", id)
	}
}

// GetHostByName retrieves host using its name.
func (c *PowerStoreClient) GetHostByName(ctx context.Context, name string) (*PowerStoreHost, error) {
	hosts, err := c.getHosts(ctx, map[string]string{"name": "eq." + name})
	if err != nil {
		return nil, err
	}

	switch len(hosts) {
	case 0:
		return nil, api.StatusErrorf(http.StatusNotFound, "Host with name %q not found", name)
	case 1:
		return &hosts[0], nil
	default:
		return nil, api.StatusErrorf(http.StatusInternalServerError, "Multiple hosts with name %q found", name)
	}
}

// CreateHost creates new host.
func (c *PowerStoreClient) CreateHost(ctx context.Context, hostname string, qn string) (id string, err error) {
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

// DeleteHostByID deletes host using its ID.
func (c *PowerStoreClient) DeleteHostByID(ctx context.Context, id string) error {
	url := api.NewURL().Path("api", "rest", "host", id)

	err := c.requestAuthenticated(ctx, http.MethodDelete, url.URL, nil, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed deleting PowerStore host: %w", err)
	}

	return nil
}

// AddInitiatorToHostByID adds initiator to host using its ID.
func (c *PowerStoreClient) AddInitiatorToHostByID(ctx context.Context, hostID string, initiator *PowerStoreHostInitiator) error {
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

// RemoveInitiatorFromHostByID removes initiator matching port name from host using its ID.
func (c *PowerStoreClient) RemoveInitiatorFromHostByID(ctx context.Context, hostID string, initiator *PowerStoreHostInitiator) error {
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

// AttachHostToVolume attaches (maps) host to volume.
func (c *PowerStoreClient) AttachHostToVolume(ctx context.Context, hostID, volID string) error {
	url := api.NewURL().Path("api", "rest", "host", hostID, "attach")

	req := map[string]any{
		"volume_id": volID,
	}

	err := c.requestAuthenticated(ctx, http.MethodPost, url.URL, req, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed attaching PowerStore host to a volume: %w", err)
	}

	return nil
}

// DetachHostFromVolume detaches (unmaps) host from volume.
func (c *PowerStoreClient) DetachHostFromVolume(ctx context.Context, hostID, volID string) error {
	url := api.NewURL().Path("api", "rest", "host", hostID, "detach")

	req := map[string]any{
		"volume_id": volID,
	}

	err := c.requestAuthenticated(ctx, http.MethodPost, url.URL, req, nil, nil)
	if err != nil {
		return fmt.Errorf("Detaching PowerStore host from a volume: %w", err)
	}

	return nil
}

func (c *PowerStoreClient) getInitiatorsByQuery(ctx context.Context, queryFilters map[string]string) ([]PowerStoreInitiator, error) {
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

		pageURL := withPaginationQuery(url.URL, offset, -1)
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
	url := api.NewURL().Path("api", "rest", "initiator")
	url = url.WithQuery("port_name", "eq."+initiator.PortName)
	url = url.WithQuery("port_type", "eq."+string(initiator.Type))

	// Parse up to 2 initiators to ensure there are no duplicates.
	initiators := []PowerStoreInitiator{}
	err := c.requestAuthenticated(ctx, http.MethodGet, withPaginationQuery(url.URL, 0, 2), nil, &initiators, nil)
	if err != nil {
		return nil, fmt.Errorf("Failed retrieving PowerStore initiators: %w", err)
	}

	switch len(initiators) {
	case 0:
		return nil, api.StatusErrorf(http.StatusNotFound, "Host with initiator port %q and type %q not found", initiator.PortName, initiator.Type)
	case 1:
		return c.GetHostByID(ctx, initiators[0].HostID)
	default:
		return nil, api.StatusErrorf(http.StatusInternalServerError, "Multiple initiators with port %q and type %q found", initiator.PortName, initiator.Type)
	}
}

// GetVolumeID returns the volume ID for the given name.
func (p *PowerStoreClient) GetVolumeID(name string) (string, error) {
	body := map[string]any{
		"name": name,
	}

	var actualResponse string
	err := p.requestAuthenticated(http.MethodPost, "/api/types/Volume/instances/action/queryIdByKey", body, &actualResponse)
	if err != nil {
		powerFlexError, ok := err.(*powerFlexError)
		if ok {
			// API returns 500 if the volume does not exist.
			// To not confuse it with other 500 that might occur check the error code too.
			if powerFlexError.HTTPStatusCode() == http.StatusInternalServerError && powerFlexError.ErrorCode() == powerFlexCodeVolumeNotFound {
				return "", api.StatusErrorf(http.StatusNotFound, "PowerFlex volume not found: %q", name)
			}
		}

		return "", fmt.Errorf("Failed getting volume ID: %q: %w", name, err)
	}

	return actualResponse, nil
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

// DeleteVolumeByID deletes volume using its ID.
func (c *PowerStoreClient) DeleteVolumeByID(ctx context.Context, volumeID string) error {
	url := api.NewURL().Path("api", "rest", "volume", volumeID)

	err := c.requestAuthenticated(ctx, http.MethodDelete, url.URL, nil, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed deleting PowerStore volume: %w", err)
	}

	return nil
}

// ResizeVolumeByID creates a new volume.
func (c *PowerStoreClient) ResizeVolume(ctx context.Context, id string, newSize int64) error {
	url := api.NewURL().Path("api", "rest", "volume", id)

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
func (c *PowerStoreClient) CloneVolume(ctx context.Context, srcVolID string, dstVolName string) (newVolID string, err error) {
	url := api.NewURL().Path("api", "rest", "volume", srcVolID, "clone")

	req := map[string]any{
		"name":        dstVolName,
		"description": `LXD Volume Clone from "` + dstVolName + `"`,
	}

	var resp PowerStoreResourceID
	err = c.requestAuthenticated(ctx, http.MethodPost, url.URL, req, &resp, nil)
	if err != nil {
		return "", fmt.Errorf("Failed cloning PowerStore volume: %w", err)
	}

	return resp.ID, nil
}

// RestoreVolume restores the volume form the volume snapshot.
func (c *PowerStoreClient) RestoreVolume(ctx context.Context, srcVolSnapID string, dstVolID string) error {
	url := api.NewURL().Path("api", "rest", "volume", dstVolID, "restore")

	req := map[string]any{
		"from_snap_id": srcVolSnapID,
	}

	err := c.requestAuthenticated(ctx, http.MethodPost, url.URL, req, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed restoring PowerStore volume from snapshot: %w", err)
	}

	return nil
}

// RefreshVolume refreshes the volume form the volume or the volume snapshot.
func (c *PowerStoreClient) RefreshVolume(ctx context.Context, srcVolID, dstVolID string) error {
	type refreshVolumeRequest struct {
		FromObjectID         string `json:"from_object_id"`
		CreateBackupSnapshot bool   `json:"create_backup_snap"`
	}

	reqBody := &refreshVolumeRequest{FromObjectID: srcVolID}
	_, err := c.doAuthenticatedHTTPRequest(ctx, http.MethodPost, "/api/rest/volume/"+dstVolID+"/refresh", reqBody, nil)
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
func (c *PowerStoreClient) GetVolumeSnapshotByID(ctx context.Context, id string) (*PowerStoreVolume, error) {
	filter := map[string]string{
		// "protection_data->>parent_id": "eq." + volumeID, // TODO: Should we search snapshot for a specific parent as well?
		"type": "eq.Snapshot",
		"id":   "eq." + id,
	}

	snapshots, err := c.getVolumes(ctx, filter, 1)
	if err != nil {
		return nil, fmt.Errorf("Failed retrieving PowerStore volume snapshot with ID %q: %w", id, err)
	}

	if len(snapshots) == 0 {
		return nil, api.StatusErrorf(http.StatusNotFound, "Volume snapshot with ID %q not found", id)
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
func (c *PowerStoreClient) CreateVolumeSnapshot(ctx context.Context, volumeID string, snapshotName string) (*PowerStoreVolume, error) {
	req := map[string]any{
		"name":        snapshotName,
		"description": "LXD Volume Snapshot",
	}

	body := &PowerStoreResourceID{}
	reqBody := &createVolumeSnapshotRequest{Name: name, Description: description}
	_, err := c.doAuthenticatedHTTPRequest(ctx, http.MethodPost, "/api/rest/volume/"+volumeID+"/snapshot", reqBody, body)
	if err != nil {
		return nil, fmt.Errorf("Failed creating PowerStore volume snapshot: %w", err)
	}

	// Fetch volume snapshot to populate all fields.
	created, err := c.GetVolumeSnapshotByID(ctx, body.ID)
	if err != nil {
		return nil, fmt.Errorf("Failed creating PowerStore volume snapshot: %w", err)
	}

	if created == nil {
		return nil, errors.New("Failed creating PowerStore volume snapshot: No data of new volume snapshot found")
	}

	return created, nil
}

// RemoveMembersFromVolumeGroup removes volumes from the volume group.
func (c *PowerStoreClient) RemoveMembersFromVolumeGroup(ctx context.Context, id string, volumeIDs []string) error {
	url := api.NewURL().Path("api", "rest", "volume_group", id, "remove_members")

	req := map[string]any{
		"volume_ids": volumeIDs,
	}

	err := c.requestAuthenticated(ctx, http.MethodPost, url.URL, req, nil, nil)
	if err != nil {
		return fmt.Errorf("Failed removing members from PowerStore volume group: %w", err)
	}

	return nil
}
