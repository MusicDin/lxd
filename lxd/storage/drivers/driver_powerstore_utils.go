package drivers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/canonical/lxd/lxd/storage/block"
	"github.com/canonical/lxd/lxd/storage/connectors"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/revert"
	"github.com/canonical/lxd/shared/units"
)

const (
	powerStoreAuthorizationCookieName = "auth_cookie"
	powerStoreCSRFHeaderName          = "DELL-EMC-TOKEN"
)

// powerStoreTokenCache stores shared PowerStore login sessions.
var powerStoreTokenCache = newTokenCache[powerStoreLoginSession]("powerstore")

// powerStoreLoginSession describes PowerStore login session.
type powerStoreLoginSession struct {
	ID              string
	IdleTimeout     time.Duration
	LastInteraction atomic.Pointer[time.Time]
	AuthToken       string
	CSRFToken       string
}

func newPowerStoreLoginSession(id string, idleTimeout time.Duration, authToken, csrfToken string) *powerStoreLoginSession {
	ls := &powerStoreLoginSession{
		ID:          id,
		IdleTimeout: idleTimeout - 30*time.Second, // subtract to add safety margin for a potential time skew
		AuthToken:   authToken,
		CSRFToken:   csrfToken,
	}

	ls.Interacted()
	return ls
}

// IsValid inform if the token associated with the login session is not
// expired.
func (ls *powerStoreLoginSession) IsValid() bool {
	if ls == nil {
		return false
	}

	lastInteraction := *ls.LastInteraction.Load()
	return time.Now().Before(lastInteraction.Add(ls.IdleTimeout))
}

// Interacted informs the login session object that interaction occurred and
// last interaction time should be updated.
func (ls *powerStoreLoginSession) Interacted() {
	now := time.Now()
	ls.LastInteraction.Store(&now)
}

// makePowerStoreFingerprint creates a fingerprint the the provided strings
// uniquely identifying them.
func makePowerStoreFingerprint(pieces ...string) string {
	raw := bytes.Buffer{}
	// Use base64 on the provided strings and separate pieces with ':' to make
	// sure fingerprint is unique regardless of strings content.
	for i, p := range pieces {
		_, _ = raw.WriteString(base64.StdEncoding.EncodeToString([]byte(p)))
		if i < len(pieces)-1 {
			raw.WriteByte(':')
		}
	}

	// Hash the concatenated data to shorten the resulting fingerprint.
	hash := sha256.Sum256(raw.Bytes())
	return base64.StdEncoding.EncodeToString(hash[:])
}

// powerStoreSprintfLimit acts just like fmt.Sprintf, but trims the output to
// the specified number of characters.
func powerStoreSprintfLimit(limit int, format string, args ...any) string {
	x := fmt.Sprintf(format, args...)
	if len(x) > limit {
		x = x[:limit]
	}

	return x
}

// powerStoreError contains arbitrary error responses from PowerStore.
type powerStoreError struct {
	httpStatusCode int
	details        powerStoreErrorResponseResource
	decoderErr     error
}

func newPowerStoreError(resp *http.Response) error {
	if resp.StatusCode == http.StatusUnauthorized {
		return api.NewStatusError(http.StatusUnauthorized, "Unauthorized request")
	}

	e := &powerStoreError{httpStatusCode: resp.StatusCode}
	if resp.Header.Get("Content-Type") != "application/json" || resp.Header.Get("Content-Length") == "0" {
		return e
	}

	err := json.NewDecoder(resp.Body).Decode(&e.details)
	if err != nil {
		e.decoderErr = fmt.Errorf("Unmarshal HTTP error response body: %w", err)
	}

	return e
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

// ErrorCode attempts to extract the error code value from a PowerStore
// response.
func (e *powerStoreError) ErrorCode() string {
	for _, em := range e.details.Messages {
		if em != nil && em.Code != "" {
			return em.Code
		}
	}
	return ""
}

// HTTPStatusCode attempts to extract the HTTP status code value from
// a PowerStore response.
func (e *powerStoreError) HTTPStatusCode() int {
	return e.httpStatusCode
}

type powerStoreErrorResponseResource struct {
	Messages []*powerStoreErrorMessageResource `json:"messages,omitempty"`
}

type powerStoreErrorMessageResource struct {
	Severity    string                                    `json:"severity"`
	Code        string                                    `json:"code"`
	MessageL10n string                                    `json:"message_l10n"`
	Arguments   []*powerStoreErrorMessageArgumentResource `json:"arguments,omitempty"`
}

type powerStoreErrorMessageArgumentResource struct {
	Delimiter string                             `json:"delimiter,omitempty"`
	Messages  []*powerStoreErrorInstanceResource `json:"messages,omitempty"`
}

type powerStoreErrorInstanceResource struct {
	Severity    string   `json:"severity"`
	Code        string   `json:"code"`
	MessageL10n string   `json:"message_l10n"`
	Arguments   []string `json:"arguments,omitempty"`
}

// powerStoreClient holds the PowerStore HTTP API client.
type powerStoreClient struct {
	gateway                  string
	gatewaySkipTLSVerify     bool
	username                 string
	password                 string
	volumeResourceNamePrefix string
	hostResourceNamePrefix   string
}

// newPowerStoreClient creates a new instance of the PowerStore HTTP API
// client.
func newPowerStoreClient(driver *powerstore) *powerStoreClient {
	return &powerStoreClient{
		gateway:                  driver.config["powerstore.gateway"],
		gatewaySkipTLSVerify:     shared.IsFalse(driver.config["powerstore.gateway.verify"]),
		username:                 driver.config["powerstore.user.name"],
		password:                 driver.config["powerstore.user.password"],
		volumeResourceNamePrefix: driver.volumeResourceNamePrefix(),
		hostResourceNamePrefix:   powerStoreResourceNamePrefix,
	}
}

func (c *powerStoreClient) marshalHTTPRequestBody(src any) (io.Reader, error) {
	if src == nil {
		return nil, nil
	}

	dst := &bytes.Buffer{}
	err := json.NewEncoder(dst).Encode(src)
	if err != nil {
		return nil, err
	}

	return dst, nil
}

func (c *powerStoreClient) doHTTPRequest(ctx context.Context, method string, path string, requestData, responseData any, requestEditors ...func(*http.Request) error) (*http.Response, error) {
	body, err := c.marshalHTTPRequestBody(requestData)
	if err != nil {
		return nil, fmt.Errorf("Marshal HTTP request body: %s: %w", path, err)
	}

	url := c.gateway + path
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, fmt.Errorf("Create request: %w", err)
	}

	req.Header.Add("Accept", "application/json")
	if body != nil {
		req.Header.Add("Content-Type", "application/json")
	}

	for _, edit := range requestEditors {
		err := edit(req)
		if err != nil {
			return nil, err
		}
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: c.gatewaySkipTLSVerify,
			},
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Send request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode > 299 {
		return resp, newPowerStoreError(resp)
	}

	if responseData != nil {
		err := json.NewDecoder(resp.Body).Decode(responseData)
		if err != nil {
			return resp, fmt.Errorf("Unmarshal HTTP response body: %s: %w", path, err)
		}
	}
	return resp, nil
}

func (c *powerStoreClient) startNewLoginSession(ctx context.Context) (*powerStoreLoginSession, error) {
	resp, info, err := c.getLoginSessionInfoWithBasicAuthorization(ctx)
	if err != nil {
		return nil, fmt.Errorf("Starting PowerStore session: %w", err)
	}

	if len(info) < 1 {
		return nil, errors.New("Starting PowerStore session: Invalid session information")
	}

	sessionInfo := info[0]

	if sessionInfo.IsPasswordChangeRequired {
		return nil, errors.New("Starting PowerStore session: Password change required")
	}

	var authCookie *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name != powerStoreAuthorizationCookieName {
			continue
		}

		authCookie = c
		break
	}

	if authCookie == nil {
		return nil, errors.New("Starting PowerStore session: Missing PowerStore authorization cookie")
	}

	csrf := resp.Header.Get(powerStoreCSRFHeaderName)
	if csrf == "" {
		return nil, errors.New("Starting PowerStore session: Missing PowerStore CSRF token")
	}

	return newPowerStoreLoginSession(sessionInfo.ID, time.Duration(sessionInfo.IdleTimeout)*time.Second, authCookie.Value, csrf), nil
}

func (c *powerStoreClient) getOrCreateLoginSession(ctx context.Context, sessionKey string) (*powerStoreLoginSession, error) {
	session := powerStoreTokenCache.Load(sessionKey)
	if session.IsValid() {
		return session, nil
	}

	return powerStoreTokenCache.Replace(sessionKey, func(ls *powerStoreLoginSession) (*powerStoreLoginSession, error) {
		if ls != session && ls.IsValid() {
			return ls, nil // session was already replaced with a new valid session
		}

		return c.startNewLoginSession(ctx)
	})
}

func (c *powerStoreClient) forceLoginSessionRemoval(sessionKey string, sessionToRemove *powerStoreLoginSession) {
	_, _ = powerStoreTokenCache.Replace(sessionKey, func(ls *powerStoreLoginSession) (*powerStoreLoginSession, error) {
		if ls != sessionToRemove {
			return ls, nil // session was already replaced
		}

		return nil, nil // delete session
	})
}

func (c *powerStoreClient) doHTTPRequestWithLoginSession(ctx context.Context, method string, path string, requestData, responseData any, requestEditors ...func(*http.Request) error) (*http.Response, error) {
	sessionKey := makePowerStoreFingerprint(c.gateway, c.username, c.password)

	session, err := c.getOrCreateLoginSession(ctx, sessionKey)
	if err != nil {
		return nil, err
	}

	requestEditors = append([]func(*http.Request) error{c.withLoginSession(session)}, requestEditors...)
	resp, err := c.doHTTPRequest(ctx, method, path, requestData, responseData, requestEditors...)
	if resp != nil && resp.StatusCode == http.StatusUnauthorized {
		// there is something wrong with the session token, remove it
		c.forceLoginSessionRemoval(sessionKey, session)
	}

	return resp, err
}

func (c *powerStoreClient) withBasicAuthorization(username, password string) func(req *http.Request) error {
	return func(req *http.Request) error {
		token := base64.StdEncoding.EncodeToString(fmt.Appendf(nil, "%s:%s", username, password))
		req.Header.Set("Authorization", "Basic "+token)
		return nil
	}
}

func (c *powerStoreClient) withLoginSession(ls *powerStoreLoginSession) func(req *http.Request) error {
	return func(req *http.Request) error {
		req.Header.Add("Cookie", fmt.Sprintf("%s=%s", powerStoreAuthorizationCookieName, ls.AuthToken))
		req.Header.Set(powerStoreCSRFHeaderName, ls.CSRFToken)
		return nil
	}
}

func (c *powerStoreClient) withQueryParams(params url.Values) func(req *http.Request) error {
	return func(req *http.Request) error {
		if params == nil {
			req.URL.RawQuery = ""
			return nil
		}

		req.URL.RawQuery = params.Encode()
		return nil
	}
}

const powerStoreMaxAPIResponseLimit = 2000

type powerStorePagination struct {
	Page         int
	ItemsPerPage int
}

// Offset computes offset value for the provided pagination state.
func (p powerStorePagination) Offset() int {
	page := max(0, p.Page)
	limit := p.Limit()
	return page * limit
}

// Limit computes limit value for the provided pagination state.
func (p powerStorePagination) Limit() int {
	return min(max(0, p.ItemsPerPage), powerStoreMaxAPIResponseLimit)
}

// SetParams sets URL pagination parameters.
func (p powerStorePagination) SetParams(params url.Values) {
	params.Set("offset", strconv.Itoa(p.Offset()))
	params.Set("limit", strconv.Itoa(p.Limit()))
}

// SetQuery sets query pagination parameters.
func (p powerStorePagination) SetQuery(params map[string]string) {
	params["offset"] = strconv.Itoa(p.Offset())
	params["limit"] = strconv.Itoa(p.Limit())
}

// powerStorePaginateQuery adds pagination parameters to the provided query.
func powerStorePaginateQuery(query map[string]string, pagination powerStorePagination) map[string]string {
	query = maps.Clone(query)
	if query == nil {
		query = map[string]string{}
	}

	pagination.SetQuery(query)
	return query
}

// powerStoreURLValuesFromQuery transforms query into URL parameters.
func powerStoreURLValuesFromQuery(query map[string]string) url.Values {
	params := url.Values{}
	for key, val := range query {
		params.Set(key, val)
	}

	return params
}

// powerStoreQueryResponseHasMoreItems informs if there are more items available for the HTTP PowerStore query response.
func powerStoreQueryResponseHasMoreItems(resp *http.Response) (bool, error) {
	if resp == nil || resp.StatusCode != http.StatusPartialContent {
		return false, nil
	}

	// valid Content-Range HTTP headers returned by PowerStore have a form:
	// - firstOffset '-' lastOffset '/' totalItems
	// - '*' '/' totalItems
	header := resp.Header.Get("Content-Range")
	if header == "" {
		return false, nil
	}

	rangeStr, totalItemsStr, ok := strings.Cut(header, "/")
	if !ok {
		return false, fmt.Errorf("Invalid format of Content-Range header: %q", header)
	}

	if rangeStr == "*" {
		return false, nil
	}

	fistOffsetStr, lastOffsetStr, ok := strings.Cut(rangeStr, "-")
	if !ok {
		return false, fmt.Errorf("Invalid format of Content-Range header: %q", header)
	}

	_, err := strconv.ParseUint(fistOffsetStr, 10, 64)
	if err != nil {
		return false, fmt.Errorf("Invalid format of Content-Range header: %q", header)
	}

	lastOffset, err := strconv.ParseUint(lastOffsetStr, 10, 64)
	if err != nil {
		return false, fmt.Errorf("Invalid format of Content-Range header: %q", header)
	}

	totalItems, err := strconv.ParseUint(totalItemsStr, 10, 64)
	if err != nil {
		return false, fmt.Errorf("Invalid format of Content-Range header: %q", header)
	}

	return totalItems > lastOffset+1, nil
}

type powerStoreIDResource struct {
	ID string `json:"id"`
}

type powerStoreLoginSessionResource struct {
	ID                       string   `json:"id"`
	User                     string   `json:"user"`
	RoleIDs                  []string `json:"role_ids"`
	IdleTimeout              int64    `json:"idle_timeout"`
	IsPasswordChangeRequired bool     `json:"is_password_change_required"`
	IsBuiltInUser            bool     `json:"is_built_in_user"`
}

func (c *powerStoreClient) getLoginSessionInfoWithBasicAuthorization(ctx context.Context) (*http.Response, []*powerStoreLoginSessionResource, error) {
	body := []*powerStoreLoginSessionResource{}
	resp, err := c.doHTTPRequest(ctx, http.MethodGet, "/api/rest/login_session", nil, &body,
		c.withBasicAuthorization(c.username, c.password),
		c.withQueryParams(url.Values{"select": []string{"id,user,role_ids,idle_timeout,is_password_change_required,is_built_in_user"}}),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("Retrieving PowerStore login session info: %w", err)
	}

	return resp, body, nil
}

type powerStoreApplianceMetricsResource struct {
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

func (c *powerStoreClient) getApplianceMetricsByQuery(ctx context.Context, query map[string]string) ([]*powerStoreApplianceMetricsResource, bool, error) {
	params := powerStoreURLValuesFromQuery(query)
	params.Set("select", "id,name,avg_latency,total_iops,total_bandwidth,last_logical_total_space,last_logical_used_space,last_physical_total_space,last_physical_used_space")

	body := []*powerStoreApplianceMetricsResource{}
	resp, err := c.doHTTPRequestWithLoginSession(ctx, http.MethodGet, "/api/rest/appliance_list_cma_view", nil, &body,
		c.withQueryParams(params),
	)
	if err != nil {
		return nil, false, fmt.Errorf("Retrieving metrics of PowerStore appliances: %w", err)
	}

	hasMore, err := powerStoreQueryResponseHasMoreItems(resp)
	if err != nil {
		return nil, false, fmt.Errorf("Retrieving metrics of PowerStore appliances: %w", err)
	}

	return body, hasMore, nil
}

// GetApplianceMetrics retrieves appliance metrics.
func (c *powerStoreClient) GetApplianceMetrics(ctx context.Context) ([]*powerStoreApplianceMetricsResource, error) {
	var metrics []*powerStoreApplianceMetricsResource
	for page := 0; ; page++ {
		metricsPage, hasMore, err := c.getApplianceMetricsByQuery(ctx, powerStorePaginateQuery(nil, powerStorePagination{Page: page}))
		if err != nil {
			return nil, err
		}

		metrics = append(metrics, metricsPage...)
		if !hasMore {
			return metrics, nil
		}
	}
}

type powerStoreHostResource struct {
	ID               string                                 `json:"id,omitempty"`
	Name             string                                 `json:"name,omitempty"`
	Description      string                                 `json:"description,omitempty"`
	Initiators       []*powerStoreHostInitiatorResource     `json:"initiators,omitempty"`
	OsType           string                                 `json:"os_type,omitempty"`
	HostConnectivity string                                 `json:"host_connectivity,omitempty"`
	MappedHosts      []*powerStoreHostVolumeMappingResource `json:"mapped_hosts,omitempty"`
}

const (
	powerStoreOsTypeEnumLinux = "Linux"
)

type powerStoreHostInitiatorResource struct {
	ID       string `json:"id,omitempty"`
	PortName string `json:"port_name,omitempty"`
	PortType string `json:"port_type,omitempty"`
}

const (
	powerStoreInitiatorPortTypeEnumISCSI = "iSCSI"
	powerStoreInitiatorPortTypeEnumFC    = "FC"
	powerStoreInitiatorPortTypeEnumNVMe  = "NVMe"
)

func (c *powerStoreClient) getHostsByQuery(ctx context.Context, query map[string]string, filterOwnedByLxd bool) ([]*powerStoreHostResource, bool, error) {
	params := url.Values{}
	for key, val := range query {
		params.Set(key, val)
	}

	params.Set("select", "id,name,description,initiators(id,port_name,port_type),os_type,host_connectivity,mapped_hosts(id,host_id,volume_id)")

	body := []*powerStoreHostResource{}
	resp, err := c.doHTTPRequestWithLoginSession(ctx, http.MethodGet, "/api/rest/host", nil, &body,
		c.withQueryParams(params),
	)
	if err != nil {
		return nil, false, fmt.Errorf("Retrieving information about PowerStore hosts: %w", err)
	}

	hasMore, err := powerStoreQueryResponseHasMoreItems(resp)
	if err != nil {
		return nil, false, fmt.Errorf("Retrieving information about PowerStore hosts: %w", err)
	}

	if !filterOwnedByLxd {
		return body, hasMore, nil
	}

	// In most cases all items in the returned body will be managed by LXD and no
	// item will be filtered out.
	filtered := make([]*powerStoreHostResource, 0, len(body))
	for _, h := range body {
		if !strings.HasPrefix(h.Name, c.hostResourceNamePrefix) {
			continue
		}

		filtered = append(filtered, h)
	}

	return filtered, hasMore, nil
}

func (c *powerStoreClient) getHostByQuery(ctx context.Context, query map[string]string, filterOwnedByLxd bool) (*powerStoreHostResource, error) {
	hosts, _, err := c.getHostsByQuery(ctx, powerStorePaginateQuery(query, powerStorePagination{ItemsPerPage: 1}), filterOwnedByLxd)
	if err != nil {
		return nil, err
	}

	if len(hosts) == 0 {
		return nil, nil
	}

	return hosts[0], nil
}

// GetHostByID retrieves host using its ID.
func (c *powerStoreClient) GetHostByID(ctx context.Context, id string) (*powerStoreHostResource, error) {
	return c.getHostByQuery(ctx, map[string]string{"id": "eq." + id}, true)
}

// getUnfilteredHostByID retrieves host using its ID without filtration
// (returns host even if it is not managed by lxd).
func (c *powerStoreClient) getUnfilteredHostByID(ctx context.Context, id string) (*powerStoreHostResource, error) {
	return c.getHostByQuery(ctx, map[string]string{"id": "eq." + id}, false)
}

// GetHostByName retrieves host using its name.
func (c *powerStoreClient) GetHostByName(ctx context.Context, name string) (*powerStoreHostResource, error) {
	return c.getHostByQuery(ctx, map[string]string{"name": "eq." + name}, true)
}

// CreateHost creates new host.
func (c *powerStoreClient) CreateHost(ctx context.Context, host *powerStoreHostResource) error {
	body := &powerStoreIDResource{}
	_, err := c.doHTTPRequestWithLoginSession(ctx, http.MethodPost, "/api/rest/host", host, body)
	if err != nil {
		return fmt.Errorf("Creating PowerStore host: %w", err)
	}

	// Fetch host to populate all fields.
	created, err := c.GetHostByID(ctx, body.ID)
	if err != nil {
		return fmt.Errorf("Creating PowerStore host: %w", err)
	}

	if created == nil {
		return errors.New("Creating PowerStore host: No data of new host found")
	}

	*host = *created
	return nil
}

// DeleteHostByID deletes host using its ID.
func (c *powerStoreClient) DeleteHostByID(ctx context.Context, id string) error {
	_, err := c.doHTTPRequestWithLoginSession(ctx, http.MethodDelete, "/api/rest/host/"+id, nil, nil)
	if err != nil {
		return fmt.Errorf("Deleting PowerStore host: %w", err)
	}

	return nil
}

type powerStoreAddInitiatorToHostResource struct {
	AddInitiators []*powerStoreHostInitiatorResource `json:"add_initiators,omitempty"`
}

// AddInitiatorToHostByID adds initiator to host using its ID.
func (c *powerStoreClient) AddInitiatorToHostByID(ctx context.Context, hostID string, initiator *powerStoreHostInitiatorResource) error {
	reqBody := &powerStoreAddInitiatorToHostResource{AddInitiators: []*powerStoreHostInitiatorResource{initiator}}
	_, err := c.doHTTPRequestWithLoginSession(ctx, http.MethodPatch, "/api/rest/host/"+hostID, reqBody, nil)
	if err != nil {
		return fmt.Errorf("Adding initiator to PowerStore host: %w", err)
	}

	return nil
}

type powerStoreRemoveInitiatorFromHostResource struct {
	RemoveInitiators []string `json:"remove_initiators,omitempty"`
}

// RemoveInitiatorFromHostByID removes initiator matching port name from host using its ID.
func (c *powerStoreClient) RemoveInitiatorFromHostByID(ctx context.Context, hostID string, initiator *powerStoreHostInitiatorResource) error {
	reqBody := &powerStoreRemoveInitiatorFromHostResource{RemoveInitiators: []string{initiator.PortName}}
	_, err := c.doHTTPRequestWithLoginSession(ctx, http.MethodPatch, "/api/rest/host/"+hostID, reqBody, nil)
	if err != nil {
		return fmt.Errorf("Removing initiator from PowerStore host: %w", err)
	}

	return nil
}

type powerStoreHostAttachResource struct {
	VolumeGroupID string `json:"volume_group_id,omitempty"`
	VolumeID      string `json:"volume_id,omitempty"`
}

// AttachHostToVolume attaches (maps) host to volume.
func (c *powerStoreClient) AttachHostToVolume(ctx context.Context, hostID, volID string) error {
	reqBody := &powerStoreHostAttachResource{VolumeID: volID}
	_, err := c.doHTTPRequestWithLoginSession(ctx, http.MethodPost, "/api/rest/host/"+hostID+"/attach", reqBody, nil)
	if err != nil {
		return fmt.Errorf("Attaching PowerStore host to a volume: %w", err)
	}

	return nil
}

type powerStoreHostDetachResource struct {
	VolumeGroupID string `json:"volume_group_id,omitempty"`
	VolumeID      string `json:"volume_id,omitempty"`
}

// DetachHostFromVolume detaches (unmaps) host from volume.
func (c *powerStoreClient) DetachHostFromVolume(ctx context.Context, hostID, volID string) error {
	reqBody := &powerStoreHostDetachResource{VolumeID: volID}
	_, err := c.doHTTPRequestWithLoginSession(ctx, http.MethodPost, "/api/rest/host/"+hostID+"/detach", reqBody, nil)
	if err != nil {
		return fmt.Errorf("Detaching PowerStore host from a volume: %w", err)
	}

	return nil
}

type powerStoreInitiatorResource struct {
	ID       string `json:"id,omitempty"`
	HostID   string `json:"host_id,omitempty"`
	PortName string `json:"port_name,omitempty"`
	PortType string `json:"port_type,omitempty"`
}

func (c *powerStoreClient) getInitiatorsByQuery(ctx context.Context, query map[string]string) ([]*powerStoreInitiatorResource, bool, error) {
	params := powerStoreURLValuesFromQuery(query)
	params.Set("select", "id,host_id,port_name,port_type")

	body := []*powerStoreInitiatorResource{}
	resp, err := c.doHTTPRequestWithLoginSession(ctx, http.MethodGet, "/api/rest/initiator", nil, &body,
		c.withQueryParams(params),
	)
	if err != nil {
		return nil, false, fmt.Errorf("Retrieving information about PowerStore initiators: %w", err)
	}

	hasMore, err := powerStoreQueryResponseHasMoreItems(resp)
	if err != nil {
		return nil, false, fmt.Errorf("Retrieving information about PowerStore initiators: %w", err)
	}

	return body, hasMore, nil
}

func (c *powerStoreClient) getInitiatorByQuery(ctx context.Context, query map[string]string) (*powerStoreInitiatorResource, error) {
	initiators, _, err := c.getInitiatorsByQuery(ctx, powerStorePaginateQuery(query, powerStorePagination{ItemsPerPage: 1}))
	if err != nil {
		return nil, err
	}

	if len(initiators) == 0 {
		return nil, nil
	}

	return initiators[0], nil
}

// GetHostByInitiator retrieves host that have initiator matching port name and
// type.
func (c *powerStoreClient) GetHostByInitiator(ctx context.Context, initiator *powerStoreHostInitiatorResource) (*powerStoreHostResource, error) {
	hostInitiator, err := c.getInitiatorByQuery(ctx, map[string]string{"port_name": "eq." + initiator.PortName, "port_type": "eq." + initiator.PortType})
	if err != nil {
		return nil, err
	}

	if hostInitiator == nil {
		return nil, nil
	}

	return c.getUnfilteredHostByID(ctx, hostInitiator.HostID)
}

type powerStoreVolumeResource struct {
	ID            string                                 `json:"id,omitempty"`
	Name          string                                 `json:"name,omitempty"`
	Description   string                                 `json:"description,omitempty"`
	Type          string                                 `json:"type,omitempty"`
	State         string                                 `json:"state,omitempty"`
	Size          int64                                  `json:"size,omitempty"`
	LogicalUsed   int64                                  `json:"logical_used,omitempty"`
	WWN           string                                 `json:"wwn,omitempty"`
	AppType       string                                 `json:"app_type,omitempty"`
	AppTypeOther  string                                 `json:"app_type_other,omitempty"`
	VolumeGroups  []*powerStoreIDResource                `json:"volume_groups,omitempty"`
	MappedVolumes []*powerStoreHostVolumeMappingResource `json:"mapped_volumes,omitempty"`
}

type powerStoreHostVolumeMappingResource struct {
	ID       string `json:"id,omitempty"`
	HostID   string `json:"host_id,omitempty"`
	VolumeID string `json:"volume_id,omitempty"`
}

func (c *powerStoreClient) getVolumesByQuery(ctx context.Context, query map[string]string, filterOwnedByLxd bool) ([]*powerStoreVolumeResource, bool, error) {
	params := powerStoreURLValuesFromQuery(query)
	params.Set("select", "id,name,description,type,state,size,logical_used,wwn,app_type,app_type_other,volume_groups(id),mapped_volumes(id,host_id,volume_id)")

	body := []*powerStoreVolumeResource{}
	resp, err := c.doHTTPRequestWithLoginSession(ctx, http.MethodGet, "/api/rest/volume", nil, &body,
		c.withQueryParams(params),
	)
	if err != nil {
		return nil, false, fmt.Errorf("Retrieving information about PowerStore volumes: %w", err)
	}

	hasMore, err := powerStoreQueryResponseHasMoreItems(resp)
	if err != nil {
		return nil, false, fmt.Errorf("Retrieving information about PowerStore volumes: %w", err)
	}

	if !filterOwnedByLxd {
		return body, hasMore, nil
	}

	// in most cases all items in the returned body will belong to the current storage pool and no item will be filtered out
	filtered := make([]*powerStoreVolumeResource, 0, len(body))
	for _, v := range body {
		if !strings.HasPrefix(v.Name, c.volumeResourceNamePrefix) {
			continue
		}

		filtered = append(filtered, v)
	}

	return filtered, hasMore, nil
}

func (c *powerStoreClient) getVolumeByQuery(ctx context.Context, query map[string]string, filterOwnedByLxd bool) (*powerStoreVolumeResource, error) {
	vols, _, err := c.getVolumesByQuery(ctx, powerStorePaginateQuery(query, powerStorePagination{ItemsPerPage: 1}), filterOwnedByLxd)
	if err != nil {
		return nil, err
	}

	if len(vols) == 0 {
		return nil, nil
	}

	return vols[0], nil
}

// GetVolumes retrieves list of volume associated with the storage pool.
func (c *powerStoreClient) GetVolumes(ctx context.Context) ([]*powerStoreVolumeResource, error) {
	query := map[string]string{"name": fmt.Sprintf("ilike.%s*", c.volumeResourceNamePrefix)}

	var vols []*powerStoreVolumeResource
	for page := 0; ; page++ {
		volsPage, hasMore, err := c.getVolumesByQuery(ctx, powerStorePaginateQuery(query, powerStorePagination{Page: page}), true)
		if err != nil {
			return nil, err
		}

		vols = append(vols, volsPage...)
		if !hasMore {
			return vols, nil
		}
	}
}

// GetVolumeByID retrieves volume using its ID.
func (c *powerStoreClient) GetVolumeByID(ctx context.Context, id string) (*powerStoreVolumeResource, error) {
	return c.getVolumeByQuery(ctx, map[string]string{"id": "eq." + id}, true)
}

// GetVolumeByName retrieves volume using its name.
func (c *powerStoreClient) GetVolumeByName(ctx context.Context, name string) (*powerStoreVolumeResource, error) {
	return c.getVolumeByQuery(ctx, map[string]string{"name": "eq." + name}, true)
}

// CreateVolume creates a new volume.
func (c *powerStoreClient) CreateVolume(ctx context.Context, vol *powerStoreVolumeResource) error {
	body := &powerStoreIDResource{}
	_, err := c.doHTTPRequestWithLoginSession(ctx, http.MethodPost, "/api/rest/volume", vol, body)
	if err != nil {
		return fmt.Errorf("Creating PowerStore volume: %w", err)
	}

	// Fetch volume to populate all fields.
	created, err := c.GetVolumeByID(ctx, body.ID)
	if err != nil {
		return fmt.Errorf("Creating PowerStore volume: %w", err)
	}

	if created == nil {
		return errors.New("Creating PowerStore volume: No data of new volume found")
	}

	*vol = *created
	return nil
}

// DeleteVolumeByID deletes volume using its ID.
func (c *powerStoreClient) DeleteVolumeByID(ctx context.Context, id string) error {
	_, err := c.doHTTPRequestWithLoginSession(ctx, http.MethodDelete, "/api/rest/volume/"+id, nil, nil)
	if err != nil {
		return fmt.Errorf("Deleting PowerStore volume: %w", err)
	}

	return nil
}

type powerStoreVolumeModifyResource struct {
	Size int64 `json:"size,omitempty"`
}

// ResizeVolumeByID creates a new volume.
func (c *powerStoreClient) ResizeVolumeByID(ctx context.Context, id string, newSize int64) error {
	reqBody := &powerStoreVolumeModifyResource{Size: newSize}
	_, err := c.doHTTPRequestWithLoginSession(ctx, http.MethodPatch, "/api/rest/volume/"+id, reqBody, nil)
	if err != nil {
		return fmt.Errorf("Resizing PowerStore volume: %w", err)
	}

	return nil
}

type powerStoreVolumeGroupRemoveMembersResource struct {
	VolumeIDs []string `json:"volume_ids,omitempty"`
}

// RemoveMembersFromVolumeGroup removes volumes from the volume group.
func (c *powerStoreClient) RemoveMembersFromVolumeGroup(ctx context.Context, id string, volumeIDs []string) error {
	reqBody := &powerStoreVolumeGroupRemoveMembersResource{VolumeIDs: volumeIDs}
	_, err := c.doHTTPRequestWithLoginSession(ctx, http.MethodPost, "/api/rest/volume_group/"+id+"/remove_members", reqBody, nil)
	if err != nil {
		return fmt.Errorf("Removing members from PowerStore volume group: %w", err)
	}

	return nil
}

// targets return discovered PowerStore targets (their addresses and associated
// qualified names).
func (d *powerstore) targets() ([]powerStoreTarget, error) {
	if len(d.discoveredTargets) == 0 {
		connector, err := d.connector()
		if err != nil {
			return nil, err
		}

		discoveryAddresses := shared.SplitNTrimSpace(d.config["powerstore.discovery"], ",", -1, true)
		var discoveryLogRecords []any
		for _, addr := range discoveryAddresses {
			discovered, err := connector.Discover(d.state.ShutdownCtx, addr)
			if err != nil {
				// Underlying connector should log a waring.
				continue
			}

			discoveryLogRecords = append(discoveryLogRecords, discovered...)
		}

		if len(discoveryLogRecords) == 0 {
			return nil, errors.New("Failed fetching a discovery log record from any of the target addresses")
		}

		discoveredTargets := []powerStoreTarget{}
		userForcedTargetAddresses := shared.SplitNTrimSpace(d.config["powerstore.target"], ",", -1, true)
		parser := d.discoveryLogRecordParser(userForcedTargetAddresses)
		for _, record := range discoveryLogRecords {
			target, includeTarget, err := parser(record)
			if err != nil {
				return nil, err
			}

			if !includeTarget {
				continue
			}

			discoveredTargets = append(discoveredTargets, target)
		}

		discoveredTargets = shared.Unique(discoveredTargets)

		if len(discoveredTargets) == 0 {
			return nil, errors.New("Failed fetching a discovery log record from any of the discovery addresses")
		}

		d.discoveredTargets = discoveredTargets
	}

	return d.discoveredTargets, nil
}

// discoveryLogRecordParser returns a parsing function that converts single
// discovery log entry to target.
func (d *powerstore) discoveryLogRecordParser(filterTargetAddresses []string) func(any) (powerStoreTarget, bool, error) {
	mode := d.config["powerstore.mode"]
	transport := d.config["powerstore.transport"]
	switch {
	case mode == powerStoreModeISCSI && transport == powerStoreTransportTCP:
		filterTargetAddresses = slices.Clone(filterTargetAddresses)
		for i := range filterTargetAddresses {
			filterTargetAddresses[i] = shared.EnsurePort(filterTargetAddresses[i], connectors.ISCSIDefaultPort)
		}

		return func(record any) (powerStoreTarget, bool, error) {
			r, ok := record.(connectors.ISCSIDiscoveryLogRecord)
			if !ok {
				return powerStoreTarget{}, false, fmt.Errorf("Invalid discovery log record entry type %T is not connectors.ISCSIDiscoveryLogRecord", record)
			}

			target := powerStoreTarget{
				Address:       r.Address,
				QualifiedName: r.IQN,
			}

			if len(filterTargetAddresses) > 0 && !slices.Contains(filterTargetAddresses, target.Address) {
				return powerStoreTarget{}, false, nil
			}

			return target, true, nil
		}

	case mode == powerStoreModeNVME && transport == powerStoreTransportTCP:
		filterTargetAddresses = slices.Clone(filterTargetAddresses)
		for i := range filterTargetAddresses {
			filterTargetAddresses[i] = shared.EnsurePort(filterTargetAddresses[i], connectors.NVMeDefaultTransportPort)
		}

		return func(record any) (powerStoreTarget, bool, error) {
			r, ok := record.(connectors.NVMeDiscoveryLogRecord)
			if !ok {
				return powerStoreTarget{}, false, fmt.Errorf("Invalid discovery log record entry type %T is not connectors.NVMeDiscoveryLogRecord", record)
			}

			target := powerStoreTarget{
				Address:       net.JoinHostPort(r.TransportAddress, r.TransportServiceIdentifier),
				QualifiedName: r.SubNQN,
			}

			if len(filterTargetAddresses) > 0 && !slices.Contains(filterTargetAddresses, target.Address) {
				return powerStoreTarget{}, false, nil
			}

			return target, true, nil
		}
	}

	panic(fmt.Errorf("storage: powerstore: bad configuration (mode: %q, transport: %q); this case should never be reached", mode, transport))
}

// hostResourceName derives the name of a host resource in PowerStore
// associated with the current node or host and mode. On success, it returns
// name applicable to use as PowerStore host resource name along of full
// unencoded name.
func (d *powerstore) hostResourceName() (resource string, hostname string, err error) {
	hostname, err = ResolveServerName(d.state.ServerName)
	if err != nil {
		return "", "", err
	}

	resource = fmt.Sprintf("%s%s-%s/%s", powerStoreResourceNamePrefix, hostname, d.config["powerstore.mode"], d.config["powerstore.transport"])
	if len(resource) > 128 { // PowerStore limits host resource name to 128 characters
		hostnameHash := sha256.Sum256([]byte(hostname))
		resource = fmt.Sprintf("%s%s-%s/%s", powerStoreResourceNamePrefix, base64.StdEncoding.EncodeToString(hostnameHash[:]), d.config["powerstore.mode"], d.config["powerstore.transport"])
	}

	return resource, hostname, nil
}

// initiator returns PowerStore initiator resource associated the current host,
// mode and transport.
func (d *powerstore) initiator() (*powerStoreHostInitiatorResource, error) {
	if d.initiatorResource == nil {
		initiatorResource := &powerStoreHostInitiatorResource{}
		connector, err := d.connector()
		if err != nil {
			return nil, err
		}

		initiatorResource.PortName, err = connector.QualifiedName()
		if err != nil {
			return nil, err
		}

		switch {
		case d.config["powerstore.mode"] == connectors.TypeNVME:
			// PowerStore uses the same port type for both NVMe/TCP and NVMe/FC
			initiatorResource.PortType = powerStoreInitiatorPortTypeEnumNVMe
		case d.config["powerstore.mode"] == connectors.TypeISCSI && d.config["powerstore.transport"] == "tcp":
			initiatorResource.PortType = powerStoreInitiatorPortTypeEnumISCSI
		case d.config["powerstore.mode"] == connectors.TypeISCSI && d.config["powerstore.transport"] == "fc":
			initiatorResource.PortType = powerStoreInitiatorPortTypeEnumFC
		default:
			return nil, fmt.Errorf("Cannot determine PowerStore initiator port type (mode: %q, transport: %q)", d.config["powerstore.mode"], d.config["powerstore.transport"])
		}

		d.initiatorResource = initiatorResource
	}

	return d.initiatorResource, nil
}

// getMappedDevicePathByVolumeID returns the local device path associated with
// the given PowerStore volume WWN.
func (d *powerstore) getMappedDevicePathByVolumeWWN(volWWN string, wait bool) (devicePath string, err error) {
	connector, err := d.connector()
	if err != nil {
		return "", err
	}

	devicePathFilter := func(path string) bool {
		return strings.Contains(path, volWWN)
	}

	if wait {
		// Wait for the device path to appear as the volume has been just mapped to the host.
		devicePath, err = connector.WaitDiskDevicePath(d.state.ShutdownCtx, devicePathFilter)
	} else {
		// Get the the device path without waiting.
		devicePath, err = connector.GetDiskDevicePath(devicePathFilter)
	}

	if err != nil {
		return "", err
	}

	return devicePath, nil
}

// mapVolumeByVolumeResource maps the volume associated with the given
// PowerStore volume resource onto this host.
func (d *powerstore) mapVolumeByVolumeResource(volResource *powerStoreVolumeResource) (revert.Hook, error) {
	connector, err := d.connector()
	if err != nil {
		return nil, err
	}

	unlock, err := remoteVolumeMapLock(connector.Type(), d.Info().Name)
	if err != nil {
		return nil, err
	}

	defer unlock()

	reverter := revert.New()
	defer reverter.Fail()

	hostResource, err := d.getOrCreateHostWithInitiatorResource()
	if err != nil {
		return nil, err
	}

	reverter.Add(func() { _ = d.deleteHostAndInitiatorResource(hostResource) })

	mapped := slices.ContainsFunc(volResource.MappedVolumes, func(mappingResource *powerStoreHostVolumeMappingResource) bool {
		return mappingResource.HostID == hostResource.ID
	})
	if !mapped {
		err := d.client().AttachHostToVolume(d.state.ShutdownCtx, hostResource.ID, volResource.ID)
		if err != nil {
			return nil, err
		}

		reverter.Add(func() { _ = d.client().DetachHostFromVolume(d.state.ShutdownCtx, hostResource.ID, volResource.ID) })
	}

	// Reverting mapping or connection outside mapVolume function
	// could conflict with other ongoing operations as lock will
	// already be released. Therefore, use unmapVolume instead
	// because it ensures the lock is acquired and accounts for
	// an existing session before unmapping a volume.
	outerReverter := revert.New()
	if !mapped {
		outerReverter.Add(func() { _ = d.unmapVolumeByVolumeResource(volResource) })
	}

	targets, err := d.targets()
	if err != nil {
		return nil, err
	}

	for qualifiedName, addresses := range powerStoreGroupTargetsAddressesByQualifiedName(targets...) {
		cleanup, err := connector.Connect(d.state.ShutdownCtx, qualifiedName, addresses...)
		if err != nil {
			return nil, err
		}

		reverter.Add(cleanup)
		outerReverter.Add(cleanup)
	}

	reverter.Success()
	return outerReverter.Fail, nil
}

// unmapVolumeByVolumeResource unmaps the volume associated with the given
// PowerStore volume resource from this host.
func (d *powerstore) unmapVolumeByVolumeResource(volResource *powerStoreVolumeResource) error {
	connector, err := d.connector()
	if err != nil {
		return err
	}

	unlock, err := remoteVolumeMapLock(connector.Type(), d.Info().Name)
	if err != nil {
		return err
	}

	defer unlock()

	hostResource, _, err := d.getHostWithInitiatorResource()
	if err != nil {
		return err
	}

	var volumePath string
	if volResource != nil {
		// Get a path of a block device we want to unmap, ignoring any errors.
		volumePath, _ := d.getMappedDevicePathByVolumeWWN(d.volumeWWN(volResource), false)
		err = connector.RemoveDiskDevice(d.state.ShutdownCtx, volumePath)
		if err != nil {
			return fmt.Errorf("Cannot unmap device for PowerStore volume resource with ID %q: %w", volResource.ID, err)
		}
	}

	if hostResource != nil && volResource != nil {
		for _, mappingResource := range volResource.MappedVolumes {
			if mappingResource.HostID != hostResource.ID {
				continue
			}

			err := d.client().DetachHostFromVolume(d.state.ShutdownCtx, mappingResource.HostID, volResource.ID)
			if err != nil {
				return err
			}
		}
	}

	if volumePath != "" {
		// Wait until the volume has disappeared.
		ctx, cancel := context.WithTimeout(d.state.ShutdownCtx, 30*time.Second)
		defer cancel()

		if !block.WaitDiskDeviceGone(ctx, volumePath) {
			return fmt.Errorf("Timeout exceeded waiting for disk device %q related to PowerStore volume resource with ID %q to disappear", volumePath, volResource.ID)
		}
	}

	// Disconnect connector if:
	// - there is no associated PowerStore host resource,
	// - there are no other volumes mapped.
	if hostResource == nil || len(hostResource.MappedHosts) == 0 {
		targets, err := d.targets()
		if err != nil {
			return err
		}

		for qualifiedName := range powerStoreGroupTargetsAddressesByQualifiedName(targets...) {
			err = connector.Disconnect(qualifiedName)
			if err != nil {
				return err
			}
		}
	}

	if hostResource != nil {
		err = d.deleteHostAndInitiatorResource(hostResource)
		if err != nil {
			return err
		}
	}

	return nil
}

// getVolumeResourceByVolume retrieves volume resource associated with
// the provided volume.
func (d *powerstore) getVolumeResourceByVolume(vol Volume) (*powerStoreVolumeResource, error) {
	volResourceName, err := d.volumeResourceName(vol)
	if err != nil {
		return nil, err
	}

	volResource, err := d.client().GetVolumeByName(d.state.ShutdownCtx, volResourceName)
	if err != nil {
		return nil, fmt.Errorf("Failed retrieving PowerStore volume resource %q associated with volume %q: %w", volResourceName, vol.name, err)
	}

	return volResource, nil
}

// getExistingVolumeResourceByVolume retrieves volume resource associated with
// the provided volume, just like getVolumeResourceByVolume function, but
// returns error if the volume resource does not exists.
func (d *powerstore) getExistingVolumeResourceByVolume(vol Volume) (*powerStoreVolumeResource, error) {
	volResourceName, err := d.volumeResourceName(vol)
	if err != nil {
		return nil, err
	}

	volResource, err := d.client().GetVolumeByName(d.state.ShutdownCtx, volResourceName)
	if err != nil {
		return nil, fmt.Errorf("Failed retrieving PowerStore volume resource %q associated with volume %q: %w", volResourceName, vol.name, err)
	}

	if volResource == nil {
		return nil, fmt.Errorf("Failed retrieving PowerStore volume resource %q associated with volume %q: resource not found", volResourceName, vol.name)
	}

	return volResource, nil
}

// createVolumeResource creates volume resources in PowerStore associated with
// the provided volume.
func (d *powerstore) createVolumeResource(vol Volume) (*powerStoreVolumeResource, error) {
	sizeBytes, err := units.ParseByteSizeString(vol.ConfigSize())
	if err != nil {
		return nil, err
	}

	if sizeBytes < powerStoreMinVolumeSizeBytes {
		return nil, fmt.Errorf("Volume size is too small, supported minimum %s", powerStoreMinVolumeSizeUnit)
	}

	if sizeBytes > powerStoreMaxVolumeSizeBytes {
		return nil, fmt.Errorf("Volume size is too large, supported maximum %s", powerStoreMaxVolumeSizeUnit)
	}

	volResourceName, err := d.volumeResourceName(vol)
	if err != nil {
		return nil, err
	}

	typ := "lxd"
	if vol.volType != "" {
		typ += ":" + string(vol.volType)
	}

	if vol.contentType != "" {
		typ += ":" + string(vol.contentType)
	}

	volResource := &powerStoreVolumeResource{
		Name:         volResourceName,
		Description:  powerStoreSprintfLimit(128, "LXD Name: %s", vol.name), // maximum allowed value length for volume description field is 128
		Size:         sizeBytes,
		AppType:      "Other",
		AppTypeOther: powerStoreSprintfLimit(32, "%s", typ), // maximum allowed value length for volume app_type_other field is 32,
	}

	err = d.client().CreateVolume(d.state.ShutdownCtx, volResource)
	if err != nil {
		return nil, err
	}

	return volResource, nil
}

// deleteVolumeResource deletes volume resources in PowerStore.
func (d *powerstore) deleteVolumeResource(volResource *powerStoreVolumeResource) error {
	for _, mappingResource := range volResource.MappedVolumes {
		err := d.client().DetachHostFromVolume(d.state.ShutdownCtx, mappingResource.HostID, volResource.ID)
		if err != nil {
			return err
		}
	}
	for _, volumeGroupResource := range volResource.VolumeGroups {
		err := d.client().RemoveMembersFromVolumeGroup(d.state.ShutdownCtx, volumeGroupResource.ID, []string{volResource.ID})
		if err != nil {
			return err
		}
	}
	return d.client().DeleteVolumeByID(d.state.ShutdownCtx, volResource.ID)
}

// getHostWithInitiatorResource retrieves initiator and associated host
// resources from PowerStore associated with the current host, mode and
// transport.
func (d *powerstore) getHostWithInitiatorResource() (*powerStoreHostResource, *powerStoreHostInitiatorResource, error) {
	initiatorResource, err := d.initiator()
	if err != nil {
		return nil, nil, err
	}

	hostResource, err := d.client().GetHostByInitiator(d.state.ShutdownCtx, initiatorResource)
	if err != nil {
		return nil, nil, err
	}

	if hostResource != nil {
		// host with initiator already exists
		return hostResource, initiatorResource, nil
	}

	// no initiator found
	hostResourceName, _, err := d.hostResourceName()
	if err != nil {
		return nil, nil, err
	}

	hostResource, err = d.client().GetHostByName(d.state.ShutdownCtx, hostResourceName)
	if err != nil {
		return nil, nil, err
	}

	if hostResource != nil {
		// host without initiator found
		return hostResource, nil, nil
	}

	// no host or initiator exists
	return nil, nil, nil
}

// getOrCreateHostWithInitiatorResource retrieves (or creates if missing)
// initiator and associated host resources in PowerStore associated with
// the current host, mode and transport.
func (d *powerstore) getOrCreateHostWithInitiatorResource() (*powerStoreHostResource, error) {
	hostResource, initiatorResource, err := d.getHostWithInitiatorResource()
	if err != nil {
		return nil, err
	}

	if hostResource == nil {
		// no host or initiator exists
		initiatorResource, err = d.initiator()
		if err != nil {
			return nil, err
		}

		hostResourceName, hostname, err := d.hostResourceName()
		if err != nil {
			return nil, err
		}

		hostResource = &powerStoreHostResource{
			Name:        hostResourceName,
			Description: powerStoreSprintfLimit(256, "LXD Name: %s", hostname), // maximum allowed value length for host description field is 256
			OsType:      powerStoreOsTypeEnumLinux,
			Initiators:  []*powerStoreHostInitiatorResource{initiatorResource},
		}

		err = d.client().CreateHost(d.state.ShutdownCtx, hostResource)
		if err != nil {
			return nil, err
		}

		return hostResource, nil
	}

	if initiatorResource == nil {
		// host exists but initiator is missing
		initiatorResource, err = d.initiator()
		if err != nil {
			return nil, err
		}

		err = d.client().AddInitiatorToHostByID(d.state.ShutdownCtx, hostResource.ID, initiatorResource)
		if err != nil {
			return nil, err
		}

		return d.client().GetHostByID(d.state.ShutdownCtx, hostResource.ID) // refetch to refresh the data
	}

	// host with initiator already exists
	return hostResource, nil
}

// deleteHostAndInitiatorResource deletes initiator and associated host
// resources in PowerStore if there are no mapped (attached) volumes.
func (d *powerstore) deleteHostAndInitiatorResource(hostResource *powerStoreHostResource) error {
	initiatorResource, err := d.initiator()
	if err != nil {
		return err
	}

	hostResourceName, _, err := d.hostResourceName()
	if err != nil {
		return err
	}

	if len(hostResource.MappedHosts) > 0 {
		// host has some other volumes mapped
		return nil
	}

	if len(hostResource.Initiators) > 1 {
		// host has multiple initiators associated
		return nil
	}

	if len(hostResource.Initiators) == 1 && (hostResource.Initiators[0].PortName != initiatorResource.PortName || hostResource.Initiators[0].PortType != initiatorResource.PortType) {
		// associated initiator do not matches the expected one
		return nil
	}

	if hostResource.Name != hostResourceName {
		// host is not managed by LXD
		return nil
	}

	return d.client().DeleteHostByID(d.state.ShutdownCtx, hostResource.ID)
}
