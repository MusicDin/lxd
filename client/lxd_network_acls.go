package lxd

import (
	"io"
	"net/http"
	"net/url"

	"github.com/canonical/lxd/shared/api"
)

// GetNetworkACLNames returns a list of network ACL names.
func (r *ProtocolLXD) GetNetworkACLNames() ([]string, error) {
	err := r.CheckExtension("network_acl")
	if err != nil {
		return nil, err
	}

	// Fetch the raw URL values.
	urls := []string{}
	baseURL := "/network-acls"
	_, err = r.queryStruct("GET", baseURL, nil, "", &urls)
	if err != nil {
		return nil, err
	}

	// Parse it.
	return urlsToResourceNames(baseURL, urls...)
}

// GetNetworkACLs returns a list of Network ACL structs.
func (r *ProtocolLXD) GetNetworkACLs() ([]api.NetworkACL, error) {
	err := r.CheckExtension("network_acl")
	if err != nil {
		return nil, err
	}

	acls := []api.NetworkACL{}

	// Fetch the raw value.
	_, err = r.queryStruct("GET", "/network-acls?recursion=1", nil, "", &acls)
	if err != nil {
		return nil, err
	}

	return acls, nil
}

// GetNetworkACL returns a Network ACL entry for the provided name.
func (r *ProtocolLXD) GetNetworkACL(name string) (*api.NetworkACL, string, error) {
	err := r.CheckExtension("network_acl")
	if err != nil {
		return nil, "", err
	}

	acl := api.NetworkACL{}

	// Fetch the raw value.
	etag, err := r.queryStruct("GET", "/network-acls/"+url.PathEscape(name), nil, "", &acl)
	if err != nil {
		return nil, "", err
	}

	return &acl, etag, nil
}

// GetNetworkACLLogfile returns a reader for the ACL log file.
//
// Note that it's the caller's responsibility to close the returned ReadCloser.
func (r *ProtocolLXD) GetNetworkACLLogfile(name string) (io.ReadCloser, error) {
	err := r.CheckExtension("network_acl_log")
	if err != nil {
		return nil, err
	}

	// Prepare the HTTP request
	url := r.httpBaseURL.String() + "/1.0/network-acls/" + url.PathEscape(name) + "/log"
	url, err = r.setQueryAttributes(url)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	// Send the request
	resp, err := r.DoHTTP(req)
	if err != nil {
		return nil, err
	}

	// Check the return value for a cleaner error
	if resp.StatusCode != http.StatusOK {
		_, _, err := lxdParseResponse(resp)
		if err != nil {
			return nil, err
		}
	}

	return resp.Body, err
}

// CreateNetworkACL defines a new network ACL using the provided struct.
func (r *ProtocolLXD) CreateNetworkACL(acl api.NetworkACLsPost) error {
	err := r.CheckExtension("network_acl")
	if err != nil {
		return err
	}

	// Send the request.
	_, _, err = r.query("POST", "/network-acls", acl, "")
	if err != nil {
		return err
	}

	return nil
}

// UpdateNetworkACL updates the network ACL to match the provided struct.
func (r *ProtocolLXD) UpdateNetworkACL(name string, acl api.NetworkACLPut, ETag string) error {
	err := r.CheckExtension("network_acl")
	if err != nil {
		return err
	}

	// Send the request.
	_, _, err = r.query("PUT", "/network-acls/"+url.PathEscape(name), acl, ETag)
	if err != nil {
		return err
	}

	return nil
}

// RenameNetworkACL renames an existing network ACL entry.
func (r *ProtocolLXD) RenameNetworkACL(name string, acl api.NetworkACLPost) error {
	err := r.CheckExtension("network_acl")
	if err != nil {
		return err
	}

	// Send the request.
	_, _, err = r.query("POST", "/network-acls/"+url.PathEscape(name), acl, "")
	if err != nil {
		return err
	}

	return nil
}

// DeleteNetworkACL deletes an existing network ACL.
func (r *ProtocolLXD) DeleteNetworkACL(name string) error {
	err := r.CheckExtension("network_acl")
	if err != nil {
		return err
	}

	// Send the request.
	_, _, err = r.query("DELETE", "/network-acls/"+url.PathEscape(name), nil, "")
	if err != nil {
		return err
	}

	return nil
}
