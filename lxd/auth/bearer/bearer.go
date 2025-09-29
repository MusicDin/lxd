package bearer

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/canonical/lxd/lxd/auth/encryption"
	"github.com/canonical/lxd/lxd/identity"
	"github.com/canonical/lxd/lxd/request"
	"github.com/canonical/lxd/shared/api"
)

// IsDevLXDRequest returns true if the caller sent a bearer token in the Authorization header that is a JWT and appears to
// have this LXD cluster as the issuer. If true, it returns the raw token, and the subject.
func IsDevLXDRequest(r *http.Request, clusterUUID string) (isRequest bool, token string, subject string) {
	// Check Authorization header for bearer token.
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return false, "", ""
	}

	subject, _, err := isLXDToken(token, clusterUUID, encryption.DevLXDAudience(clusterUUID))
	if err != nil {
		return false, "", ""
	}

	return true, token, subject
}

// isLXDToken checks if the given token looks like it was issued by this LXD cluster and returns an error if it doesn't.
// It does not verify the token signature.
func isLXDToken(token string, clusterUUID string, expectedAudience string) (string, *time.Time, error) {
	// Check we can parse it as a JWT.
	claims := jwt.MapClaims{}
	t, _, err := jwt.NewParser().ParseUnverified(token, claims)
	if err != nil {
		return "", nil, fmt.Errorf("Failed to parse JWT: %w", err)
	}

	// There must be an issuer
	issuer, err := t.Claims.GetIssuer()
	if err != nil {
		return "", nil, fmt.Errorf("Failed to get token issuer: %w", err)
	}

	// There must be a subject
	sub, err := t.Claims.GetSubject()
	if err != nil {
		return "", nil, fmt.Errorf("Failed to get token subject: %w", err)
	}

	// Expect the issuer to be "lxd:{cluster_uuid}".
	expectIssuer := encryption.Issuer(clusterUUID)
	if issuer != expectIssuer {
		return "", nil, errors.New("Token issuer does not match")
	}

	audience, err := t.Claims.GetAudience()
	if err != nil {
		return "", nil, fmt.Errorf("Failed to get token audience: %w", err)
	}

	if len(audience) != 1 || audience[0] != expectedAudience {
		return "", nil, errors.New("Token does not contain the expected audience")
	}

	issuedAt, err := t.Claims.GetIssuedAt()
	if err != nil {
		return "", nil, fmt.Errorf("Failed to get token issued at: %w", err)
	}

	return sub, &issuedAt.Time, nil
}

// Authenticate gets a bearer identity from the cache using the given subject, and verifies that it is of the expected
// type. It then verifies that the token was signed by the secret associated with that identity, and that the token has
// not expired.
func Authenticate(token string, subject string, identityCache *identity.Cache) (*request.RequestorArgs, error) {
	// Get the identity from the cache by the subject.
	entry, err := identityCache.Get(api.AuthenticationMethodBearer, subject)
	if err != nil {
		return nil, err
	}

	err = verifyToken(token, func() ([]byte, error) {
		return entry.Secret, nil
	})
	if err != nil {
		return nil, fmt.Errorf("Failed to authenticate bearer token: %w", err)
	}

	return &request.RequestorArgs{
		Trusted:  true,
		Protocol: api.AuthenticationMethodBearer,
		Username: entry.Identifier,
	}, nil
}

// verifyToken verifies that the given token was signed by the key returned by the given key func.
func verifyToken(token string, keyFunc func() ([]byte, error)) error {
	// Always use UTC time.
	timeFunc := func() time.Time {
		return time.Now().UTC()
	}

	// Get a parser. We don't need to verify the issuer or audience because we have already inspected the payload to check this.
	// We do not use a leeway. This is so the expiry is exact. This might cause issues if there is time skew between
	// cluster members.
	parser := jwt.NewParser(
		jwt.WithIssuedAt(),           // Verify time now is not before the token was issued. The not before is automatically verified.
		jwt.WithExpirationRequired(), // Verify token has not expired.
		jwt.WithTimeFunc(timeFunc),   // Ensure the UTC time is used for comparison.
	)

	// Use the identity secret as the signing key.
	jwtKeyFunc := func(_ *jwt.Token) (any, error) {
		return keyFunc()
	}

	// Verify the token.
	_, err := parser.Parse(token, jwtKeyFunc)
	if err != nil {
		return api.StatusErrorf(http.StatusForbidden, "Token is not valid: %w", err)
	}

	return nil
}
