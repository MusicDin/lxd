package encryption

import (
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	audienceDevLXD = "devlxd"
)

// DevLXDAudience returns the aud claim for all DevLXD tokens issued by this cluster.
func DevLXDAudience(clusterUUID string) string {
	return strings.Join([]string{audienceDevLXD, clusterUUID}, ":")
}

// Issuer returns the iss claim for all tokens issued by this LXD cluster.
func Issuer(clusterUUID string) string {
	return strings.Join([]string{"lxd", clusterUUID}, ":")
}

// LXDAudience is the expected audience for the main API tokens (including OIDC session tokens).
// This is the same as the [Issuer].
func LXDAudience(clusterUUID string) string {
	return Issuer(clusterUUID)
}

// GetDevLXDBearerToken generates and signs a token for use with the DevLXD API. For claims it has:
// - Subject (sub): Identity identifier (UUID)
// - Issuer (iss): "lxd:{cluster_uuid}"
// - Audience (aud): "devlxd:{cluster_uuid}"
// - Not before (nbf): time now (UTC)
// - Issued at (iat): time now (UTC)
// - Expiry (exp): The given time (UTC).
func GetDevLXDBearerToken(secret []byte, identityIdentifier string, clusterUUID string, expiresAt time.Time) (string, error) {
	return getToken(secret, nil, identityIdentifier, clusterUUID, DevLXDAudience, expiresAt)
}

// getToken generates and signs a token for use with the LXD. If a salt is provided, a signing key will be generated
// using [TokenSigningKey] with the secret, otherwise the given secret will be used directly.
// For claims it has:
// - Subject (sub): Identity identifier (UUID)
// - Issuer (iss): "lxd:{cluster_uuid}"
// - Audience (aud): Result of audienceFunc(clusterUUID)
// - Not before (nbf): time now (UTC)
// - Issued at (iat): time now (UTC)
// - Expiry (exp): The given time (UTC).
func getToken(secret []byte, salt []byte, subject string, clusterUUID string, audienceFunc func(string) string, expiresAt time.Time) (string, error) {
	claims := jwt.RegisteredClaims{
		Issuer:    Issuer(clusterUUID),
		Subject:   subject,
		Audience:  jwt.ClaimStrings{audienceFunc(clusterUUID)},
		NotBefore: jwt.NewNumericDate(time.Now().UTC()),
		IssuedAt:  jwt.NewNumericDate(time.Now().UTC()),
		ExpiresAt: jwt.NewNumericDate(expiresAt.UTC()),
	}

	var err error
	signingKey := secret
	if salt != nil {
		signingKey, err = TokenSigningKey(secret, salt)
		if err != nil {
			return "", fmt.Errorf("Failed to issue token: %w", err)
		}
	}

	signedToken, err := jwt.NewWithClaims(jwt.SigningMethodHS512, claims).SignedString(signingKey)
	if err != nil {
		return "", fmt.Errorf("Failed to sign JWT: %w", err)
	}

	return signedToken, nil
}
