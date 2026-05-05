package web

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"

	"auction-site-go/internal/domain"
)

// JwtUser represents a user from a JWT token payload
type JwtUser struct {
	Subject string `json:"sub"`
	Name    string `json:"name,omitempty"`
	UType   string `json:"u_typ"`
}

// DecodeJwtUser decodes a JWT payload to a domain User
func DecodeJwtUser(jwtPayload string) (domain.User, error) {
	// Decode base64 (handle potential padding issues)
	payload := []byte(jwtPayload)
	if len(payload)%4 != 0 {
		// Add padding if necessary
		padding := 4 - len(payload)%4
		jwtPayload += strings.Repeat("=", padding)
	}

	decoded, err := base64.StdEncoding.DecodeString(jwtPayload)
	if err != nil {
		// Try URL-safe version if standard fails
		decoded, err = base64.URLEncoding.DecodeString(jwtPayload)
		if err != nil {
			// Try URL-safe version without padding
			decoded, err = base64.RawURLEncoding.DecodeString(jwtPayload)
			if err != nil {
				return domain.User{}, err
			}
		}
	}

	// Parse JSON
	var jwtUser JwtUser
	if err := json.Unmarshal(decoded, &jwtUser); err != nil {
		return domain.User{}, err
	}

	// Convert to domain User
	switch jwtUser.UType {
	case "0":
		return domain.NewBuyerOrSeller(domain.UserId(jwtUser.Subject), jwtUser.Name), nil
	case "1":
		return domain.NewSupport(domain.UserId(jwtUser.Subject)), nil
	default:
		return domain.User{}, errors.New("invalid user type")
	}
}

// ExtractJwtUser extracts a user from an Authorization header
func ExtractJwtUser(authHeader string) (domain.User, error) {
	if authHeader == "" {
		return domain.User{}, errors.New("missing authentication header")
	}

	// Split header on spaces
	parts := strings.Split(authHeader, " ")
	if len(parts) != 2 || parts[0] != "Bearer" {
		return domain.User{}, errors.New("invalid authentication header format")
	}

	// Decode JWT payload
	return DecodeJwtUser(parts[1])
}
