package v2

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var JwtKey = []byte(os.Getenv("JWT_KEY"))
var JwtExpire = []byte(os.Getenv("JWT_EXPIRE"))
var JwtRefreshExpire = []byte(os.Getenv("JWT_REFRESH_EXPIRE"))

type (
	JWTAccessClaim struct {
		ID           uint64 `json:"id"`
		OnboardingID uint64 `json:"onboarding_id"`
		Email        string `json:"email"`
		Role         string `json:"role"`
		IsVerify     bool   `json:"is_verify"`
		jwt.RegisteredClaims
	}

	JWTRefreshClaim struct {
		ID    uint64 `json:"id"`
		Email string `json:"email"`
		jwt.RegisteredClaims
	}
)

func GenerateTokens(id uint64, email string, onboardingID uint64, role string, isVerify bool) (string, string) {
	jwtExpire := string(JwtExpire)
	if jwtExpire == "" {
		jwtExpire = "1h"
	}

	jwtRefreshExpire := string(JwtRefreshExpire)
	if jwtRefreshExpire == "" {
		jwtRefreshExpire = "10h"
	}

	accessTokenExpire, err := time.ParseDuration(jwtExpire)
	if err != nil {
		log.Printf("parse jwt expire: %v", err)
		return "", ""
	}

	refreshTokenExpire, err := time.ParseDuration(jwtRefreshExpire)
	if err != nil {
		log.Printf("parse jwt refresh expire: %v", err)
		return "", ""
	}

	accessToken := jwt.NewWithClaims(jwt.SigningMethodHS256, JWTAccessClaim{
		ID:           id,
		OnboardingID: onboardingID,
		Email:        email,
		Role:         role,
		IsVerify:     isVerify,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(accessTokenExpire)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Issuer:    "auth-service",
			Subject:   "access-token",
		},
	})

	refreshToken := jwt.NewWithClaims(jwt.SigningMethodHS256, JWTRefreshClaim{
		ID:    id,
		Email: email,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(refreshTokenExpire)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Issuer:    "auth-service",
			Subject:   "refresh-token",
		},
	})

	accessTokenStr, err := accessToken.SignedString(JwtKey)
	if err != nil {
		return "", ""
	}
	refreshTokenStr, err := refreshToken.SignedString(JwtKey)
	if err != nil {
		return "", ""
	}
	return accessTokenStr, refreshTokenStr
}

// accessWire is the on-the-wire access token. v2 names stay canonical.
// UserID and IsVerify are the claim names security.JWTAccessClaim marshals,
// so tokens issued here still decode with the v1 claim type.
type accessWire struct {
	ID           uint64 `json:"id"`
	OnboardingID uint64 `json:"onboarding_id,omitempty"`
	Email        string `json:"email,omitempty"`
	Role         string `json:"role,omitempty"`
	IsVerify     bool   `json:"is_verify"`
	UserID       uint64 `json:"UserID"`
	LegacyVerify bool   `json:"IsVerify"`
	jwt.RegisteredClaims
}

func (c JWTAccessClaim) MarshalJSON() ([]byte, error) {
	return json.Marshal(accessWire{
		ID:               c.ID,
		OnboardingID:     c.OnboardingID,
		Email:            c.Email,
		Role:             c.Role,
		IsVerify:         c.IsVerify,
		UserID:           c.ID,
		LegacyVerify:     c.IsVerify,
		RegisteredClaims: c.RegisteredClaims,
	})
}

func (c *JWTAccessClaim) UnmarshalJSON(data []byte) error {
	var raw accessWire
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	id := raw.ID
	if id == 0 {
		id = raw.UserID
	}
	*c = JWTAccessClaim{
		ID:               id,
		OnboardingID:     raw.OnboardingID,
		Email:            raw.Email,
		Role:             raw.Role,
		IsVerify:         raw.IsVerify || raw.LegacyVerify,
		RegisteredClaims: raw.RegisteredClaims,
	}
	return nil
}

// refreshWire keeps security.JWTRefreshClaim's UserID key beside the v2 id.
// Email already matches v1: encoding/json accepts "Email" and "email".
type refreshWire struct {
	ID     uint64 `json:"id"`
	Email  string `json:"email"`
	UserID uint64 `json:"UserID"`
	jwt.RegisteredClaims
}

func (c JWTRefreshClaim) MarshalJSON() ([]byte, error) {
	return json.Marshal(refreshWire{
		ID:               c.ID,
		Email:            c.Email,
		UserID:           c.ID,
		RegisteredClaims: c.RegisteredClaims,
	})
}

func (c *JWTRefreshClaim) UnmarshalJSON(data []byte) error {
	var raw refreshWire
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	id := raw.ID
	if id == 0 {
		id = raw.UserID
	}
	*c = JWTRefreshClaim{
		ID:               id,
		Email:            raw.Email,
		RegisteredClaims: raw.RegisteredClaims,
	}
	return nil
}
