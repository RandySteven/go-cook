// Package jwt_client provides JWT (JSON Web Token) generation and management
// for user authentication. It supports both access tokens (short-lived) and
// refresh tokens (long-lived) using the HS256 signing method.
package security

import (
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// JwtKey is the secret key used for signing JWT tokens.
// It is loaded from the JWT_KEY environment variable.
var JwtKey = []byte(os.Getenv("JWT_KEY"))

type (
	// JWTAccessClaim represents the claims structure for access tokens.
	// Access tokens are short-lived (1 hour) and contain user identity and authorization data.
	JWTAccessClaim struct {
		UserID   uint64   // Unique user identifier
		Username string   // User's username
		RoleID   []uint64 // List of role IDs for authorization
		IsVerify bool     // Whether the user's account is verified
		jwt.RegisteredClaims
	}

	// JWTRefreshClaim represents the claims structure for refresh tokens.
	// Refresh tokens are long-lived (10 hours) and contain minimal user identity data.
	JWTRefreshClaim struct {
		UserID uint64 // Unique user identifier
		Email  string // User's email address
		jwt.RegisteredClaims
	}
)

// GenerateTokens creates a new pair of access and refresh tokens for a user.
// The access token expires in 1 hour, and the refresh token expires in 10 hours.
// Both tokens are signed using HS256 with the JWT_KEY environment variable.
// Returns empty strings if token generation fails.
//
// Example:
//
//	accessToken, refreshToken := GenerateTokens(&user)
func GenerateTokens(userID uint64, email string) (string, string) {

	access := &JWTAccessClaim{
		UserID: userID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "Applications",
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour * 1)),
		},
	}
	//for _, role := range roleUser {
	//	access.RoleID = append(access.RoleID, role.RoleID)
	//}
	tokenAccessAlgo := jwt.NewWithClaims(jwt.SigningMethodHS256, access)
	accessToken, err := tokenAccessAlgo.SignedString(JwtKey)
	if err != nil {
		return "", ""
	}

	refresh := &JWTRefreshClaim{
		UserID: userID,
		Email:  email,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "Applications",
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour * 10)),
		},
	}
	tokenRefreshAlgo := jwt.NewWithClaims(jwt.SigningMethodHS256, refresh)
	refreshToken, err := tokenRefreshAlgo.SignedString(JwtKey)
	if err != nil {
		return "", ""
	}

	return accessToken, refreshToken
}
