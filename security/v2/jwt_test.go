package v2

import (
	"testing"
	"time"

	"github.com/RandySteven/go-cook/security"
	"github.com/golang-jwt/jwt/v5"
)

func TestV2TokenDecodesWithV1Claims(t *testing.T) {
	key := []byte("test-secret-key")
	JwtKey = key
	security.JwtKey = key

	access, refresh := GenerateTokens(42, "user@example.com", 7, "admin", true)
	if access == "" || refresh == "" {
		t.Fatal("expected non-empty access and refresh tokens")
	}

	accessParsed, err := jwt.ParseWithClaims(access, &security.JWTAccessClaim{}, func(token *jwt.Token) (interface{}, error) {
		return key, nil
	})
	if err != nil {
		t.Fatalf("parse access token with v1 claims: %v", err)
	}
	accessClaims := accessParsed.Claims.(*security.JWTAccessClaim)
	if accessClaims.UserID != 42 {
		t.Fatalf("v1 access UserID = %d, want 42", accessClaims.UserID)
	}
	if !accessClaims.IsVerify {
		t.Fatal("v1 access IsVerify = false, want true")
	}

	refreshParsed, err := jwt.ParseWithClaims(refresh, &security.JWTRefreshClaim{}, func(token *jwt.Token) (interface{}, error) {
		return key, nil
	})
	if err != nil {
		t.Fatalf("parse refresh token with v1 claims: %v", err)
	}
	refreshClaims := refreshParsed.Claims.(*security.JWTRefreshClaim)
	if refreshClaims.UserID != 42 {
		t.Fatalf("v1 refresh UserID = %d, want 42", refreshClaims.UserID)
	}
	if refreshClaims.Email != "user@example.com" {
		t.Fatalf("v1 refresh Email = %q, want user@example.com", refreshClaims.Email)
	}
}

func TestV2ParsesV1Token(t *testing.T) {
	key := []byte("test-secret-key")
	JwtKey = key
	security.JwtKey = key

	access, refresh := security.GenerateTokens(42, "user@example.com")

	accessParsed, err := jwt.ParseWithClaims(access, &JWTAccessClaim{}, func(token *jwt.Token) (interface{}, error) {
		return key, nil
	})
	if err != nil {
		t.Fatalf("parse v1 access token: %v", err)
	}
	accessClaims := accessParsed.Claims.(*JWTAccessClaim)
	if accessClaims.ID != 42 {
		t.Fatalf("access ID = %d, want 42", accessClaims.ID)
	}
	if accessClaims.ExpiresAt == nil || accessClaims.ExpiresAt.Time.Before(time.Now()) {
		t.Fatal("access token should expire in the future")
	}

	refreshParsed, err := jwt.ParseWithClaims(refresh, &JWTRefreshClaim{}, func(token *jwt.Token) (interface{}, error) {
		return key, nil
	})
	if err != nil {
		t.Fatalf("parse v1 refresh token: %v", err)
	}
	refreshClaims := refreshParsed.Claims.(*JWTRefreshClaim)
	if refreshClaims.ID != 42 {
		t.Fatalf("refresh ID = %d, want 42", refreshClaims.ID)
	}
	if refreshClaims.Email != "user@example.com" {
		t.Fatalf("refresh Email = %q, want user@example.com", refreshClaims.Email)
	}
}
