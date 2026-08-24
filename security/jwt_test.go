package security

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestGenerateTokens(t *testing.T) {
	JwtKey = []byte("test-secret-key")

	access, refresh := GenerateTokens(42, "user@example.com")
	if access == "" || refresh == "" {
		t.Fatal("expected non-empty access and refresh tokens")
	}
	if access == refresh {
		t.Fatal("access and refresh tokens should differ")
	}

	accessParsed, err := jwt.ParseWithClaims(access, &JWTAccessClaim{}, func(token *jwt.Token) (interface{}, error) {
		return JwtKey, nil
	})
	if err != nil {
		t.Fatalf("parse access token: %v", err)
	}
	accessClaims, ok := accessParsed.Claims.(*JWTAccessClaim)
	if !ok || !accessParsed.Valid {
		t.Fatal("expected valid access claims")
	}
	if accessClaims.UserID != 42 {
		t.Fatalf("access UserID = %d, want 42", accessClaims.UserID)
	}
	if accessClaims.Issuer != "Applications" {
		t.Fatalf("access Issuer = %q, want Applications", accessClaims.Issuer)
	}
	if accessClaims.ExpiresAt == nil || accessClaims.ExpiresAt.Time.Before(time.Now()) {
		t.Fatal("access token should expire in the future")
	}

	refreshParsed, err := jwt.ParseWithClaims(refresh, &JWTRefreshClaim{}, func(token *jwt.Token) (interface{}, error) {
		return JwtKey, nil
	})
	if err != nil {
		t.Fatalf("parse refresh token: %v", err)
	}
	refreshClaims, ok := refreshParsed.Claims.(*JWTRefreshClaim)
	if !ok || !refreshParsed.Valid {
		t.Fatal("expected valid refresh claims")
	}
	if refreshClaims.UserID != 42 {
		t.Fatalf("refresh UserID = %d, want 42", refreshClaims.UserID)
	}
	if refreshClaims.Email != "user@example.com" {
		t.Fatalf("refresh Email = %q, want user@example.com", refreshClaims.Email)
	}
}

func TestGenerateTokensRejectsWrongKey(t *testing.T) {
	JwtKey = []byte("signing-key")
	access, _ := GenerateTokens(1, "a@b.c")

	_, err := jwt.Parse(access, func(token *jwt.Token) (interface{}, error) {
		return []byte("other-key"), nil
	})
	if err == nil {
		t.Fatal("expected signature error with a different key")
	}
}
