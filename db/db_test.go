package db_client

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestNewMYSQLClientInvalidDB(t *testing.T) {
	_, err := NewMYSQLClient(&DBConfig{Db: "oracle"})
	if err == nil || err.Error() != "invalid db" {
		t.Fatalf("err = %v, want invalid db", err)
	}
}

func TestNewMYSQLClientInvalidSSLMode(t *testing.T) {
	_, err := NewMYSQLClient(&DBConfig{
		Db:      "postgresql",
		SSLMode: "made-up",
	})
	if err == nil || err.Error() != "invalid sslmode" {
		t.Fatalf("err = %v, want invalid sslmode", err)
	}
}

func TestNewMYSQLClientDefaultSSLMode(t *testing.T) {
	_, err := NewMYSQLClient(&DBConfig{
		Db:     "postgresql",
		DbUser: "u",
		DbPass: "p",
		DbHost: "127.0.0.1:1",
		DbName: "db",
	})
	if err == nil {
		t.Fatal("expected ping/connect error with default sslmode=require")
	}
}

func TestNewMYSQLClientValidSSLModesRejectedAtPing(t *testing.T) {
	for _, mode := range []string{"allow", "prefer", "require", "verify-ca", "verify-full"} {
		_, err := NewMYSQLClient(&DBConfig{
			Db:      "postgresql",
			SSLMode: mode,
			DbUser:  "u",
			DbPass:  "p",
			DbHost:  "127.0.0.1:1",
			DbName:  "db",
		})
		if err == nil {
			t.Fatalf("sslmode %s: expected ping/connect error", mode)
		}
	}
}

func TestNewMYSQLClientUnknownMySQLDriver(t *testing.T) {
	_, err := NewMYSQLClient(&DBConfig{
		Db:      "mysql",
		SSLMode: "disable",
		DbUser:  "u",
		DbPass:  "p",
		DbHost:  "localhost",
		DbName:  "db",
	})
	if err == nil {
		t.Fatal("expected error because the mysql driver is not registered")
	}
}

func TestNewMYSQLClientPingFailure(t *testing.T) {
	_, err := NewMYSQLClient(&DBConfig{
		Db:      "postgresql",
		SSLMode: "disable",
		DbUser:  "u",
		DbPass:  "p",
		DbHost:  "127.0.0.1:1",
		DbName:  "db",
	})
	if err == nil {
		t.Fatal("expected ping/connect error")
	}
}

func TestDBClientPingClientClose(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}

	client := &dbClient{db: db}
	if client.Client() != db {
		t.Fatal("Client() should return the wrapped *sql.DB")
	}

	mock.ExpectPing()
	if err := client.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	mock.ExpectClose()
	client.Close()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDBClientImplementsInterface(t *testing.T) {
	var _ DBClient = &dbClient{}
}
