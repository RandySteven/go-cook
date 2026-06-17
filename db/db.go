// Package db_client provides a PostgreSQL database client wrapper with connection
// pooling, health checks, and lifecycle management. Despite the package name,
// it currently implements a PostgreSQL connection using the lib/pq driver.
package db_client

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
)

type (
	DBConfig struct {
		Db              string
		DbUser          string
		DbPass          string
		DbName          string
		DbHost          string
		SSLMode         string
		MaxIdleConns    int
		MaxOpenConns    int
		ConnMaxLifeTime int
		ConnMaxIdleTime int
	}
	// MySQL defines the interface for database operations including connection
	// management, health checks, and migrations.
	DBClient interface {
		// Close closes the database connection and releases resources.
		Close()
		// Ping verifies the database connection is still alive.
		Ping() error
		// Client returns the underlying *sql.DB instance for direct access.
		Client() *sql.DB
	}
	// mysqlClient is the internal implementation of the MySQL interface.
	dbClient struct {
		db *sql.DB
	}
)

// Client returns the underlying *sql.DB instance for direct database access.
func (m *dbClient) Client() *sql.DB {
	return m.db
}

// Close closes the database connection and releases all associated resources.
func (m *dbClient) Close() {
	m.db.Close()
}

// Ping verifies the database connection is still alive by sending a ping request.
func (m *dbClient) Ping() error {
	return m.db.Ping()
}

// NewMYSQLClient creates a new PostgreSQL database client with connection pooling.
// It configures the connection with:
//   - MaxIdleConns: 10
//   - MaxOpenConns: 8
//   - ConnMaxLifetime: 10 minutes
//   - ConnMaxIdleTime: 8 minutes
//
// Returns an error if the connection cannot be established or ping fails.
func NewMYSQLClient(config *DBConfig) (*dbClient, error) {
	openConnectDB := ``

	switch config.Db {
	case `mysql`:
		openConnectDB = `mysql`
	case `postgresql`:
		openConnectDB = `postgres`
	default:
		return nil, fmt.Errorf(`invalid db`)
	}

	conn := fmt.Sprintf("%s://%s:%s@%s/%s?sslmode=%s",
		config.Db,
		config.DbUser,
		config.DbPass,
		config.DbHost,
		config.DbName,
		config.SSLMode,
	)
	log.Println(conn)

	db, err := sql.Open(openConnectDB, conn)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetConnMaxLifetime(time.Duration(config.ConnMaxLifeTime) * time.Minute)
	db.SetConnMaxIdleTime(time.Duration(config.ConnMaxIdleTime) * time.Minute)

	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, err
	}
	return &dbClient{
		db: db,
	}, nil
}

var _ DBClient = &dbClient{}
