// Package mysql_client provides a PostgreSQL database client wrapper with connection
// pooling, health checks, and lifecycle management. Despite the package name,
// it currently implements a PostgreSQL connection using the lib/pq driver.
package mysql_client

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
)

type (
	PostgresConfig struct {
		DbUser string
		DbPass string
		DbName string
		DbHost string
	}
	// MySQL defines the interface for database operations including connection
	// management, health checks, and migrations.
	MySQL interface {
		// Close closes the database connection and releases resources.
		Close()
		// Ping verifies the database connection is still alive.
		Ping() error
		// Client returns the underlying *sql.DB instance for direct access.
		Client() *sql.DB
	}
	// mysqlClient is the internal implementation of the MySQL interface.
	mysqlClient struct {
		db *sql.DB
	}
)

// Client returns the underlying *sql.DB instance for direct database access.
func (m *mysqlClient) Client() *sql.DB {
	return m.db
}

// Close closes the database connection and releases all associated resources.
func (m *mysqlClient) Close() {
	m.db.Close()
}

// Ping verifies the database connection is still alive by sending a ping request.
func (m *mysqlClient) Ping() error {
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
func NewMYSQLClient(config *PostgresConfig) (*mysqlClient, error) {
	conn := fmt.Sprintf("postgresql://%s:%s@%s/%s?sslmode=require",
		config.DbUser,
		config.DbPass,
		config.DbHost,
		config.DbName,
	)
	log.Println(conn)
	db, err := sql.Open("postgres", conn)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(8)
	db.SetConnMaxLifetime(10 * time.Minute)
	db.SetConnMaxIdleTime(8 * time.Minute)
	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, err
	}
	return &mysqlClient{
		db: db,
	}, nil
}

var _ MySQL = &mysqlClient{}
