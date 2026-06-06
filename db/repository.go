package mysql_client

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
)

// SQL command constants for query validation.
const (
	selectQuery = `SELECT`
	insertQuery = `INSERT`
	updateQuery = `UPDATE`
	deleteQuery = `DELETE`
)


type (
	Many2ManyRepository[T any] interface {
		Save(ctx context.Context, entity []*T) (result *T, err error)
	}

	Saver[T any] interface {
		Save(ctx context.Context, entity *T) (result *T, err error)
	}

	Finder[T any] interface {
		FindByID(ctx context.Context, id uint64) (result *T, err error)
		FindAll(ctx context.Context, skip uint64, take uint64) (result []*T, err error)
	}

	Updater[T any] interface {
		Update(ctx context.Context, entity *T) (result *T, err error)
	}

	Deleter[T any] interface {
		DeleteByID(ctx context.Context, id uint64) (err error)
	}

	DBX func(ctx context.Context) Trigger

	Trigger interface {
		PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
		ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	}

	Transaction interface {
		RunInTx(ctx context.Context, txFunc func(ctx context.Context) (customErr error)) (customErr error)
	}

	Index interface {
		CreateIndex(ctx context.Context) (err error)
		DropIndex(ctx context.Context) (err error)
	}
)


// QueryValidation validates that a query string contains the expected SQL command.
// Returns an error if the query does not contain the specified command.
func QueryValidation(query string, command string) error {
	if !strings.Contains(query, command) {
		return fmt.Errorf(`the query command is not valid`)
	}
	return nil
}

// Save executes an INSERT query and returns the last inserted ID.
// It uses prepared statements for safe query execution.
// Type parameter T is unused but provided for consistency with other repository functions.
//
// Example:
//
//	id, err := Save[User](ctx, db, insertQuery, name, email)
func Save[T any](ctx context.Context, db Trigger, query string, requests ...any) (*uint64, error) {
	err := QueryValidation(query, insertQuery)
	if err != nil {
		return nil, err
	}

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	// Execute the insert statement
	result, err := stmt.ExecContext(ctx, requests...)
	if err != nil {
		log.Println("exec context tidak aman : ", err)
		return nil, err
	}

	// Retrieve the last inserted ID
	id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}
	uid := uint64(id)
	return &uid, nil
}

// FindAll executes a SELECT query and returns all matching rows as a slice of pointers.
// It uses reflection to dynamically scan row values into struct fields.
// Type parameter T specifies the struct type to scan results into.
// The struct fields must match the column order in the query result.
//
// Example:
//
//	users, err := FindAll[User](ctx, db, selectAllQuery)
func FindAll[T any](ctx context.Context, db Trigger, query string) (result []*T, err error) {
	requests := new(T)
	err = QueryValidation(query, selectQuery)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	typ := reflect.TypeOf(requests).Elem()
	var ptrs = make([]interface{}, typ.NumField())
	for i := range ptrs {
		ptrs[i] = reflect.New(typ.Field(i).Type).Interface()
	}

	for rows.Next() {
		request := reflect.New(typ).Elem()
		err := rows.Scan(ptrs...)
		if err != nil {
			return nil, err
		}
		for i, ptr := range ptrs {
			field := request.Field(i)
			field.Set(reflect.ValueOf(ptr).Elem())
		}
		result = append(result, request.Addr().Interface().(*T))
	}
	return result, nil
}

// Delete removes a record from the specified table by ID.
// Constructs and executes a DELETE query using the provided table name and ID.
func Delete[T any](ctx context.Context, db Trigger, table string, id uint64) (err error) {
	query := `DELETE FROM %s WHERE id = ?`
	query = fmt.Sprintf(query, table, id)
	_, err = db.ExecContext(ctx, query, id)
	if err != nil {
		return err
	}
	return nil
}

// FindByID executes a SELECT query to find a single record by ID.
// The result is scanned into the provided pointer using reflection.
// Returns sql.ErrNoRows wrapped in an error if no record is found.
//
// Example:
//
//	var user User
//	err := FindByID[User](ctx, db, selectByIDQuery, 1, &user)
func FindByID[T any](ctx context.Context, db Trigger, query string, id uint64, result *T) error {
	// Log query for debugging
	log.Println("Executing query:", strings.ReplaceAll(query, "?", fmt.Sprintf("%d", id)))

	// Validate the query to ensure it's a SELECT query
	err := QueryValidation(query, selectQuery)
	if err != nil {
		return fmt.Errorf("query validation failed: %w", err)
	}

	// Prepare the SQL statement
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare context: %w", err)
	}
	defer stmt.Close()

	// Use reflection to ensure the result is properly initialized
	if reflect.ValueOf(result).Kind() != reflect.Ptr || reflect.ValueOf(result).IsNil() {
		return fmt.Errorf("result argument must be a non-nil pointer")
	}

	// Get the underlying type and value of the result
	typ := reflect.TypeOf(result).Elem()
	val := reflect.ValueOf(result).Elem()

	var ptrs []interface{}

	// Create a slice of pointers to each field in the struct
	for i := 0; i < typ.NumField(); i++ {
		field := val.Field(i)
		ptrs = append(ptrs, field.Addr().Interface())
	}

	// Execute the query and scan the result into the struct fields
	err = stmt.QueryRowContext(ctx, id).Scan(ptrs...)
	if err != nil {
		// Handle no rows found
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("no rows found for ID %d: %w", id, err)
		}
		// Handle other scan errors
		return fmt.Errorf("failed to scan result for ID %d: %w", id, err)
	}

	return nil
}

// Update executes an UPDATE query with the provided parameters.
// Returns an error if the query is not a valid UPDATE statement or execution fails.
func Update[T any](ctx context.Context, db Trigger, query string, requests ...any) (err error) {
	err = QueryValidation(query, updateQuery)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, query, requests...)
	if err != nil {
		return err
	}
	return nil
}
