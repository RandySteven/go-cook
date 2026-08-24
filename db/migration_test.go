package db_client

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestInitMigrationWorkerAndRegister(t *testing.T) {
	worker := InitMigrationWorker(nil)
	if worker == nil || worker.DBClient != nil {
		t.Fatal("expected worker with nil client")
	}

	worker.RegisterMigration("CREATE TABLE a (id INT)", "CREATE TABLE b (id INT)")
	if len(worker.Tables) != 2 {
		t.Fatalf("Tables len = %d, want 2", len(worker.Tables))
	}
}

func TestMigration(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	worker := InitMigrationWorker(&dbClient{db: db})
	worker.RegisterMigration("CREATE TABLE users (id INT)")

	t.Run("success", func(t *testing.T) {
		mock.ExpectExec("CREATE TABLE users").
			WillReturnResult(sqlmock.NewResult(0, 0))
		if err := worker.Migration(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("exec error", func(t *testing.T) {
		worker.Tables = []string{"CREATE TABLE fail (id INT)"}
		mock.ExpectExec("CREATE TABLE fail").
			WillReturnError(errors.New("ddl failed"))
		if err := worker.Migration(context.Background()); err == nil {
			t.Fatal("expected error")
		}
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
