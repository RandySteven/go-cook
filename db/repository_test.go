package db_client

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestQueryValidation(t *testing.T) {
	if err := QueryValidation("SELECT * FROM users", selectQuery); err != nil {
		t.Fatalf("valid SELECT: %v", err)
	}
	if err := QueryValidation("INSERT INTO users VALUES (1)", insertQuery); err != nil {
		t.Fatalf("valid INSERT: %v", err)
	}
	if err := QueryValidation("UPDATE users SET name = 1", updateQuery); err != nil {
		t.Fatalf("valid UPDATE: %v", err)
	}
	if err := QueryValidation("DELETE FROM users", deleteQuery); err != nil {
		t.Fatalf("valid DELETE: %v", err)
	}
	err := QueryValidation("SELECT * FROM users", insertQuery)
	if err == nil || err.Error() != "the query command is not valid" {
		t.Fatalf("err = %v, want invalid command error", err)
	}
}

type sampleRow struct {
	ID   int64
	Name string
}

func TestSave(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t.Run("invalid query", func(t *testing.T) {
		_, err := Save[sampleRow](ctx, db, "SELECT 1")
		if err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("success", func(t *testing.T) {
		mock.ExpectPrepare("INSERT INTO users").
			ExpectExec().
			WithArgs("ada").
			WillReturnResult(sqlmock.NewResult(9, 1))

		id, err := Save[sampleRow](ctx, db, "INSERT INTO users (name) VALUES (?)", "ada")
		if err != nil {
			t.Fatal(err)
		}
		if id == nil || *id != 9 {
			t.Fatalf("id = %v, want 9", id)
		}
	})

	t.Run("prepare error", func(t *testing.T) {
		mock.ExpectPrepare("INSERT INTO users").
			WillReturnError(errors.New("prepare failed"))
		_, err := Save[sampleRow](ctx, db, "INSERT INTO users (name) VALUES (?)")
		if err == nil {
			t.Fatal("expected prepare error")
		}
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestFindAll(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t.Run("invalid query", func(t *testing.T) {
		_, err := FindAll[sampleRow](ctx, db, "DELETE FROM users")
		if err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("query error", func(t *testing.T) {
		mock.ExpectQuery("SELECT id, name FROM users").
			WillReturnError(errors.New("query failed"))
		_, err := FindAll[sampleRow](ctx, db, "SELECT id, name FROM users")
		if err == nil {
			t.Fatal("expected query error")
		}
	})

	t.Run("success", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(1), "ada")
		mock.ExpectQuery("SELECT id, name FROM users").WillReturnRows(rows)

		got, err := FindAll[sampleRow](ctx, db, "SELECT id, name FROM users")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].ID != 1 || got[0].Name != "ada" {
			t.Fatalf("got = %+v", got)
		}
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestFindByID(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t.Run("invalid query", func(t *testing.T) {
		var row sampleRow
		err := FindByID[sampleRow](ctx, db, "DELETE FROM users WHERE id = ?", 1, &row)
		if err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("nil result", func(t *testing.T) {
		mock.ExpectPrepare("SELECT id, name FROM users WHERE id = ?")
		err := FindByID[sampleRow](ctx, db, "SELECT id, name FROM users WHERE id = ?", 1, nil)
		if err == nil {
			t.Fatal("expected nil pointer error")
		}
	})

	t.Run("success", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(1), "ada")
		mock.ExpectPrepare("SELECT id, name FROM users WHERE id = ?").
			ExpectQuery().
			WithArgs(uint64(1)).
			WillReturnRows(rows)

		var row sampleRow
		if err := FindByID[sampleRow](ctx, db, "SELECT id, name FROM users WHERE id = ?", 1, &row); err != nil {
			t.Fatal(err)
		}
		if row.ID != 1 || row.Name != "ada" {
			t.Fatalf("row = %+v", row)
		}
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestUpdate(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := Update[sampleRow](ctx, db, "SELECT 1"); err == nil {
		t.Fatal("expected validation error")
	}

	mock.ExpectExec("UPDATE users SET name").
		WithArgs("grace", 1).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := Update[sampleRow](ctx, db, "UPDATE users SET name = ? WHERE id = ?", "grace", 1); err != nil {
		t.Fatal(err)
	}

	mock.ExpectExec("UPDATE users SET name").
		WillReturnError(errors.New("exec failed"))
	if err := Update[sampleRow](ctx, db, "UPDATE users SET name = ? WHERE id = ?", "x", 2); err == nil {
		t.Fatal("expected exec error")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectExec("DELETE FROM users WHERE id").
		WithArgs(uint64(3)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := Delete[sampleRow](ctx, db, "users", 3); err != nil {
		t.Fatal(err)
	}

	mock.ExpectExec("DELETE FROM users WHERE id").
		WithArgs(uint64(4)).
		WillReturnError(errors.New("exec failed"))
	if err := Delete[sampleRow](ctx, db, "users", 4); err == nil {
		t.Fatal("expected exec error")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
