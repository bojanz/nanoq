package nanoq_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bojanz/nanoq"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/oklog/ulid/v2"
	"github.com/rs/zerolog"
)

func Test_NewTask(t *testing.T) {
	t.Run("empty_task", func(t *testing.T) {
		task := nanoq.NewTask("my-type", nil)

		if _, err := ulid.ParseStrict(task.ID); err != nil {
			t.Errorf("id: %v", err)
		}
		if task.Type != "my-type" {
			t.Errorf("type: got %q, want %q", task.Type, "my-type")
		}
		if !slices.Equal(task.Payload, []byte("{}")) {
			t.Errorf("payload: got %q, want %q", task.Payload, []byte("{}"))
		}
		if task.MaxRetries != 10 {
			t.Errorf("max retries: got %v, want %v", task.MaxRetries, 10)
		}
		if task.CreatedAt.IsZero() {
			t.Errorf("created_at must not be empty")
		}
		if task.ScheduledAt.IsZero() {
			t.Errorf("scheduled_at must not be empty")
		}
		if !task.CreatedAt.Equal(task.ScheduledAt) {
			t.Errorf("created_at %q does not match scheduled_at %q", task.CreatedAt, task.ScheduledAt)
		}
		if task.Fingerprint != "25c084d0" {
			t.Errorf("fingerprint: got %q, want %q", task.Fingerprint, "25c084d0")
		}
	})

	t.Run("payload_and_options", func(t *testing.T) {
		payload := []byte(`{"product_id": "123", "user_id": "456"}`)
		scheduledAt := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
		task := nanoq.NewTask("my-type", payload, nanoq.WithMaxRetries(2), nanoq.WithScheduledAt(scheduledAt))

		if _, err := ulid.ParseStrict(task.ID); err != nil {
			t.Errorf("id: %v", err)
		}
		if task.Type != "my-type" {
			t.Errorf("type: got %q, want %q", task.Type, "my-type")
		}
		if !slices.Equal(task.Payload, payload) {
			t.Errorf("payload: got %q, want %q", task.Payload, payload)
		}
		if task.MaxRetries != 2 {
			t.Errorf("max retries: got %v, want %v", task.MaxRetries, 2)
		}
		if task.CreatedAt.IsZero() {
			t.Errorf("created_at must not be empty")
		}
		if !task.ScheduledAt.Equal(scheduledAt) {
			t.Errorf("created_at: got %q want %q", task.ScheduledAt, scheduledAt)
		}
		if task.Fingerprint != "3f16b1c4" {
			t.Errorf("fingerprint: got %q, want %q", task.Fingerprint, "3f16b1c4")
		}
	})

	t.Run("custom_fingerprint", func(t *testing.T) {
		payload := []byte(`{"product_id": "123", "user_id": "456"}`)
		fingerprintData := []byte(`{"product_id": "123"}`)
		task := nanoq.NewTask("my-type", payload, nanoq.WithFingerprintData(fingerprintData))

		if task.Type != "my-type" {
			t.Errorf("type: got %q, want %q", task.Type, "my-type")
		}
		if !slices.Equal(task.Payload, payload) {
			t.Errorf("payload: got %q, want %q", task.Payload, payload)
		}
		if task.Fingerprint != "a48cb4c4" {
			t.Errorf("fingerprint: got %q, want %q", task.Fingerprint, "a48cb4c4")
		}
	})
}

func TestClient_CreateTask(t *testing.T) {
	ctx := context.Background()
	db, mock, _ := sqlmock.New()
	defer db.Close()
	client := nanoq.NewClient(sqlx.NewDb(db, "sqlmock"))
	task := nanoq.NewTask("my-type", nil)

	t.Run("success", func(t *testing.T) {
		mock.ExpectBegin()
		mock.ExpectExec(`INSERT INTO tasks(.+) VALUES(.+)`).
			WithArgs(task.ID, task.Fingerprint, task.Type, task.Payload, task.Retries, task.MaxRetries, task.CreatedAt, task.ScheduledAt).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		client.RunTransaction(ctx, func(tx *sqlx.Tx) error {
			return client.CreateTask(ctx, tx, task)
		})

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("duplicate", func(t *testing.T) {
		mock.ExpectBegin()
		mock.ExpectExec(`INSERT INTO tasks(.+) VALUES(.+)`).
			WithArgs(task.ID, task.Fingerprint, task.Type, task.Payload, task.Retries, task.MaxRetries, task.CreatedAt, task.ScheduledAt).
			WillReturnError(&mysql.MySQLError{Number: 1022})
		mock.ExpectRollback()

		err := client.RunTransaction(ctx, func(tx *sqlx.Tx) error {
			return client.CreateTask(ctx, tx, task)
		})
		if err != nanoq.ErrDuplicateTask {
			t.Errorf("got %v, want ErrDuplicateTask", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
}

func TestProcessor_Run(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	client := nanoq.NewClient(sqlx.NewDb(db, "sqlmock"))
	processor := nanoq.NewProcessor(client, zerolog.Nop())
	processor.Handle("my-type", func(ctx context.Context, task nanoq.Task) error {
		// Fail the task once.
		if task.Retries == 0 {
			return errors.New("temporary error")
		}
		return nil
	})
	errorHandlerCalled := 0
	processor.OnError(func(ctx context.Context, task nanoq.Task, err error) {
		errorHandlerCalled++
	})

	// First task claim and retry.
	mock.ExpectBegin()
	rows := sqlmock.NewRows([]string{"id", "fingerprint", "type", "payload", "retries", "max_retries", "created_at", "scheduled_at"}).
		AddRow("01HQJHTZCAT5WDCGVTWJ640VMM", "25c084d0", "my-type", "{}", "0", "1", time.Now(), time.Now())
	mock.ExpectQuery(`SELECT \* FROM tasks WHERE(.+)`).WillReturnRows(rows)

	mock.ExpectExec("UPDATE tasks SET retries = (.+), scheduled_at = (.+) WHERE id = (.+)").WithArgs(1, sqlmock.AnyArg(), "01HQJHTZCAT5WDCGVTWJ640VMM").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	// Second task claim and deletion (due to success).
	mock.ExpectBegin()
	rows = sqlmock.NewRows([]string{"id", "fingerprint", "type", "payload", "retries", "max_retries", "created_at", "scheduled_at"}).
		AddRow("01HQJHTZCAT5WDCGVTWJ640VMM", "25c084d0", "my-type", "{}", "1", "1", time.Now(), time.Now())
	mock.ExpectQuery(`SELECT \* FROM tasks WHERE(.+)`).WillReturnRows(rows)

	mock.ExpectExec("DELETE FROM tasks WHERE id = (.+)").WithArgs("01HQJHTZCAT5WDCGVTWJ640VMM").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	ctx, cancel := context.WithCancel(context.Background())
	go processor.Run(ctx, 1, 1*time.Second)
	time.Sleep(1 * time.Second)
	cancel()

	err := mock.ExpectationsWereMet()
	if err != nil {
		t.Error(err)
	}

	if errorHandlerCalled != 1 {
		t.Errorf("erorr handler called %v times instead of %v", errorHandlerCalled, 1)
	}
}

func TestProcessor_Run_RetriesExhausted(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	client := nanoq.NewClient(sqlx.NewDb(db, "sqlmock"))
	processor := nanoq.NewProcessor(client, zerolog.Nop())
	processor.Handle("my-type", func(ctx context.Context, task nanoq.Task) error {
		return errors.New("temporary error")
	})
	errorHandlerCalled := 0
	processor.OnError(func(ctx context.Context, task nanoq.Task, err error) {
		errorHandlerCalled++
	})

	// First task claim and retry.
	mock.ExpectBegin()
	rows := sqlmock.NewRows([]string{"id", "fingerprint", "type", "payload", "retries", "max_retries", "created_at", "scheduled_at"}).
		AddRow("01HQJHTZCAT5WDCGVTWJ640VMM", "25c084d0", "my-type", "{}", "0", "1", time.Now(), time.Now())
	mock.ExpectQuery(`SELECT \* FROM tasks WHERE(.+)`).WillReturnRows(rows)

	mock.ExpectExec("UPDATE tasks SET retries = (.+), scheduled_at = (.+) WHERE id = (.+)").WithArgs(1, sqlmock.AnyArg(), "01HQJHTZCAT5WDCGVTWJ640VMM").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	// Second task claim and deletion (due to exhausted retries).
	mock.ExpectBegin()
	rows = sqlmock.NewRows([]string{"id", "fingerprint", "type", "payload", "retries", "max_retries", "created_at", "scheduled_at"}).
		AddRow("01HQJHTZCAT5WDCGVTWJ640VMM", "25c084d0", "my-type", "{}", "1", "1", time.Now(), time.Now())
	mock.ExpectQuery(`SELECT \* FROM tasks WHERE(.+)`).WillReturnRows(rows)

	mock.ExpectExec("DELETE FROM tasks WHERE id = (.+)").WithArgs("01HQJHTZCAT5WDCGVTWJ640VMM").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	ctx, cancel := context.WithCancel(context.Background())
	go processor.Run(ctx, 1, 1*time.Second)
	time.Sleep(1 * time.Second)
	cancel()

	err := mock.ExpectationsWereMet()
	if err != nil {
		t.Error(err)
	}

	if errorHandlerCalled != 2 {
		t.Errorf("erorr handler called %v times instead of %v", errorHandlerCalled, 2)
	}
}

func TestProcessor_Run_SkipRetry(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	client := nanoq.NewClient(sqlx.NewDb(db, "sqlmock"))
	processor := nanoq.NewProcessor(client, zerolog.Nop())
	processor.Handle("my-type", func(ctx context.Context, task nanoq.Task) error {
		return fmt.Errorf("something terrible happened: %w", nanoq.ErrSkipRetry)
	})
	errorHandlerCalled := 0
	processor.OnError(func(ctx context.Context, task nanoq.Task, err error) {
		if !errors.Is(err, nanoq.ErrSkipRetry) {
			t.Errorf("error handler called with unexpected error: %v", err)
		}
		errorHandlerCalled++
	})

	// Task claim and deletion.
	mock.ExpectBegin()
	rows := sqlmock.NewRows([]string{"id", "fingerprint", "type", "payload", "retries", "max_retries", "created_at", "scheduled_at"}).
		AddRow("01HQJHTZCAT5WDCGVTWJ640VMM", "25c084d0", "my-type", "{}", "0", "1", time.Now(), time.Now())
	mock.ExpectQuery(`SELECT \* FROM tasks WHERE(.+)`).WillReturnRows(rows)

	mock.ExpectExec("DELETE FROM tasks WHERE id = (.+)").WithArgs("01HQJHTZCAT5WDCGVTWJ640VMM").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	ctx, cancel := context.WithCancel(context.Background())
	go processor.Run(ctx, 1, 1*time.Second)
	time.Sleep(1 * time.Second)
	cancel()

	err := mock.ExpectationsWereMet()
	if err != nil {
		t.Error(err)
	}

	if errorHandlerCalled != 1 {
		t.Errorf("erorr handler called %v times instead of %v", errorHandlerCalled, 1)
	}
}

func TestProcessor_Run_Middleware(t *testing.T) {
	// Used to store and retrieve the context value.
	type contextKey string

	db, mock, _ := sqlmock.New()
	defer db.Close()
	client := nanoq.NewClient(sqlx.NewDb(db, "sqlmock"))
	processor := nanoq.NewProcessor(client, zerolog.Nop())
	processor.Use(func(next nanoq.Handler) nanoq.Handler {
		return func(ctx context.Context, t nanoq.Task) error {
			middlewareValue := ctx.Value(contextKey("middleware"))
			if middlewareValue == nil {
				middlewareValue = make([]string, 0, 10)
			}
			middleware := append(middlewareValue.([]string), "first_global")
			ctx = context.WithValue(ctx, contextKey("middleware"), middleware)

			return next(ctx, t)
		}
	})
	processor.Use(func(next nanoq.Handler) nanoq.Handler {
		return func(ctx context.Context, t nanoq.Task) error {
			middlewareValue := ctx.Value(contextKey("middleware"))
			if middlewareValue == nil {
				middlewareValue = make([]string, 0, 10)
			}
			middleware := append(middlewareValue.([]string), "second_global")
			ctx = context.WithValue(ctx, contextKey("middleware"), middleware)

			return next(ctx, t)
		}
	})

	firstHandlerMiddleware := func(next nanoq.Handler) nanoq.Handler {
		return func(ctx context.Context, t nanoq.Task) error {
			middlewareValue := ctx.Value(contextKey("middleware"))
			if middlewareValue == nil {
				middlewareValue = make([]string, 0, 10)
			}
			middleware := append(middlewareValue.([]string), "first")
			ctx = context.WithValue(ctx, contextKey("middleware"), middleware)

			return next(ctx, t)
		}
	}
	secondHandlerMiddleware := func(next nanoq.Handler) nanoq.Handler {
		return func(ctx context.Context, t nanoq.Task) error {
			middlewareValue := ctx.Value(contextKey("middleware"))
			if middlewareValue == nil {
				middlewareValue = make([]string, 0, 10)
			}
			middleware := append(middlewareValue.([]string), "second")
			ctx = context.WithValue(ctx, contextKey("middleware"), middleware)

			return next(ctx, t)
		}
	}
	handler := func(ctx context.Context, task nanoq.Task) error {
		middlewareValue := ctx.Value(contextKey("middleware"))
		middleware := middlewareValue.([]string)
		wantMiddleware := []string{"first_global", "second_global", "first", "second"}
		if !slices.Equal(middleware, wantMiddleware) {
			t.Errorf("got %v, want %v", middleware, wantMiddleware)
		}

		return nil
	}
	processor.Handle("my-type", handler, firstHandlerMiddleware, secondHandlerMiddleware)

	// Task claim and deletion.
	mock.ExpectBegin()
	rows := sqlmock.NewRows([]string{"id", "fingerprint", "type", "payload", "retries", "max_retries", "created_at", "scheduled_at"}).
		AddRow("01HQJHTZCAT5WDCGVTWJ640VMM", "25c084d0", "my-type", "{}", "0", "1", time.Now(), time.Now())
	mock.ExpectQuery(`SELECT \* FROM tasks WHERE(.+)`).WillReturnRows(rows)

	mock.ExpectExec("DELETE FROM tasks WHERE id = (.+)").WithArgs("01HQJHTZCAT5WDCGVTWJ640VMM").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	ctx, cancel := context.WithCancel(context.Background())
	go processor.Run(ctx, 1, 1*time.Second)
	time.Sleep(1 * time.Second)
	cancel()

	err := mock.ExpectationsWereMet()
	if err != nil {
		t.Error(err)
	}
}
