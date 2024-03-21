package nanoq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/oklog/ulid/v2"
	"github.com/rs/zerolog"
)

var (
	// ErrDuplicateTask indicates that an identical task already exists in the queue.
	// Tasks are considered identical if they have the same fingerprint.
	ErrDuplicateTask = errors.New("duplicate task")

	// ErrNoTasks indicates that no tasks are available for processing.
	ErrNoTasks = errors.New("no tasks available")

	// ErrSkipRetry indicates that the task should not be retried.
	ErrSkipRetry = errors.New("skip retry")
)

// Task represents a task.
type Task struct {
	ID          string    `db:"id"`
	Fingerprint string    `db:"fingerprint"`
	Type        string    `db:"type"`
	Payload     []byte    `db:"payload"`
	Retries     uint8     `db:"retries"`
	MaxRetries  uint8     `db:"max_retries"`
	CreatedAt   time.Time `db:"created_at"`
	ScheduledAt time.Time `db:"scheduled_at"`
}

// NewTask creates a new task.
func NewTask(taskType string, payload []byte, opts ...TaskOption) Task {
	if len(payload) == 0 {
		// Empty payloads must be valid JSON.
		payload = []byte("{}")
	}
	now := time.Now().UTC()
	t := Task{
		ID:          ulid.Make().String(),
		Type:        taskType,
		Payload:     payload,
		MaxRetries:  10,
		CreatedAt:   now,
		ScheduledAt: now,
	}
	for _, opt := range opts {
		opt(&t)
	}
	if t.Fingerprint == "" {
		t.Fingerprint = getFingerprint(t.Type, t.Payload)
	}

	return t
}

// TaskOption represents a task option.
type TaskOption func(t *Task)

// WithFingerprintData provides the data used to fingerprint the task.
//
// By default, the entire task payload is used.
func WithFingerprintData(data []byte) TaskOption {
	return func(t *Task) {
		t.Fingerprint = getFingerprint(t.Type, data)
	}
}

// WithMaxRetries allows the task to be retried the given number of times.
//
// Defaults to 10. Use 0 to disallow retries.
func WithMaxRetries(maxRetries uint8) TaskOption {
	return func(t *Task) {
		t.MaxRetries = maxRetries
	}
}

// WithScheduledAt schedules the task at the given time.
func WithScheduledAt(scheduledAt time.Time) TaskOption {
	return func(t *Task) {
		t.ScheduledAt = scheduledAt
	}
}

// WithScheduledIn schedules the task after the given duration.
func WithScheduledIn(scheduledIn time.Duration) TaskOption {
	return func(t *Task) {
		t.ScheduledAt = time.Now().UTC().Add(scheduledIn)
	}
}

// getFingerprint returns the fingerprint for the given data.
// The fingerprint is a hash, base16 encoded and 8 characters long.
func getFingerprint(taskType string, data []byte) string {
	crc32q := crc32.MakeTable(crc32.Castagnoli)
	fingerprint := crc32.Checksum(append(data, taskType...), crc32q)

	return fmt.Sprintf("%x", fingerprint)
}

// Client represents a database-backed queue client.
type Client struct {
	db *sqlx.DB
}

// NewClient creates a new client.
func NewClient(db *sqlx.DB) *Client {
	return &Client{db: db}
}

// RunTransaction runs the given function in a transaction.
//
// The transaction will be rolled back if the function returns an error.
// Otherwise, it will be committed.
func (q *Client) RunTransaction(ctx context.Context, fn func(tx *sqlx.Tx) error) error {
	tx, err := q.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}

// ClaimTask claims a task for processing.
//
// The claim is valid until the transaction is committed or rolled back.
func (q *Client) ClaimTask(ctx context.Context, tx *sqlx.Tx) (Task, error) {
	t := Task{}
	err := tx.GetContext(ctx, &t, `SELECT * FROM tasks WHERE scheduled_at <= UTC_TIMESTAMP() ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED`)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return t, ErrNoTasks
		}
		return t, err
	}

	return t, nil
}

// CreateTask creates the given task.
//
// Returns ErrDuplicateTask if a task with the same fingerprint already exists.
func (q *Client) CreateTask(ctx context.Context, tx *sqlx.Tx, t Task) error {
	_, err := tx.NamedExecContext(ctx, `
		INSERT INTO tasks
			(id, fingerprint, type, payload, retries, max_retries, created_at, scheduled_at)
		VALUES
			(:id, :fingerprint, :type, :payload, :retries, :max_retries, :created_at, :scheduled_at)`, t)
	if err != nil {
		if isConflictError(err) {
			return ErrDuplicateTask
		}
		return err
	}

	return nil
}

// UpdateTask updates the given task.
func (q *Client) UpdateTask(ctx context.Context, tx *sqlx.Tx, t Task) error {
	_, err := tx.NamedExecContext(ctx, `UPDATE tasks SET retries = :retries, scheduled_at = :scheduled_at WHERE id = :id`, t)
	if err != nil {
		return err
	}

	return nil
}

// DeleteTask deletes the given task.
func (q *Client) DeleteTask(ctx context.Context, tx *sqlx.Tx, t Task) error {
	_, err := tx.NamedExecContext(ctx, `DELETE FROM tasks WHERE id = :id`, t)
	if err != nil {
		return err
	}

	return nil
}

// isConflictError checks whether the given error is a MySQL / MariaDB conflict error.
func isConflictError(err error) bool {
	if sqlErr, ok := err.(*mysql.MySQLError); ok {
		switch sqlErr.Number {
		case 1022, 1062, 1088, 1092, 1586, 1761, 1762:
			return true
		}
	}
	return false
}

type (
	// ErrorHandler handles a task processing error.
	ErrorHandler func(ctx context.Context, t Task, err error)

	// Handler processes tasks of a specific type.
	//
	// Should return nil if the processing of a task was successful.
	// If an error is returned, the task will be retried after a delay, until max retries are reached.
	//
	// Return an ErrSkipRetry error to skip any remaining retries and remove the task from the queue.
	Handler func(ctx context.Context, t Task) error

	// Middleware wrap a handler in order to run logic before/after it.
	Middleware func(next Handler) Handler
)

// Processor represents the queue processor.
type Processor struct {
	client *Client
	logger zerolog.Logger

	errorHandler ErrorHandler
	handlers     map[string]Handler
	middleware   []Middleware

	done atomic.Bool
}

// NewProcessor creates a new processor.
func NewProcessor(client *Client, logger zerolog.Logger) *Processor {
	return &Processor{
		client:     client,
		logger:     logger,
		handlers:   make(map[string]Handler),
		middleware: make([]Middleware, 0),
	}
}

// OnError registers the given error handler.
func (p *Processor) OnError(h ErrorHandler) {
	p.errorHandler = h
}

// Use registers a middleware that runs for all task types.
func (p *Processor) Use(m Middleware) {
	p.middleware = append(p.middleware, m)
}

// Handle registers the handler for a task type.
func (p *Processor) Handle(taskType string, h Handler, ms ...Middleware) {
	// Wrap the handler with the passed middleware.
	for _, m := range ms {
		h = m(h)
	}
	p.handlers[taskType] = h
}

// Run starts the processor and blocks until a shutdown signal (SIGINT/SIGTERM) is received.
//
// Once the shutdown signal is received, workers stop claiming new tasks.
// The tasks that are still being processed are then given until shutdownTimeout to complete,
// after which they are stopped (via canceled context).
func (p *Processor) Run(ctx context.Context, concurrency int, shutdownTimeout time.Duration) {
	processorCtx, cancel := context.WithCancelCause(ctx)
	go func() {
		shutdownCh := make(chan os.Signal, 1)
		signal.Notify(shutdownCh, os.Interrupt, syscall.SIGTERM)

		select {
		case <-shutdownCh:
		case <-ctx.Done():
		}

		p.logger.Info().Str("timeout", shutdownTimeout.String()).Msg("Shutting down processor")
		p.done.Store(true)
		time.AfterFunc(shutdownTimeout, func() {
			cancel(errors.New("shutdown timeout reached"))
		})
	}()

	p.logger.Info().Int("concurrency", concurrency).Msg("Starting processor")
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)

		go func() {
			for !p.done.Load() {
				err := p.process(processorCtx)
				if err != nil {
					if errors.Is(err, ErrNoTasks) {
						// The queue is empty. Wait a second before trying again.
						time.Sleep(1 * time.Second)
						continue
					}
					p.logger.Error().Err(err).Msg("Could not process task")
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

// process claims a single task and processes it.
func (p *Processor) process(ctx context.Context) error {
	return p.client.RunTransaction(ctx, func(tx *sqlx.Tx) error {
		t, err := p.client.ClaimTask(ctx, tx)
		if err != nil {
			return fmt.Errorf("claim task: %w", err)
		}

		h, ok := p.handlers[t.Type]
		if !ok {
			h = func(ctx context.Context, t Task) error {
				return fmt.Errorf("no handler not found for task type %v", t.Type)
			}
		}
		// Apply global middleware.
		for _, m := range p.middleware {
			h = m(h)
		}

		if err = h(ctx, t); err != nil {
			if errors.Is(err, context.Canceled) {
				return fmt.Errorf("task %v canceled: %v", t.ID, context.Cause(ctx))
			}

			if p.errorHandler != nil {
				p.errorHandler(ctx, t, err)
			}
			if t.Retries < t.MaxRetries && !errors.Is(err, ErrSkipRetry) {
				t.Retries = t.Retries + 1
				t.ScheduledAt = getNextRetryTime(int(t.Retries))

				if err := p.client.UpdateTask(ctx, tx, t); err != nil {
					return fmt.Errorf("update task %v: %w", t.ID, err)
				}

				return nil
			}
		}

		if err := p.client.DeleteTask(ctx, tx, t); err != nil {
			return fmt.Errorf("delete task %v: %w", t.ID, err)
		}

		return nil
	})
}

// getNextRetryTime returns the time of the next retry.
// Uses an exponential base delay with jitter.
// Approximate examples: 7s, 50s, 5min, 20min, 50min, 2h, 4h, 9h, 16h, 27h.
func getNextRetryTime(n int) time.Time {
	exp := 5 + int(math.Pow(float64(n), float64(5)))
	s := exp + rand.IntN(exp/2)
	d := time.Duration(s) * time.Second

	return time.Now().UTC().Add(d)
}
