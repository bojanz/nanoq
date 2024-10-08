package nanoq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/oklog/ulid/v2"
)

var (
	// ErrDuplicateTask indicates that an identical task already exists in the queue.
	// Tasks are considered identical if they have the same fingerprint.
	ErrDuplicateTask = errors.New("duplicate task")

	// ErrNoTasks indicates that no tasks are available for processing.
	ErrNoTasks = errors.New("no tasks available")

	// ErrSkipRetry indicates that the task should not be retried.
	ErrSkipRetry = errors.New("skip retry")

	// ErrTaskTimeout indicates that the task timeout has been exceeded.
	ErrTaskTimeout = errors.New("task timeout exceeded")
)

// Task represents a task.
type Task struct {
	ID             string     `db:"id"`
	Fingerprint    string     `db:"fingerprint"`
	Type           string     `db:"type"`
	Payload        []byte     `db:"payload"`
	Retries        uint8      `db:"retries"`
	MaxRetries     uint8      `db:"max_retries"`
	TimeoutSeconds int32      `db:"timeout_seconds"`
	CreatedAt      time.Time  `db:"created_at"`
	ScheduledAt    time.Time  `db:"scheduled_at"`
	ClaimedAt      *time.Time `db:"claimed_at"`
}

// NewTask creates a new task.
func NewTask(taskType string, payload []byte, opts ...TaskOption) Task {
	if len(payload) == 0 {
		// Empty payloads must be valid JSON.
		payload = []byte("{}")
	}
	now := time.Now().UTC()
	t := Task{
		ID:             ulid.Make().String(),
		Type:           taskType,
		Payload:        payload,
		MaxRetries:     10,
		TimeoutSeconds: 60,
		CreatedAt:      now,
		ScheduledAt:    now,
	}
	for _, opt := range opts {
		opt(&t)
	}
	if t.Fingerprint == "" {
		t.Fingerprint = getFingerprint(t.Type, t.Payload)
	}

	return t
}

// Timeout returns the task timeout as a time.Duration.
func (t Task) Timeout() time.Duration {
	return time.Duration(t.TimeoutSeconds) * time.Second
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

// WithTimeout sets the task timeout.
//
// Must be at least 1s. Defaults to 60s.
func WithTimeout(timeout time.Duration) TaskOption {
	return func(t *Task) {
		t.TimeoutSeconds = int32(timeout.Seconds())
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

// CreateTask creates the given task.
//
// Expected to run in an existing transaction.
// Returns ErrDuplicateTask if a task with the same fingerprint already exists.
func (c *Client) CreateTask(ctx context.Context, tx *sqlx.Tx, t Task) error {
	_, err := tx.NamedExecContext(ctx, `
		INSERT INTO tasks
			(id, fingerprint, type, payload, max_retries, timeout_seconds, created_at, scheduled_at)
		VALUES
			(:id, :fingerprint, :type, :payload, :max_retries, :timeout_seconds, :created_at, :scheduled_at)`, t)
	if err != nil {
		if isConflictError(err) {
			return ErrDuplicateTask
		}
		return err
	}

	return nil
}

// ClaimTask claims a task for processing.
//
// Returns ErrNoTasks if no tasks are available.
func (c *Client) ClaimTask(ctx context.Context) (Task, error) {
	t := Task{}
	err := c.RunTransaction(ctx, func(tx *sqlx.Tx) error {
		// Tasks are expected to be canceled and released after the task timeout is reached.
		// If a task is still claimed after that point, it likely means that the processor has crashed, requiring the task to be reclaimed.
		// Task are reclaimed after timeout_seconds * 1.1, to allow for extra processing to occur post-cancelation.
		err := tx.GetContext(ctx, &t, `
			SELECT * FROM tasks
			WHERE scheduled_at <= UTC_TIMESTAMP()
				AND (claimed_at IS NULL OR DATE_ADD(claimed_at, INTERVAL timeout_seconds*1.1 SECOND) < UTC_TIMESTAMP())
			ORDER BY scheduled_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED`)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNoTasks
			}
			return fmt.Errorf("get task: %w", err)
		}
		now := time.Now().UTC()
		t.ClaimedAt = &now

		_, err = tx.NamedExecContext(ctx, `UPDATE tasks SET claimed_at = :claimed_at WHERE id = :id`, t)
		if err != nil {
			return fmt.Errorf("update task: %w", err)
		}

		return nil
	})

	return t, err
}

// DeleteTask deletes the given task.
func (c *Client) DeleteTask(ctx context.Context, t Task) error {
	return c.RunTransaction(ctx, func(tx *sqlx.Tx) error {
		_, err := tx.NamedExecContext(ctx, `DELETE FROM tasks WHERE id = :id`, t)
		return err
	})
}

// ReleaseTask releases the given task, allowing it to be claimed again.
func (c *Client) ReleaseTask(ctx context.Context, t Task) error {
	return c.RunTransaction(ctx, func(tx *sqlx.Tx) error {
		_, err := tx.NamedExecContext(ctx, `UPDATE tasks SET claimed_at = NULL WHERE id = :id`, t)
		return err
	})
}

// RetryTask schedules a retry of the given task.
func (c *Client) RetryTask(ctx context.Context, t Task, retryIn time.Duration) error {
	return c.RunTransaction(ctx, func(tx *sqlx.Tx) error {
		t.Retries++
		t.ScheduledAt = time.Now().UTC().Add(retryIn)
		t.ClaimedAt = nil

		_, err := tx.NamedExecContext(ctx, `UPDATE tasks SET retries = :retries, scheduled_at = :scheduled_at, claimed_at = :claimed_at WHERE id = :id`, t)
		return err
	})
}

// RunTransaction runs the given function in a transaction.
//
// The transaction will be rolled back if the function returns an error.
// Otherwise, it will be committed.
func (c *Client) RunTransaction(ctx context.Context, fn func(tx *sqlx.Tx) error) error {
	tx, err := c.db.BeginTxx(ctx, nil)
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

	// RetryPolicy determines the retry delay for a given task.
	RetryPolicy func(t Task) time.Duration
)

// DefaultRetryPolicy uses an exponential base delay with jitter.
// Approximate examples: 7s, 50s, 5min, 20min, 50min, 2h, 4h, 9h, 16h, 27h.
func DefaultRetryPolicy(t Task) time.Duration {
	exp := 5 + int(math.Pow(float64(t.Retries+1), float64(5)))
	s := exp + rand.IntN(exp/2)

	return time.Duration(s) * time.Second
}

// Processor represents the queue processor.
type Processor struct {
	client *Client
	logger *slog.Logger

	errorHandler ErrorHandler
	handlers     map[string]Handler
	middleware   []Middleware
	retryPolicy  RetryPolicy

	workers chan struct{}
	done    atomic.Bool
}

// NewProcessor creates a new processor.
func NewProcessor(client *Client, logger *slog.Logger) *Processor {
	return &Processor{
		client:      client,
		logger:      logger,
		handlers:    make(map[string]Handler),
		middleware:  make([]Middleware, 0),
		retryPolicy: DefaultRetryPolicy,
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
	for i := len(ms) - 1; i >= 0; i-- {
		h = ms[i](h)
	}
	p.handlers[taskType] = h
}

// RetryPolicy registers the given retry policy.
func (p *Processor) RetryPolicy(rp RetryPolicy) {
	p.retryPolicy = rp
}

// Run starts the processor and blocks until a shutdown signal (SIGINT/SIGTERM) is received.
//
// Once the shutdown signal is received, workers stop claiming new tasks.
// The tasks that are still being processed are then given until shutdownTimeout to complete,
// after which they are stopped (via canceled context).
func (p *Processor) Run(ctx context.Context, concurrency int, shutdownTimeout time.Duration) {
	processorCtx, cancel := context.WithCancelCause(context.Background())
	go func() {
		shutdownCh := make(chan os.Signal, 1)
		signal.Notify(shutdownCh, os.Interrupt, syscall.SIGTERM)

		select {
		case <-shutdownCh:
		case <-ctx.Done():
		}

		p.logger.Info("Shutting down processor", slog.String("timeout", shutdownTimeout.String()))
		p.done.Store(true)
		time.AfterFunc(shutdownTimeout, func() {
			cancel(errors.New("shutdown timeout reached"))
		})
	}()

	p.logger.Info("Starting processor", slog.Int("concurrency", concurrency))
	p.workers = make(chan struct{}, concurrency)
	for !p.done.Load() {
		// Acquire a worker before claiming a task, to avoid holding claimed tasks while all workers are busy.
		p.workers <- struct{}{}

		t, err := p.client.ClaimTask(processorCtx)
		if err != nil {
			if !errors.Is(err, ErrNoTasks) && !errors.Is(err, context.Canceled) {
				p.logger.Error("Could not claim task", slog.Any("error", err))
			}
			<-p.workers
			time.Sleep(1 * time.Second)
			continue
		}

		go func() {
			if err = p.processTask(processorCtx, t); err != nil {
				p.logger.Error("Could not process task", slog.Any("error", err))
			}
			<-p.workers
		}()
	}

	// Wait for workers to finish.
	for range cap(p.workers) {
		p.workers <- struct{}{}
	}
}

// processTask processes a single task.
func (p *Processor) processTask(ctx context.Context, t Task) error {
	h, ok := p.handlers[t.Type]
	if !ok {
		h = func(ctx context.Context, t Task) error {
			return fmt.Errorf("no handler found for task type %v: %w", t.Type, ErrSkipRetry)
		}
	}
	// Apply global middleware.
	for i := len(p.middleware) - 1; i >= 0; i-- {
		h = p.middleware[i](h)
	}

	if err := callHandler(ctx, h, t); err != nil {
		if errors.Is(err, context.Canceled) {
			// The processor is shutting down. Release the task and exit.
			if err = p.client.ReleaseTask(context.Background(), t); err != nil {
				return fmt.Errorf("release task %v: %w", t.ID, err)
			}
			return fmt.Errorf("task %v canceled: %v", t.ID, context.Cause(ctx))
		}

		if p.errorHandler != nil {
			p.errorHandler(ctx, t, err)
		}
		if t.Retries < t.MaxRetries && !errors.Is(err, ErrSkipRetry) {
			retryIn := p.retryPolicy(t)
			if err := p.client.RetryTask(ctx, t, retryIn); err != nil {
				return fmt.Errorf("retry task %v: %w", t.ID, err)
			}

			return nil
		}
	}

	if err := p.client.DeleteTask(ctx, t); err != nil {
		return fmt.Errorf("delete task %v: %w", t.ID, err)
	}

	return nil
}

// callHandler calls the given handler, converting panics into errors.
func callHandler(ctx context.Context, h Handler, t Task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			var file string
			var line int
			// Skip the first two frames (callHandler, panic).
			// If the panic came from the runtime, find the first application frame.
			for i := 2; i < 10; i++ {
				_, file, line, _ = runtime.Caller(i)
				if !strings.HasPrefix(file, "runtime/") {
					break
				}
			}

			err = fmt.Errorf("panic [%s:%d]: %v: %w", file, line, r, ErrSkipRetry)
		}
	}()
	taskCtx, cancel := context.WithTimeoutCause(ctx, t.Timeout(), ErrTaskTimeout)
	defer cancel()

	err = h(taskCtx, t)
	if err != nil && errors.Is(err, context.DeadlineExceeded) {
		// Extract a more specific timeout error, if any.
		// context.Cause returns nil if the canceled context is a child of taskCtx.
		if cerr := context.Cause(taskCtx); cerr != nil {
			err = cerr
		}
	}

	return err
}
