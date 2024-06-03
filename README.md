# NanoQ

NanoQ is a MySQL-powered task queue, implemented in ~400 lines of code.

While it can be used as-is, you are encouraged to copy-paste it into your project and customize it according to your tastes and needs.

## Requirements

Due to its use of SKIP LOCKED, NanoQ requires MySQL 8.0 or newer / MariaDB 10.6.0 or newer.

## Features

1. Delayed tasks
2. Unique tasks (automatic fingerprinting based on task type and payload)
3. Per-task timeouts
4. Retries (with exponential backoff and jitter)
5. Processor with global and per-handler middleware

Failed and completed tasks are not retained in the database.

## Usage

Add table.sql to your migrations.

Define a task payload and a handler to process it:
```go
// recalculateStockPayload is the payload for the recalculate stock task.
type recalculateStockPayload struct {
	ProductID string `json:"product_id"`
}

// This could also be a method on a Handler struct containing dependencies.
func RecalculateStock(logger *slog.Logger) nanoq.Handler {
	return func(ctx context.Context, t nanoq.Task) error {
		var payload recalculateStockPayload
		if err := json.Unmarshal(t.Payload, &payload); err != nil {
			return fmt.Errorf("json unmarshal: %v: %w", err, nanoq.ErrSkipRetry)
		}

		// Do your thing here.

		logger.Info("Task completed",
			slog.String("task_type", "recalculate-stock"),
			slog.String("product_id", payload.ProductID),
		)

		return nil
	}
}
```

Create a task (usually in an HTTP handler):
```go
// Usually provided to the HTTP handler.
queueClient := nanoq.Client(db)

payload, _ := json.Marshal(recalculateStockPayload{
	ProductID: "my-product",
})
t := nanoq.NewTask("recalculate-stock", payload, nanoq.WithTimeout(15*time.Second), nanoq.WithScheduledIn(5 * time.Minute))

// The transaction (tx) usually already exists. Otherwise, queueClient.RunTransaction() can be used to start one.
if err := queueClient.CreateTask(ctx, tx, t); err != nanoq.ErrDuplicateTask {
	// Handle error.
}
```

Finally, initialize the processor:
```go
// logger is an existing *slog.Logger.
processor := nanoq.NewProcessor(nanoq.NewClient(db), logger)

// The default retry policy uses an exponential backoff with jitter,
// but callers can provide their own if necessary.
processor.RetryPolicy(func (t nanoq.Task) {
	// First retry in 5s, every next retry in 1h.
	if t.Retries == 0 {
		return 5 * time.Second
	}
	return 1 * time.Hour
})
processor.OnError(func(ctx context.Context, t nanoq.Task, err error) {
	// Log each failed task. 
	// Idea: Send to Sentry when t.Retries == t.MaxRetries.
	logger.Error(err.Error(),
		slog.String("task_type", t.Type),
		slog.String("attempt", fmt.Sprintf("%v/%v", t.Retries, t.MaxRetries)),
	)
})
processor.Handle("recalculate-stock", RecalculateStock(logger))

// Use as many workers as we have CPUs.
processor.Run(context.Background(), runtime.NumCPU(), 5 * time.Second)
```





