# NanoQ

NanoQ is a MySQL-powered task queue, implemented in ~300 lines of code.

While it can be used as-is, you are encouraged to copy-paste it into your project and customize it according to your tastes and needs.

## Requirements

Due to its use of SKIP LOCKED, NanoQ requires MySQL 8.0 or newer / MariaDB 10.6.0 or newer.

## Features

1. Delayed tasks
2. Unique tasks (automatic fingerprinting based on task type and payload)
3. Retries (with exponential backoff and jitter)
4. Processor with global and per-handler middleware

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
func RecalculateStock(logger zerolog.Logger) nanoq.Handler {
	return func(ctx context.Context, t nanoq.Task) error {
		var payload recalculateStockPayload
		if err := json.Unmarshal(t.Payload, &payload); err != nil {
			return fmt.Errorf("json unmarshal: %v: %w", err, nanoq.ErrSkipRetry)
		}

		// Do your thing here.

		logger.Info().
			Str("task_type", "recalculate-stock").
			Str("product_id", payload.ProductID).
			Msg("Task completed")

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
t := nanoq.NewTask("recalculate-stock", payload, nanoq.WithScheduledIn(5 * time.Minute))

// The transaction (tx) usually already exists. Otherwise, queueClient.RunTransaction() can be used to start one.
if err := queueClient.CreateTask(ctx, tx, t); err != nanoq.ErrDuplicateTask {
	// Handle error.
}
```

Finally, initialize the processor:
```go
// logger is assumed to be a zerolog instance.
processor := nanoq.NewProcessor(nanoq.NewClient(db), logger)

processor.OnError(func(ctx context.Context, t nanoq.Task, err error) {
	// Log each failed task. 
	// Idea: Send to Sentry when t.Retries == t.MaxRetries.
	logger.Error().
		Str("task_type", t.Type).
		Str("attempt", fmt.Sprintf("%v/%v", t.Retries, t.MaxRetries)).
		Msg(err.Error())
})
processor.Handle("recalculate-stock", RecalculateStock(logger))

// Use as many workers as we have CPUs.
processor.Run(context.Background(), runtime.NumCPU(), 5 * time.Second)
```





