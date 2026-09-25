// Package batch provides the common result envelope and bounded execution used
// by tools that accept several independent operations in one MCP call.
package batch

import (
	"context"
	"fmt"
	"sync"
)

const (
	// MaxItems bounds metadata and administrative batches.
	MaxItems = 100
	// MaxHeavyItems bounds batches that open record readers or return messages.
	MaxHeavyItems      = 20
	defaultParallelism = 4
)

// Result is one item in a batch. Exactly one of Result and Error is set.
type Result[T any] struct {
	Index  int    `json:"index"`
	Result *T     `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

// Output reports every item in input order. Batch operations are deliberately
// non-atomic: Kafka offers no transaction spanning these administrative and
// record operations.
//
// This is the only response shape a batch tool has. A tool used to offer a
// second, single-target shape alongside it, which meant every field was
// declared twice and the two drifted; one operation is now an items array of
// length one.
type Output[T any] struct {
	Results   []Result[T] `json:"results"`
	Succeeded int         `json:"succeeded"`
	Failed    int         `json:"failed"`
	Applied   int         `json:"applied"`
	Atomic    bool        `json:"atomic"`
}

// Validate checks the structural limits shared by every batch tool.
func Validate(length int, maximum int) error {
	if length == 0 {
		return fmt.Errorf("items must contain at least one operation")
	}
	if length > maximum {
		return fmt.Errorf("items may contain at most %d operations, got %d", maximum, length)
	}

	return nil
}

// Run applies fn with bounded concurrency and preserves input order. An item
// failure is data in the response, not an error for the whole MCP call.
func Run[I, O any](
	ctx context.Context,
	items []I,
	maximum int,
	fn func(context.Context, I) (O, error),
) (Output[O], error) {
	if err := Validate(len(items), maximum); err != nil {
		return Output[O]{}, err
	}

	out := Output[O]{Results: make([]Result[O], len(items))}
	workers := defaultParallelism
	if len(items) < workers {
		workers = len(items)
	}

	jobs := make(chan int)
	var wait sync.WaitGroup

	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := range jobs {
				value, err := fn(ctx, items[index])
				out.Results[index].Index = index
				if err != nil {
					out.Results[index].Error = err.Error()
					continue
				}
				out.Results[index].Result = &value
			}
		}()
	}

	for index := range items {
		select {
		case jobs <- index:
		case <-ctx.Done():
			close(jobs)
			wait.Wait()
			return Output[O]{}, ctx.Err()
		}
	}
	close(jobs)
	wait.Wait()

	for _, result := range out.Results {
		if result.Error != "" {
			out.Failed++
		} else {
			out.Succeeded++
		}
	}

	return out, nil
}
