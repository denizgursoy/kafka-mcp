package searchmessages

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dop251/goja"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/script"
	"github.com/denizgursoy/kafka-mcp/internal/domain/serde"
)

// filter is a compiled user predicate bound to Kafka records.
//
// The runtime and its hardening live in internal/domain/script, which
// list_topics shares. What stays here is the part that is specific to a
// message: how a record's fields are rendered for JavaScript.
type filter struct {
	*script.Script
}

// compileScript prepares a user script for evaluation against records.
func compileScript(source string) (*filter, error) {
	compiled, err := script.Compile(source, script.RecordParameters...)
	if err != nil {
		return nil, err
	}

	return &filter{Script: compiled}, nil
}

// guard interrupts the script when ctx is done, so a predicate that never
// returns cannot hold a scanning goroutine forever.
func (f *filter) guard(ctx context.Context) func() {
	if f == nil {
		return func() {}
	}

	return script.Guard(ctx, f.Script, "search stopped")
}

// match reports whether a record satisfies the script, given its value and key
// already decoded, so a message is decoded once for both matching and showing.
//
// An error means the script failed on this message, which is different from
// the message not matching, and the caller counts the two separately.
func (f *filter) match(record *kgo.Record, value, key serde.Decoded) (bool, error) {
	arguments, err := f.arguments(record, value, key)
	if err != nil {
		return false, err
	}

	matched, err := f.Call(arguments...)
	if err != nil {
		return false, fmt.Errorf("script failed on partition %d offset %d: %w",
			record.Partition, record.Offset, err)
	}

	return matched, nil
}

// group evaluates a group_by expression and returns the bucket name. null and
// undefined become "null", so a missing field is its own visible bucket.
func (f *filter) group(record *kgo.Record, value, key serde.Decoded) (string, error) {
	arguments, err := f.arguments(record, value, key)
	if err != nil {
		return "", err
	}

	result, err := f.Evaluate(arguments...)
	if err != nil {
		return "", fmt.Errorf("group_by failed on partition %d offset %d: %w",
			record.Partition, record.Offset, err)
	}

	if goja.IsNull(result) || goja.IsUndefined(result) {
		return "null", nil
	}

	if _, isObject := result.(*goja.Object); isObject {
		encoded, err := json.Marshal(result.Export())
		if err == nil {
			return string(encoded), nil
		}
	}

	return result.String(), nil
}

// arguments renders a record as the values a script sees, in the order of
// script.RecordParameters.
func (f *filter) arguments(record *kgo.Record, value, key serde.Decoded) ([]goja.Value, error) {
	return script.RecordArguments(f.Script, record, value, key)
}
