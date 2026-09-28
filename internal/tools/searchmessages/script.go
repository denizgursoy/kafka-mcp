package searchmessages

import (
	"context"
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/script"
)

// scriptParameters are the variables a search predicate sees, in the order
// match supplies them.
var scriptParameters = []string{
	"value", "key", "headers", "partition", "offset", "timestamp",
}

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
	compiled, err := script.Compile(source, scriptParameters...)
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

// match reports whether a record satisfies the script.
//
// An error means the script failed on this message, which is different from
// the message not matching, and the caller counts the two separately.
func (f *filter) match(record *kgo.Record) (bool, error) {
	runtime := f.Runtime()

	// A time.Time passed through ToValue arrives as a wrapped Go value with no
	// Date methods, so the timestamp is constructed as a real JavaScript Date.
	timestamp, err := runtime.New(
		runtime.Get("Date").ToObject(runtime),
		runtime.ToValue(record.Timestamp.UnixMilli()),
	)
	if err != nil {
		return false, fmt.Errorf("build timestamp for partition %d offset %d: %w",
			record.Partition, record.Offset, err)
	}

	matched, err := f.Call(
		f.Value(decodeValue(record.Value)),
		f.Value(decodeKey(record.Key)),
		f.Value(decodeHeaders(record.Headers)),
		f.Value(record.Partition),
		f.Value(record.Offset),
		timestamp,
	)
	if err != nil {
		return false, fmt.Errorf("script failed on partition %d offset %d: %w",
			record.Partition, record.Offset, err)
	}

	return matched, nil
}

// decodeValue turns a record value into what the script sees: the parsed
// document when it is JSON, and the raw text otherwise, so topics that do not
// hold JSON remain searchable.
func decodeValue(value []byte) any {
	if len(value) == 0 {
		return nil
	}

	var parsed any

	if err := json.Unmarshal(value, &parsed); err == nil {
		return parsed
	}

	if utf8.Valid(value) {
		return string(value)
	}

	// Binary payloads cannot be rendered as text without corrupting them, so
	// the script sees the bytes and can inspect their length or contents.
	return value
}

// decodeKey renders a key as a string, or null when there is none, so a
// script can tell a missing key from an empty one.
func decodeKey(key []byte) any {
	if key == nil {
		return nil
	}

	if utf8.Valid(key) {
		return string(key)
	}

	return key
}

func decodeHeaders(headers []kgo.RecordHeader) map[string]any {
	decoded := make(map[string]any, len(headers))

	for _, header := range headers {
		if utf8.Valid(header.Value) {
			decoded[header.Key] = string(header.Value)

			continue
		}

		decoded[header.Key] = header.Value
	}

	return decoded
}
