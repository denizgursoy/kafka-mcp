package searchmessages

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
	"unicode/utf8"

	"github.com/dop251/goja"
	"github.com/twmb/franz-go/pkg/kgo"
)

// maxCallStackSize turns runaway recursion into a catchable JavaScript error
// rather than letting it exhaust the host stack and take the server with it.
const maxCallStackSize = 2000

// script is a compiled user filter, evaluated once per message.
//
// The program is compiled once and the runtime is reused across a whole scan,
// because creating either per message would cost more than the filtering.
// A runtime is not safe for concurrent use, so each scanning goroutine owns
// one of its own.
type script struct {
	runtime *goja.Runtime
	fn      goja.Callable
	source  string
}

// compileScript prepares a user script for evaluation.
//
// The script body is wrapped in a function so that a bare "return" works at
// the top level, which is the form a caller naturally writes.
//
// Compilation happens here rather than per message, so a malformed script is
// reported before any message is read.
func compileScript(source string) (*script, error) {
	program, err := goja.Compile(
		"filter.js",
		"(function (value, key, headers, partition, offset, timestamp) {\n"+
			source+
			"\n})",
		true,
	)
	if err != nil {
		return nil, fmt.Errorf("script does not compile: %w", err)
	}

	runtime := goja.New()

	// Nothing from the host is injected: the runtime has no require, no
	// filesystem, no network and no clock of its own. A filter has no
	// business reaching any of them.
	runtime.SetMaxCallStackSize(maxCallStackSize)

	// A scan must give the same answer twice. Both sources of nondeterminism
	// in JavaScript are pinned, so Date.now() and Math.random() cannot make
	// two identical searches disagree.
	fixed := time.Unix(0, 0).UTC()
	runtime.SetTimeSource(func() time.Time { return fixed })

	seeded := rand.New(rand.NewSource(1))
	runtime.SetRandSource(func() float64 { return seeded.Float64() })

	value, err := runtime.RunProgram(program)
	if err != nil {
		return nil, fmt.Errorf("script does not load: %w", err)
	}

	fn, ok := goja.AssertFunction(value)
	if !ok {
		return nil, fmt.Errorf("script did not produce a function")
	}

	return &script{runtime: runtime, fn: fn, source: source}, nil
}

// match reports whether a record satisfies the script.
//
// An error means the script failed on this message, which is different from
// the message not matching, and the caller counts the two separately.
func (s *script) match(record *kgo.Record) (bool, error) {
	// A time.Time passed through ToValue arrives as a wrapped Go value with no
	// Date methods, so the timestamp is constructed as a real JavaScript Date.
	timestamp, err := s.runtime.New(
		s.runtime.Get("Date").ToObject(s.runtime),
		s.runtime.ToValue(record.Timestamp.UnixMilli()),
	)
	if err != nil {
		return false, fmt.Errorf("build timestamp for partition %d offset %d: %w",
			record.Partition, record.Offset, err)
	}

	result, err := s.fn(
		goja.Undefined(),
		s.runtime.ToValue(decodeValue(record.Value)),
		s.runtime.ToValue(decodeKey(record.Key)),
		s.runtime.ToValue(decodeHeaders(record.Headers)),
		s.runtime.ToValue(record.Partition),
		s.runtime.ToValue(record.Offset),
		timestamp,
	)
	if err != nil {
		return false, fmt.Errorf("script failed on partition %d offset %d: %w",
			record.Partition, record.Offset, err)
	}

	// Ordinary JavaScript truthiness, so a script may return a field directly
	// rather than spelling out a comparison.
	return result.ToBoolean(), nil
}

// interrupt stops a script that is currently running. It is safe to call from
// another goroutine, and is what keeps an endless loop from hanging a scan.
func (s *script) interrupt() {
	s.runtime.Interrupt("search stopped")
}

// close releases the interrupt flag. A runtime that was interrupted keeps the
// flag set, so reusing one without clearing it would kill the next script
// immediately.
func (s *script) close() {
	s.runtime.ClearInterrupt()
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
