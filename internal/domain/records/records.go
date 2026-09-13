// Package records reads Kafka records at exact offsets and renders them for
// an MCP client.
//
// It lives in internal/domain because four tools use it: describe_topic,
// get_message, search_messages and sample_messages. It is not a tool itself
// and registers nothing.
package records

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Message is a Kafka record rendered for an MCP client.
//
// Values are strings so they can be read directly by the caller. A payload
// that is not valid UTF-8 cannot survive JSON, so it is base64 encoded and
// Encoding says so.
type Message struct {
	Partition  int32             `json:"partition"`
	Offset     int64             `json:"offset"`
	Timestamp  time.Time         `json:"timestamp"`
	Key        string            `json:"key"`
	Value      string            `json:"value"`
	Headers    map[string]string `json:"headers,omitempty"`
	Encoding   string            `json:"encoding"`
	ValueBytes int               `json:"value_bytes"`
	Truncated  bool              `json:"truncated"`
}

// DefaultMaxValueBytes bounds a rendered value when the caller sets no limit,
// so one large message cannot flood an MCP client's context window.
const DefaultMaxValueBytes = 4096

// Render converts a Kafka record into a Message, truncating the value to
// maxValueBytes. A maxValueBytes of zero or less means DefaultMaxValueBytes.
func Render(record *kgo.Record, maxValueBytes int) Message {
	if maxValueBytes <= 0 {
		maxValueBytes = DefaultMaxValueBytes
	}

	message := Message{
		Partition:  record.Partition,
		Offset:     record.Offset,
		Timestamp:  record.Timestamp.UTC(),
		Key:        renderText(record.Key),
		ValueBytes: len(record.Value),
		Encoding:   "utf8",
	}

	value := record.Value

	if !utf8.Valid(value) {
		message.Encoding = "base64"
		message.Value = base64.StdEncoding.EncodeToString(value)
	} else {
		message.Value = string(value)
	}

	if len(message.Value) > maxValueBytes {
		message.Value = message.Value[:maxValueBytes]
		message.Truncated = true
	}

	if len(record.Headers) > 0 {
		message.Headers = make(map[string]string, len(record.Headers))

		for _, header := range record.Headers {
			message.Headers[header.Key] = renderText(header.Value)
		}
	}

	return message
}

func renderText(value []byte) string {
	if !utf8.Valid(value) {
		return base64.StdEncoding.EncodeToString(value)
	}

	return string(value)
}

// Range is a half-open offset range [Start, End) within one partition.
type Range struct {
	Partition int32
	Start     int64
	End       int64
}

// Empty reports whether the range covers no offsets.
func (r Range) Empty() bool {
	return r.End <= r.Start
}

// Reader reads records from a cluster.
//
// A Reader opens its own clients rather than sharing the server's. Consuming
// is client-wide state in franz-go: assigning partitions on the shared client
// would disturb any other tool call running at the same time.
type Reader struct {
	seeds []string
}

// NewReader returns a Reader for the given seed brokers.
func NewReader(seeds ...string) *Reader {
	return &Reader{seeds: seeds}
}

const (
	// pollTimeout bounds a single poll. PollFetches blocks until records
	// arrive, so a request for an offset that holds no data would hang
	// forever without a deadline of its own.
	pollTimeout = 2 * time.Second

	// fetchMaxWait bounds how long a broker holds a fetch open waiting for
	// data. It is short because a scan that has reached the end of a partition
	// must not stall on an idle topic.
	fetchMaxWait = time.Second

	// maxEmptyPolls stops a scan that keeps polling nothing. Records can be
	// absent from an offset range because the range is past the end of the
	// partition, or because retention or compaction removed them, and without
	// this the scan would wait for data that will never come.
	maxEmptyPolls = 2
)

// Session is one connection used for several scans of the same topic.
//
// A search reads a large offset range in chunks. Opening a client per chunk
// would cost one connection per chunk, so callers that scan repeatedly open a
// Session once and reuse it. Always pair it with Close.
type Session struct {
	client *kgo.Client
	topic  string
}

// Session opens a connection for scanning a topic.
//
// The partitions to read are chosen per scan, so the session starts consuming
// nothing. It must be closed by the caller.
func (r *Reader) Session(topic string) (*Session, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(r.seeds...),
		// Consuming must be configured at construction for partitions to be
		// addable later: franz-go only sets up a direct consumer when the
		// client is built with one of the consume options.
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {}}),
		kgo.FetchMaxWait(fetchMaxWait),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to %v: %w", r.seeds, err)
	}

	return &Session{client: client, topic: topic}, nil
}

// Close releases the session's connection.
func (s *Session) Close() {
	s.client.Close()
}

// Scan reads the records in the given ranges and calls visit for each one, in
// offset order within a partition but in no guaranteed order across
// partitions.
//
// Scanning stops when visit returns false, when every range is exhausted, or
// when ctx is done. Reaching the end of the data is not an error, and neither
// is a range whose records have been deleted or never existed.
//
// The session is left consuming nothing afterwards, so the next scan starts
// clean and cannot see records buffered for a range that is already done.
func (s *Session) Scan(
	ctx context.Context,
	ranges []Range,
	visit func(*kgo.Record) bool,
) error {

	active := make(map[int32]int64, len(ranges))
	offsets := make(map[int32]kgo.Offset, len(ranges))
	partitions := make([]int32, 0, len(ranges))

	for _, rng := range ranges {
		if rng.Empty() {
			continue
		}

		active[rng.Partition] = rng.End
		offsets[rng.Partition] = kgo.NewOffset().At(rng.Start)
		partitions = append(partitions, rng.Partition)
	}

	if len(active) == 0 {
		return nil
	}

	s.client.AddConsumePartitions(map[string]map[int32]kgo.Offset{s.topic: offsets})

	// Removing the partitions again drops whatever the client buffered for
	// this chunk, so a later chunk cannot be handed stale records.
	defer s.client.RemoveConsumePartitions(map[string][]int32{s.topic: partitions})

	// The empty-poll count is per scan, not per session: a chunk that ends
	// early must not make the next chunk give up sooner.
	empty := 0

	for len(active) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}

		fetches, err := poll(ctx, s.client)
		if err != nil {
			return fmt.Errorf("fetch from %s: %w", s.topic, err)
		}

		if fetches.Empty() {
			empty++

			if empty >= maxEmptyPolls {
				return nil
			}

			continue
		}

		empty = 0

		stopped := false

		fetches.EachRecord(func(record *kgo.Record) {
			if stopped {
				return
			}

			end, ok := active[record.Partition]
			if !ok {
				return
			}

			// Records buffered before this chunk began can still arrive, so
			// anything outside the requested range is ignored rather than
			// trusted.
			if record.Offset >= end {
				delete(active, record.Partition)

				return
			}

			// This was the last offset wanted from this partition. Drop it now
			// rather than polling again just to discover the range is done: on
			// an idle topic that extra poll costs a full poll timeout.
			if record.Offset == end-1 {
				delete(active, record.Partition)
			}

			if !visit(record) {
				stopped = true
			}
		})

		if stopped {
			return nil
		}
	}

	return nil
}

// Scan reads one set of ranges over a connection of its own.
//
// Callers that scan the same topic repeatedly should open a Session instead,
// so the whole read costs a single connection.
func (r *Reader) Scan(
	ctx context.Context,
	topic string,
	ranges []Range,
	visit func(*kgo.Record) bool,
) error {

	session, err := r.Session(topic)
	if err != nil {
		return err
	}

	defer session.Close()

	return session.Scan(ctx, ranges, visit)
}

// poll fetches one batch, returning an empty batch rather than an error when
// the batch simply had nothing in it before the poll deadline.
func poll(ctx context.Context, client *kgo.Client) (kgo.Fetches, error) {
	pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)
	defer cancel()

	fetches := client.PollFetches(pollCtx)

	if err := fetches.Err0(); err != nil {
		// The caller's own cancellation is a real error; the poll deadline
		// only means this batch was empty.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if pollCtx.Err() != nil {
			return nil, nil
		}

		return nil, err
	}

	return fetches, nil
}

// Last returns the final record of each given partition, keyed by partition.
// Partitions whose last record cannot be read are absent from the result
// rather than reported as an error, because an empty or fully deleted
// partition is a normal state, not a failure.
func (r *Reader) Last(
	ctx context.Context,
	topic string,
	endOffsets map[int32]int64,
) (map[int32]*kgo.Record, error) {

	ranges := make([]Range, 0, len(endOffsets))

	for partition, end := range endOffsets {
		if end <= 0 {
			continue
		}

		ranges = append(ranges, Range{
			Partition: partition,
			Start:     end - 1,
			End:       end,
		})
	}

	found := make(map[int32]*kgo.Record, len(ranges))

	err := r.Scan(ctx, topic, ranges, func(record *kgo.Record) bool {
		found[record.Partition] = record

		return len(found) < len(ranges)
	})
	if err != nil {
		return nil, err
	}

	return found, nil
}
