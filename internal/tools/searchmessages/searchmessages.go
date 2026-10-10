// Package searchmessages implements the search_messages MCP tool.
package searchmessages

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
	"github.com/denizgursoy/kafka-mcp/internal/domain/serde"
)

// Input is the argument set accepted by the search_messages tool.
type Input struct {
	Topic         string     `json:"topic" jsonschema:"Topic to search. Matched exactly and case-sensitively."`
	Script        string     `json:"script,omitempty" jsonschema:"Optional JavaScript that decides whether a message matches. Return true to keep it. In scope: value (the parsed document for JSON, and the decoded record for Avro, Protobuf, JSON Schema or a configured format; the raw text otherwise), key (string, decoded document when the key has a schema, or null), headers (object of header name to string), partition, offset, timestamp (a Date), value_bytes and key_bytes (raw sizes, before decoding), format (json, avro, protobuf, json_schema, msgpack, text, binary or null), schema_id (number or null) and decode_error (string or null). Examples: return key === 'order-123'; return value.eventType === 'NEW' && value.payload.amount >= 500; return value.payload.cancelledAt === null. Omit to match every message."`
	Parallelism   int        `json:"parallelism,omitempty" jsonschema:"Optional number of concurrent readers, from 1 to 16. Defaults to 1. It splits a single-partition topic's offsets between readers, which makes a full scan of one large partition faster. A multi-partition topic is already read across its partitions together, so this does not apply there. Worth using for count_only, output_file or a full scan of one partition."`
	Partitions    []int32    `json:"partitions,omitempty" jsonschema:"Optional partitions to restrict the search to. Defaults to every partition. Do not guess a partition from a message key: producers may set the partition explicitly, so the key does not determine it."`
	FromOffset    *int64     `json:"from_offset,omitempty" jsonschema:"Optional inclusive offset to start scanning from, applied to every searched partition."`
	ToOffset      *int64     `json:"to_offset,omitempty" jsonschema:"Optional exclusive offset to stop scanning at, applied to every searched partition."`
	FromTimestamp *time.Time `json:"from_timestamp,omitempty" jsonschema:"Optional inclusive start time (RFC3339). Resolved to the first offset at or after this time."`
	ToTimestamp   *time.Time `json:"to_timestamp,omitempty" jsonschema:"Optional exclusive end time (RFC3339). Resolved to the first offset at or after this time."`
	Direction     string     `json:"direction,omitempty" jsonschema:"Optional scan direction: newest_first (default) or oldest_first. Decides which matches are kept when max_matches cuts the search short. Every partition is read together, so newest_first means newest in the topic, ordered by timestamp, rather than newest in one partition."`
	MaxMatches    int        `json:"max_matches,omitempty" jsonschema:"Optional maximum number of matches to return. Defaults to 10, at most 1000. Use output_file to export more."`
	MaxScanned    int        `json:"max_messages_scanned,omitempty" jsonschema:"Optional maximum number of messages to read before giving up. Defaults to 10000, at most 100000000."`
	MaxValueBytes int        `json:"max_value_bytes,omitempty" jsonschema:"Optional maximum value bytes to return per match. Defaults to 512, at most 1048576. Longer values are cut and flagged with truncated=true."`
	TimeoutSecond int        `json:"timeout_seconds,omitempty" jsonschema:"Optional wall-clock limit for the scan in seconds. Defaults to 30, at most 3600."`
	CountOnly     bool       `json:"count_only,omitempty" jsonschema:"Optional. When true, scan the whole range and return only how many messages matched, with a per-partition breakdown and no message bodies. Use this first when a query may match a great many messages, then ask the user how they want them before fetching any."`
	GroupBy       string     `json:"group_by,omitempty" jsonschema:"Optional JavaScript expression, with the same variables as script, that returns a bucket name for each matching message, e.g. return headers['error-reason'] or return key or return schema_id. Every match in the range is counted per bucket, like count_only, and no message bodies are returned. null and undefined form the bucket null. Cannot be combined with output_file."`
	MaxGroups     int        `json:"max_groups,omitempty" jsonschema:"Optional number of largest buckets to return with group_by. Defaults to 20, at most 1000. groups_truncated says whether more existed."`
	OutputFile    string     `json:"output_file,omitempty" jsonschema:"Optional file name to write every match to, as one JSON message per line. Use this instead of returning thousands of messages. A new name only, not a path: the server chooses the directory and refuses a name that already exists. The response reports the path, the number written and a short preview."`
}

// ScannedRange reports the offsets actually covered in one partition.
type ScannedRange struct {
	Partition int32 `json:"partition"`
	Start     int64 `json:"start"`
	End       int64 `json:"end"`
}

// PartitionCount reports how many matches came from one partition.
type PartitionCount struct {
	Partition int32 `json:"partition"`
	Matches   int   `json:"matches"`
}

// Group is one group_by bucket: how many matches it holds and where one is.
type Group struct {
	Key     string  `json:"key"`
	Count   int     `json:"count"`
	Example Address `json:"example"`
}

// Address locates one message, enough to open it with get_message.
type Address struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

// Output is the result returned by the search_messages tool.
type Output struct {
	Topic           string            `json:"topic"`
	Matches         []records.Message `json:"matches"`
	MatchCount      int               `json:"match_count"`
	MatchesByPart   []PartitionCount  `json:"matches_by_partition,omitempty"`
	ScannedMessages int               `json:"scanned_messages"`
	ScannedRanges   []ScannedRange    `json:"scanned_ranges"`
	StoppedReason   string            `json:"stopped_reason"`
	Complete        bool              `json:"complete"`
	ScriptErrors    int               `json:"script_errors,omitempty"`
	OutputFile      string            `json:"output_file,omitempty"`
	WrittenMessages int               `json:"written_messages,omitempty"`
	Groups          []Group           `json:"groups,omitempty"`
	GroupsTruncated bool              `json:"groups_truncated,omitempty"`
}

// Stop reasons reported back to the caller.
const (
	reasonExhausted  = "range_exhausted"
	reasonMaxMatches = "max_matches"
	reasonMaxScanned = "max_scanned"
	reasonTimeout    = "timeout"
)

// Defaults chosen to keep a scan cheap and its result small enough for an MCP
// client to read.
const (
	defaultMaxMatches    = 10
	defaultMaxScanned    = 10000
	defaultMaxValueBytes = 512
	defaultTimeout       = 30 * time.Second

	// chunkSize bounds one backward scan window. Kafka only reads forward, so
	// a newest-first search walks backwards in chunks of this many offsets.
	chunkSize = 500

	// Limits on caller input. max_matches and max_value_bytes size what is
	// held in memory; max_messages_scanned and timeout_seconds bound how long
	// one call may hold its readers.
	maxMaxMatches    = 1000
	maxMaxScanned    = 100_000_000
	maxValueBytesCap = 1 << 20
	maxTimeout       = 3600

	defaultMaxGroups = 20
	maxMaxGroups     = 1000
)

const description = `
Search message key, value, headers or metadata with a JavaScript predicate.
The script returns true for a match and receives value (parsed JSON, a decoded
Avro/Protobuf/JSON Schema record, or text), key, headers, partition, offset,
timestamp, value_bytes, key_bytes, format, schema_id and decode_error. Omit it
to match all messages. Schema-encoded values are searched by
field exactly like JSON.

group_by summarises instead of listing: an expression over the same variables
returns a bucket name, and every match in the range is counted per bucket with
one example address each. Use it to break a DLQ down by error header, find hot
keys on a partition, or count messages per schema_id.

Every partition is read together, one chunk deep at a time, so a limited
newest-first search returns the newest matches in the topic rather than the
newest in whichever partition was read first. Kafka orders records only within a
partition, so matches are merged by timestamp; producers set timestamps unless
the topic uses LogAppendTime.

Kafka has no server-side search, so scans are bounded. Check complete,
stopped_reason and scanned_ranges before treating no matches as conclusive.
Use count_only or output_file for large result sets. A script that runs past
timeout_seconds is interrupted and counted in script_errors; scripts are not
memory-sandboxed, so keep predicates simple.
`

// Register adds the search_messages tool to the MCP server.
func Register(
	server *mcp.Server,
	admin *kadm.Client,
	reader *records.Reader,
	outputDir string,
) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "search_messages",
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, Output, error) {

			out, err := Run(ctx, admin, reader, outputDir, input)
			if err != nil {
				return nil, Output{}, fmt.Errorf("search messages: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run scans a bounded part of a topic and returns the messages that match.
func Run(
	ctx context.Context,
	admin *kadm.Client,
	reader *records.Reader,
	outputDir string,
	input Input,
) (Output, error) {

	options, err := newOptions(input)
	if err != nil {
		return Output{}, err
	}

	windows, err := resolveWindows(ctx, admin, input)
	if err != nil {
		return Output{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, options.timeout)
	defer cancel()

	out := Output{
		Topic:         input.Topic,
		Matches:       []records.Message{},
		ScannedRanges: []ScannedRange{},
		StoppedReason: reasonExhausted,
	}

	var export *exporter

	if options.outputFile != "" {
		export, err = newExporter(outputDir, options.outputFile)
		if err != nil {
			return Output{}, err
		}

		defer export.Close()
	}

	state := &scanState{
		options: options,
		export:  export,
		out:     &out,
		scanned: make(map[int32]*ScannedRange, len(windows)),
		matches: make(map[int32]int),
		// Counting and exporting both walk the whole range and keep no
		// bodies, so the match ceiling does not apply to them.
		collecting: !options.countOnly && export == nil,
	}

	// Every partition is scanned together, one chunk deep at a time, rather
	// than one partition being drained before the next begins.
	//
	// Kafka orders records within a partition and never across them, so "the
	// newest matches in this topic" can only mean newest by timestamp. Draining
	// partition 0 first and stopping at max_matches answers with whichever
	// partition happened to be read first: a topic whose newest messages live
	// on partition 5 would report day-old matches from partition 0 as the
	// newest. That is a confidently wrong answer rather than a slow one.
	//
	// One Scan takes every partition's current chunk, so this costs one
	// connection however many partitions the topic has.
	if err := state.scan(ctx, reader, input.Topic, windows); err != nil {
		return Output{}, err
	}

	if export != nil {
		if err := export.Close(); err != nil {
			return Output{}, fmt.Errorf("close output file: %w", err)
		}

		out.OutputFile = export.path()
		out.WrittenMessages = export.written
	}

	finish(&out, state.scanned, state.split, state.matches, options)
	finishGroups(&out, state.groups, options.maxGroups)

	return out, nil
}

// scanState is everything the readers of one search share.
type scanState struct {
	options    *options
	export     *exporter
	out        *Output
	collecting bool
	// ordered is true when a round reads a single range, so records arrive in
	// offset order and the first matches seen really are the oldest. An
	// oldest-first search can then stop the moment it has enough, instead of
	// reading the rest of the chunk. With several partitions in flight the
	// arrival order means nothing and the whole round has to be read.
	ordered bool

	// mu guards every field below it, because readers run concurrently.
	mu      sync.Mutex
	scanned map[int32]*ScannedRange
	matches map[int32]int
	// groups counts matches per group_by bucket.
	groups map[string]*Group

	// split holds the covered runs of a partition read by parallel readers,
	// which need not be contiguous, in place of its entry in scanned.
	split []ScannedRange
}

// scan reads every partition together, one chunk deep at a time.
//
// A round takes the current chunk of every partition in a single Scan, so the
// newest records of the whole topic are seen before any partition is read more
// deeply. Only when a round leaves the search short of max_matches does it step
// every partition back another chunk.
//
// The alternative — draining one partition before starting the next — makes
// "newest" mean "newest in the partition that happened to be read first".
func (s *scanState) scan(
	ctx context.Context,
	reader *records.Reader,
	topic string,
	windows []records.Range,
) error {

	// One partition split between readers runs them concurrently. Several
	// partitions are already read together by one session, so parallelism
	// does not apply there.
	if len(windows) == 1 && worthSplitting(windows[0], s.options.parallelism) {
		return s.scanParallel(ctx, reader, topic, windows[0])
	}

	rounds := rounds(windows, s.options.newestFirst)
	if len(rounds) == 0 {
		return nil
	}

	// A round covering one range delivers records in offset order, which is
	// what lets an oldest-first search stop at the match that satisfies it.
	s.setOrdered(len(rounds[0]) == 1)

	compiled, err := s.options.newScripts()
	if err != nil {
		return err
	}

	// The guard is what makes the timeout real. goja does not yield, so a
	// predicate that never returns is never preempted and the context is not
	// observed until the call comes back. Interrupting the runtime from
	// outside is the only thing that can stop it.
	stop := compiled.guard(ctx)
	defer stop()
	defer compiled.close()

	session, err := reader.Session(topic)
	if err != nil {
		return fmt.Errorf("search %s: %w", topic, err)
	}

	defer session.Close()

	for _, round := range rounds {
		if s.stopped() {
			return nil
		}

		// Records inside a chunk always arrive oldest first, because Kafka only
		// reads forward, and records from different partitions arrive in no
		// order at all. A newest-first search therefore cannot stop at the
		// first match: it reads the whole round and keeps the newest matches,
		// which is affordable because a round is bounded by chunkSize per
		// partition.
		found := make([]records.Message, 0, s.options.maxMatches)

		var scanErr error

		err := session.Scan(ctx, round, func(record *kgo.Record) bool {
			matched, err := s.visit(ctx, reader, record, compiled, &found)
			if err != nil {
				scanErr = err

				return false
			}

			return matched
		})
		if scanErr != nil {
			return scanErr
		}

		if err != nil {
			// A timeout is a bounded-search outcome, not a failure: the caller
			// still gets the matches found so far and is told why it stopped.
			if ctx.Err() != nil {
				s.timedOut(found)

				return nil
			}

			return fmt.Errorf("search %s: %w", topic, err)
		}

		s.keep(found)
	}

	return nil
}

// visit filters one record and reports whether scanning should continue.
//
// The record is decoded before the lock is taken: a first sight of a schema id
// fetches it from the registry, and holding the lock across that request would
// stall every other reader of the search.
func (s *scanState) visit(
	ctx context.Context,
	reader *records.Reader,
	record *kgo.Record,
	compiled *scripts,
	found *[]records.Message,
) (bool, error) {

	value := reader.Decode(ctx, record, serde.Value)
	key := reader.Decode(ctx, record, serde.Key)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.out.ScannedMessages++

	track(s.scanned, record)

	matched := true

	if compiled.filter != nil {
		var err error

		matched, err = compiled.filter.match(record, value, key)
		if err != nil {
			// A script that throws on one message says nothing about the
			// others, so the message is counted and the scan continues.
			s.out.ScriptErrors++

			matched = false
		}
	}

	if matched && compiled.group != nil {
		bucket, err := compiled.group.group(record, value, key)
		if err != nil {
			s.out.ScriptErrors++
			matched = false
		} else {
			s.bucket(bucket, record)
		}
	}

	if matched {
		s.out.MatchCount++
		s.matches[record.Partition]++

		switch {
		case s.export != nil:
			if err := s.export.write(
				records.RenderDecoded(record, key, value, s.options.maxValueBytes),
			); err != nil {
				return false, err
			}

		case s.collecting:
			*found = append(*found, records.RenderDecoded(record, key, value, s.options.maxValueBytes))

			// Stopping mid-round is only sound when the round reads a single
			// range, because then records arrive oldest first and the matches
			// in hand are genuinely the oldest. With several partitions in
			// flight they arrive in no order, so stopping here would return
			// whichever partition answered first rather than the oldest
			// matches. A round is bounded by chunkSize per partition, so
			// reading it out costs little, and keep sorts by timestamp after.
			if s.ordered && !s.options.newestFirst &&
				len(s.out.Matches)+len(*found) >= s.options.maxMatches {
				s.out.StoppedReason = reasonMaxMatches

				return false, nil
			}
		}
	}

	if s.out.ScannedMessages >= s.options.maxScanned {
		s.out.StoppedReason = reasonMaxScanned

		return false, nil
	}

	return true, nil
}

func (s *scanState) keep(found []records.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.collecting {
		return
	}

	// Merging across rounds, not only within one: a later round reads deeper
	// into every partition, and a match found there is older than what an
	// earlier round returned. Re-sorting the whole set keeps the reported
	// order true no matter which round produced a match.
	s.out.Matches = append(s.out.Matches, found...)
	s.out.Matches = keep(s.out.Matches, s.options)

	// Stopping once the limit is met is what keeps a narrow newest-first search
	// cheap. It is only sound because a round covers every partition at the
	// same depth: the matches in hand are the newest in the topic, not merely
	// the newest in the partition read first.
	if len(s.out.Matches) >= s.options.maxMatches &&
		s.out.StoppedReason == reasonExhausted {

		s.out.StoppedReason = reasonMaxMatches
	}
}

func (s *scanState) timedOut(found []records.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.out.StoppedReason = reasonTimeout

	if s.collecting {
		s.out.Matches = keep(append(s.out.Matches, found...), s.options)
	}
}

// setOrdered records whether records arrive in offset order for this search.
func (s *scanState) setOrdered(ordered bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ordered = ordered
}

func (s *scanState) stopped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.out.StoppedReason != reasonExhausted
}

// worthSplitting reports whether one partition's range is large enough to
// read with several readers. A small range is left to one: opening several
// connections to read a handful of messages costs more in setup than the
// concurrency saves.
func worthSplitting(window records.Range, parallelism int) bool {
	return parallelism > 1 && window.End-window.Start >= int64(parallelism)*chunkSize
}

// keep reduces a round's matches to the ones worth returning: the newest when
// searching newest first, the oldest otherwise.
//
// A round spans every partition, and records from different partitions arrive
// in no order at all, so arrival order says nothing about which match is newer.
// The matches are sorted by timestamp, which is the only thing comparable
// across partitions: Kafka orders offsets within a partition and never between
// them.
//
// Timestamps are set by the producer unless the topic uses LogAppendTime, so
// they can be skewed or even out of order within a partition. That is still the
// best available answer to "which of these is newer", and it is the answer the
// caller asked for.
func keep(found []records.Message, options *options) []records.Message {
	sortByTime(found, options.newestFirst)

	if len(found) > options.maxMatches {
		found = found[:options.maxMatches]
	}

	return found
}

// sortByTime orders matches by timestamp, newest or oldest first.
//
// Partition and offset break ties so that two messages sharing a timestamp —
// common when a producer batches — always come back in the same order. Without
// that, two identical searches could disagree.
func sortByTime(messages []records.Message, newestFirst bool) {
	sort.SliceStable(messages, func(i, j int) bool {
		left, right := messages[i], messages[j]

		if !left.Timestamp.Equal(right.Timestamp) {
			if newestFirst {
				return left.Timestamp.After(right.Timestamp)
			}

			return left.Timestamp.Before(right.Timestamp)
		}

		if left.Partition != right.Partition {
			return left.Partition < right.Partition
		}

		if newestFirst {
			return left.Offset > right.Offset
		}

		return left.Offset < right.Offset
	})
}

func track(scanned map[int32]*ScannedRange, record *kgo.Record) {
	current, ok := scanned[record.Partition]
	if !ok {
		scanned[record.Partition] = &ScannedRange{
			Partition: record.Partition,
			Start:     record.Offset,
			End:       record.Offset + 1,
		}

		return
	}

	if record.Offset < current.Start {
		current.Start = record.Offset
	}

	if record.Offset+1 > current.End {
		current.End = record.Offset + 1
	}
}

func finish(
	out *Output,
	scanned map[int32]*ScannedRange,
	split []ScannedRange,
	byPartition map[int32]int,
	options *options,
) {
	for _, rng := range scanned {
		out.ScannedRanges = append(out.ScannedRanges, *rng)
	}

	out.ScannedRanges = append(out.ScannedRanges, split...)

	// Map order is random, so sort for a stable report.
	sort.Slice(out.ScannedRanges, func(i, j int) bool {
		left, right := out.ScannedRanges[i], out.ScannedRanges[j]
		if left.Partition != right.Partition {
			return left.Partition < right.Partition
		}
		return left.Start < right.Start
	})

	for partition, count := range byPartition {
		out.MatchesByPart = append(out.MatchesByPart, PartitionCount{
			Partition: partition,
			Matches:   count,
		})
	}

	sort.Slice(out.MatchesByPart, func(i, j int) bool {
		return out.MatchesByPart[i].Partition < out.MatchesByPart[j].Partition
	})

	// Present the matches the way the caller asked to search. Grouping by
	// partition first would undo the merge: it would bury a match from a late
	// partition below every match from partition 0, whatever their times, and
	// the newest message in the topic would not be the one reported first.
	sortByTime(out.Matches, options.newestFirst)

	// When bodies are returned, the count reports what the caller received.
	// Counting and exporting instead report every match seen, which is the
	// whole point of asking for them.
	if !options.countOnly && out.OutputFile == "" {
		out.MatchCount = len(out.Matches)
	}

	out.Complete = out.StoppedReason == reasonExhausted
}

// options holds the validated, defaulted form of an Input.
type options struct {
	newestFirst   bool
	countOnly     bool
	outputFile    string
	maxMatches    int
	maxScanned    int
	maxValueBytes int
	timeout       time.Duration
	parallelism   int

	// source is the user script, empty when every message matches.
	source string

	// groupBy is the bucket expression, empty when not grouping.
	groupBy   string
	maxGroups int
}

// maxParallelism bounds how many readers one search may open. Each reader is
// a separate connection, so an unbounded value would let one call exhaust the
// broker's connection budget.
const maxParallelism = 16

func newOptions(input Input) (*options, error) {
	if input.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}

	if input.Parallelism < 0 || input.Parallelism > maxParallelism {
		return nil, fmt.Errorf(
			"parallelism must be between 1 and %d, got %d", maxParallelism, input.Parallelism)
	}

	for _, limit := range []struct {
		name           string
		value, maximum int
	}{
		{"max_matches", input.MaxMatches, maxMaxMatches},
		{"max_messages_scanned", input.MaxScanned, maxMaxScanned},
		{"max_value_bytes", input.MaxValueBytes, maxValueBytesCap},
		{"timeout_seconds", input.TimeoutSecond, maxTimeout},
		{"max_groups", input.MaxGroups, maxMaxGroups},
	} {
		if err := batch.Bounded(limit.name, limit.value, limit.maximum); err != nil {
			return nil, err
		}
	}

	if input.GroupBy != "" && input.OutputFile != "" {
		return nil, fmt.Errorf(
			"group_by and output_file cannot be combined: grouping returns counts, not messages to write")
	}

	if input.CountOnly && input.OutputFile != "" {
		return nil, fmt.Errorf(
			"count_only and output_file cannot be combined: counting returns no messages to write")
	}

	o := &options{
		source:        input.Script,
		groupBy:       input.GroupBy,
		maxGroups:     input.MaxGroups,
		countOnly:     input.CountOnly || input.GroupBy != "",
		outputFile:    input.OutputFile,
		maxMatches:    input.MaxMatches,
		maxScanned:    input.MaxScanned,
		maxValueBytes: input.MaxValueBytes,
		timeout:       time.Duration(input.TimeoutSecond) * time.Second,
		parallelism:   input.Parallelism,
	}

	if o.maxMatches <= 0 {
		o.maxMatches = defaultMaxMatches
	}

	if o.maxScanned <= 0 {
		o.maxScanned = defaultMaxScanned
	}

	if o.maxValueBytes <= 0 {
		o.maxValueBytes = defaultMaxValueBytes
	}

	if o.timeout <= 0 {
		o.timeout = defaultTimeout
	}

	if o.parallelism <= 0 {
		o.parallelism = 1
	}

	if o.maxGroups <= 0 {
		o.maxGroups = defaultMaxGroups
	}

	switch input.Direction {
	case "", "newest_first":
		o.newestFirst = true
	case "oldest_first":
		o.newestFirst = false
	default:
		return nil, fmt.Errorf(
			"direction must be newest_first or oldest_first, got %q", input.Direction)
	}

	// Compiling here reports a malformed script before a single message is
	// read, rather than after a scan that could not have matched anything.
	compiled, err := o.newScripts()
	if err != nil {
		return nil, err
	}
	compiled.close()

	return o, nil
}

// scripts are the compiled scripts one reader evaluates. Either may be nil.
type scripts struct {
	filter *filter
	group  *filter
}

// newScripts compiles the scripts for one reader. Each reader needs its own,
// because a goja runtime cannot be used from two goroutines at once.
func (o *options) newScripts() (*scripts, error) {
	compiled := &scripts{}

	if o.source != "" {
		filter, err := compileScript(o.source)
		if err != nil {
			return nil, err
		}
		compiled.filter = filter
	}

	if o.groupBy != "" {
		group, err := compileScript(o.groupBy)
		if err != nil {
			compiled.close()
			return nil, fmt.Errorf("group_by: %w", err)
		}
		compiled.group = group
	}

	return compiled, nil
}

// guard interrupts both scripts once ctx is done; the returned function stops
// the watchers.
func (c *scripts) guard(ctx context.Context) func() {
	stopFilter := c.filter.guard(ctx)
	stopGroup := c.group.guard(ctx)

	return func() {
		stopFilter()
		stopGroup()
	}
}

func (c *scripts) close() {
	if c.filter != nil {
		c.filter.Close()
	}
	if c.group != nil {
		c.group.Close()
	}
}

// resolveWindows turns the requested partitions, offsets and timestamps into
// the concrete offset range to scan in each partition.
func resolveWindows(
	ctx context.Context,
	admin *kadm.Client,
	input Input,
) ([]records.Range, error) {

	details, err := admin.ListTopics(ctx, input.Topic)
	if err != nil {
		return nil, fmt.Errorf("list topic %q: %w", input.Topic, err)
	}

	detail, ok := details[input.Topic]
	if !ok {
		return nil, fmt.Errorf("topic %q does not exist", input.Topic)
	}

	if detail.Err != nil {
		return nil, fmt.Errorf("topic %q: %w", input.Topic, detail.Err)
	}

	wanted := make(map[int32]bool, len(input.Partitions))

	for _, partition := range input.Partitions {
		if _, ok := detail.Partitions[partition]; !ok {
			return nil, fmt.Errorf(
				"topic %q has no partition %d", input.Topic, partition)
		}

		wanted[partition] = true
	}

	starts, err := admin.ListStartOffsets(ctx, input.Topic)
	if err != nil {
		return nil, fmt.Errorf("list start offsets for %q: %w", input.Topic, err)
	}

	ends, err := admin.ListEndOffsets(ctx, input.Topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets for %q: %w", input.Topic, err)
	}

	fromTime, err := offsetsAt(ctx, admin, input.Topic, input.FromTimestamp)
	if err != nil {
		return nil, err
	}

	toTime, err := offsetsAt(ctx, admin, input.Topic, input.ToTimestamp)
	if err != nil {
		return nil, err
	}

	windows := make([]records.Range, 0, len(detail.Partitions))

	for id := range detail.Partitions {
		if len(wanted) > 0 && !wanted[id] {
			continue
		}

		start, hasStart := starts.Lookup(input.Topic, id)
		end, hasEnd := ends.Lookup(input.Topic, id)

		if !hasStart || !hasEnd {
			continue
		}

		window := records.Range{
			Partition: id,
			Start:     start.Offset,
			End:       end.Offset,
		}

		// Explicit offsets and timestamps both narrow the window; whichever is
		// tighter wins, so the two can be combined safely.
		if at, ok := fromTime[id]; ok && at > window.Start {
			window.Start = at
		}

		if at, ok := toTime[id]; ok && at < window.End {
			window.End = at
		}

		if input.FromOffset != nil && *input.FromOffset > window.Start {
			window.Start = *input.FromOffset
		}

		if input.ToOffset != nil && *input.ToOffset < window.End {
			window.End = *input.ToOffset
		}

		if window.Empty() {
			continue
		}

		windows = append(windows, window)
	}

	sort.Slice(windows, func(i, j int) bool {
		return windows[i].Partition < windows[j].Partition
	})

	return windows, nil
}

func offsetsAt(
	ctx context.Context,
	admin *kadm.Client,
	topic string,
	at *time.Time,
) (map[int32]int64, error) {

	if at == nil {
		return nil, nil
	}

	listed, err := admin.ListOffsetsAfterMilli(ctx, at.UnixMilli(), topic)
	if err != nil {
		return nil, fmt.Errorf("resolve time %s in %q: %w", at, topic, err)
	}

	resolved := make(map[int32]int64)

	listed.Each(func(offset kadm.ListedOffset) {
		if offset.Err != nil || offset.Offset < 0 {
			return
		}

		resolved[offset.Partition] = offset.Offset
	})

	return resolved, nil
}

// rounds groups the partitions' chunks so that one round reads the same depth
// of every partition at once.
//
// Round 0 is the newest chunk of every partition (or the oldest, for an
// oldest-first search), round 1 the chunk behind it, and so on. Scanning a
// round therefore covers the whole topic at that depth before going deeper,
// which is what lets a limited search return the newest matches of the topic
// rather than of whichever partition was read first.
//
// Partitions run out at different depths, because they rarely hold the same
// number of records. A round simply contains fewer partitions once the short
// ones are exhausted.
func rounds(windows []records.Range, newestFirst bool) [][]records.Range {
	// One partition needs no merging: its chunks are already in the order the
	// caller asked for. Each chunk is its own round, because a Scan consumes
	// at most one range per partition — it keys its bookkeeping by partition
	// number, so two chunks of one partition in a round would collide.
	if len(windows) == 1 {
		sliced := chunks(windows, newestFirst)

		grouped := make([][]records.Range, 0, len(sliced))

		for _, chunk := range sliced {
			grouped = append(grouped, []records.Range{chunk})
		}

		return grouped
	}

	perPartition := make([][]records.Range, 0, len(windows))
	deepest := 0

	for _, window := range windows {
		sliced := chunks([]records.Range{window}, newestFirst)
		if len(sliced) == 0 {
			continue
		}

		perPartition = append(perPartition, sliced)

		if len(sliced) > deepest {
			deepest = len(sliced)
		}
	}

	grouped := make([][]records.Range, 0, deepest)

	for depth := range deepest {
		round := make([]records.Range, 0, len(perPartition))

		for _, chunked := range perPartition {
			if depth < len(chunked) {
				round = append(round, chunked[depth])
			}
		}

		if len(round) > 0 {
			grouped = append(grouped, round)
		}
	}

	return grouped
}

// chunks splits the windows into bounded scan ranges. Kafka only reads
// forward, so a newest-first search is done by reading the last chunk of each
// partition first and walking backwards.
func chunks(windows []records.Range, newestFirst bool) []records.Range {
	out := make([]records.Range, 0, len(windows))

	for _, window := range windows {
		if newestFirst {
			for end := window.End; end > window.Start; end -= chunkSize {
				start := end - chunkSize
				if start < window.Start {
					start = window.Start
				}

				out = append(out, records.Range{
					Partition: window.Partition,
					Start:     start,
					End:       end,
				})
			}

			continue
		}

		for start := window.Start; start < window.End; start += chunkSize {
			end := start + chunkSize
			if end > window.End {
				end = window.End
			}

			out = append(out, records.Range{
				Partition: window.Partition,
				Start:     start,
				End:       end,
			})
		}
	}

	return out
}

// bucket counts a match in its group_by bucket. The caller holds s.mu.
func (s *scanState) bucket(key string, record *kgo.Record) {
	if s.groups == nil {
		s.groups = make(map[string]*Group)
	}

	group, ok := s.groups[key]
	if !ok {
		group = &Group{Key: key, Example: Address{Partition: record.Partition, Offset: record.Offset}}
		s.groups[key] = group
	}

	group.Count++

	// The example is the newest message of the bucket: the one most likely to
	// reflect what is happening now. Within one partition the higher offset
	// is newer; across partitions offsets are not comparable, so the first
	// partition seen keeps the example.
	if record.Partition == group.Example.Partition && record.Offset > group.Example.Offset {
		group.Example.Offset = record.Offset
	}
}

// finishGroups reports the largest buckets, largest first.
func finishGroups(out *Output, groups map[string]*Group, maxGroups int) {
	if groups == nil {
		return
	}

	out.Groups = make([]Group, 0, len(groups))
	for _, group := range groups {
		out.Groups = append(out.Groups, *group)
	}

	sort.Slice(out.Groups, func(i, j int) bool {
		if out.Groups[i].Count != out.Groups[j].Count {
			return out.Groups[i].Count > out.Groups[j].Count
		}
		return out.Groups[i].Key < out.Groups[j].Key
	})

	if len(out.Groups) > maxGroups {
		out.Groups = out.Groups[:maxGroups]
		out.GroupsTruncated = true
	}
}
