// Package searchmessages implements the search_messages MCP tool.
package searchmessages

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
)

// Input is the argument set accepted by the search_messages tool.
type Input struct {
	Topic         string     `json:"topic" jsonschema:"Topic to search. Matched exactly and case-sensitively."`
	Script        string     `json:"script,omitempty" jsonschema:"Optional JavaScript that decides whether a message matches. Return true to keep it. In scope: value (the parsed JSON document, or the raw text when the message is not JSON), key (string or null), headers (object of header name to string), partition, offset and timestamp (a Date). Examples: return key === 'order-123'; return value.eventType === 'NEW' && value.payload.amount >= 500; return value.payload.cancelledAt === null. Omit to match every message."`
	Parallelism   int        `json:"parallelism,omitempty" jsonschema:"Optional number of concurrent readers, from 1 to 16. Defaults to 1. Each reader takes its own slice of a partition, so a topic with one partition is parallelised too. Worth using for count_only, output_file or a full scan; a narrow newest-first search is usually faster without it, because sequential scanning can stop after the newest chunk."`
	Partitions    []int32    `json:"partitions,omitempty" jsonschema:"Optional partitions to restrict the search to. Defaults to every partition. Do not guess a partition from a message key: producers may set the partition explicitly, so the key does not determine it."`
	FromOffset    *int64     `json:"from_offset,omitempty" jsonschema:"Optional inclusive offset to start scanning from, applied to every searched partition."`
	ToOffset      *int64     `json:"to_offset,omitempty" jsonschema:"Optional exclusive offset to stop scanning at, applied to every searched partition."`
	FromTimestamp *time.Time `json:"from_timestamp,omitempty" jsonschema:"Optional inclusive start time (RFC3339). Resolved to the first offset at or after this time."`
	ToTimestamp   *time.Time `json:"to_timestamp,omitempty" jsonschema:"Optional exclusive end time (RFC3339). Resolved to the first offset at or after this time."`
	Direction     string     `json:"direction,omitempty" jsonschema:"Optional scan direction: newest_first (default) or oldest_first. Decides which matches are found first when max_matches cuts the search short."`
	MaxMatches    int        `json:"max_matches,omitempty" jsonschema:"Optional maximum number of matches to return. Defaults to 10."`
	MaxScanned    int        `json:"max_messages_scanned,omitempty" jsonschema:"Optional maximum number of messages to read before giving up. Defaults to 10000."`
	MaxValueBytes int        `json:"max_value_bytes,omitempty" jsonschema:"Optional maximum value bytes to return per match. Defaults to 512. Longer values are cut and flagged with truncated=true."`
	TimeoutSecond int        `json:"timeout_seconds,omitempty" jsonschema:"Optional wall-clock limit for the scan in seconds. Defaults to 30."`
	CountOnly     bool       `json:"count_only,omitempty" jsonschema:"Optional. When true, scan the whole range and return only how many messages matched, with a per-partition breakdown and no message bodies. Use this first when a query may match a great many messages, then ask the user how they want them before fetching any."`
	OutputFile    string     `json:"output_file,omitempty" jsonschema:"Optional file name to write every match to, as one JSON message per line. Use this instead of returning thousands of messages. A name only, not a path: the server chooses the directory. The response reports the path, the number written and a short preview."`
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
)

const description = `
Search message key, value, headers or metadata with a JavaScript predicate.
The script returns true for a match and receives value (parsed JSON or text),
key, headers, partition, offset and timestamp. Omit it to match all messages.

Kafka has no server-side search, so scans are bounded. Check complete,
stopped_reason and scanned_ranges before treating no matches as conclusive.
Use count_only or output_file for large result sets. Scripts are time-limited
but not memory-sandboxed; keep predicates simple.
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

	// Partitions are scanned one at a time, with every reader working on the
	// same partition. That keeps the number of connections at parallelism
	// regardless of how many partitions the topic has, and it means a
	// partition is complete before the next begins, so its matches can be
	// merged without waiting on other partitions.
	for _, window := range windows {
		if out.StoppedReason != reasonExhausted {
			break
		}

		if err := state.scanPartition(ctx, reader, input.Topic, window); err != nil {
			return Output{}, err
		}

		// Stopping here means later partitions go unscanned. That is reported
		// through stopped_reason and scanned_ranges, so a caller can see the
		// answer is partial rather than assume the topic was covered.
		if state.collecting && len(out.Matches) >= options.maxMatches {
			out.Matches = out.Matches[:options.maxMatches]

			if out.StoppedReason == reasonExhausted {
				out.StoppedReason = reasonMaxMatches
			}
		}
	}

	if export != nil {
		if err := export.Close(); err != nil {
			return Output{}, fmt.Errorf("close output file: %w", err)
		}

		out.OutputFile = export.path()
		out.WrittenMessages = export.written
	}

	finish(&out, state.scanned, state.matches, options)

	return out, nil
}

// scanState is everything the readers of one search share.
type scanState struct {
	options    *options
	export     *exporter
	out        *Output
	collecting bool

	// mu guards every field below it, because readers run concurrently.
	mu      sync.Mutex
	scanned map[int32]*ScannedRange
	matches map[int32]int
}

// scanPartition reads one partition, splitting its offset range between the
// configured number of readers.
func (s *scanState) scanPartition(
	ctx context.Context,
	reader *records.Reader,
	topic string,
	window records.Range,
) error {

	slices := splitRange(window, s.options.parallelism)

	if len(slices) == 1 {
		return s.scanSlice(ctx, reader, topic, slices[0])
	}

	group, groupCtx := errgroup.WithContext(ctx)

	for _, slice := range slices {
		group.Go(func() error {
			return s.scanSlice(groupCtx, reader, topic, slice)
		})
	}

	return group.Wait()
}

// scanSlice reads one contiguous offset range with a connection and a script
// of its own, because neither can be shared between goroutines.
func (s *scanState) scanSlice(
	ctx context.Context,
	reader *records.Reader,
	topic string,
	slice records.Range,
) error {

	filter, err := s.options.newScript()
	if err != nil {
		return err
	}

	if filter != nil {
		defer filter.close()
	}

	session, err := reader.Session(topic)
	if err != nil {
		return fmt.Errorf("search %s: %w", topic, err)
	}

	defer session.Close()

	for _, chunk := range chunks([]records.Range{slice}, s.options.newestFirst) {
		if s.stopped() {
			return nil
		}

		// Records inside a chunk always arrive oldest first, because Kafka
		// only reads forward. A newest-first search therefore cannot stop at
		// the first match in a chunk: that is the chunk's oldest match. It
		// reads the whole chunk and keeps the newest matches instead, which is
		// affordable because a chunk is bounded to chunkSize records.
		found := make([]records.Message, 0, s.options.maxMatches)

		var scanErr error

		err := session.Scan(ctx, []records.Range{chunk}, func(record *kgo.Record) bool {
			matched, err := s.visit(record, filter, &found)
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
func (s *scanState) visit(
	record *kgo.Record,
	filter *script,
	found *[]records.Message,
) (bool, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	s.out.ScannedMessages++

	track(s.scanned, record)

	matched := true

	if filter != nil {
		var err error

		matched, err = filter.match(record)
		if err != nil {
			// A script that throws on one message says nothing about the
			// others, so the message is counted and the scan continues.
			s.out.ScriptErrors++

			matched = false
		}
	}

	if matched {
		s.out.MatchCount++
		s.matches[record.Partition]++

		switch {
		case s.export != nil:
			if err := s.export.write(
				records.Render(record, s.options.maxValueBytes),
			); err != nil {
				return false, err
			}

		case s.collecting:
			*found = append(*found, records.Render(record, s.options.maxValueBytes))

			if !s.options.newestFirst &&
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

	s.out.Matches = append(s.out.Matches, keep(found, s.options)...)

	// Stopping as soon as the limit is met is what keeps a narrow newest-first
	// search cheap: it reads the newest chunk and goes no further. Without
	// this the scan would read the whole range to return the same answer.
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
		s.out.Matches = append(s.out.Matches, keep(found, s.options)...)
	}
}

func (s *scanState) stopped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.out.StoppedReason != reasonExhausted
}

// splitRange divides one partition's offsets between readers.
//
// A range too small to be worth dividing is left whole: opening several
// connections to read a handful of messages costs more in setup than the
// concurrency saves.
func splitRange(window records.Range, parallelism int) []records.Range {
	size := window.End - window.Start

	if parallelism <= 1 || size < int64(parallelism)*chunkSize {
		return []records.Range{window}
	}

	slices := make([]records.Range, 0, parallelism)
	per := size / int64(parallelism)

	for i := 0; i < parallelism; i++ {
		start := window.Start + int64(i)*per
		end := start + per

		// The last slice takes the remainder, so no offset is left unread.
		if i == parallelism-1 {
			end = window.End
		}

		slices = append(slices, records.Range{
			Partition: window.Partition,
			Start:     start,
			End:       end,
		})
	}

	return slices
}

// keep reduces a chunk's matches to the ones worth returning: the newest when
// searching newest first, the oldest otherwise.
func keep(found []records.Message, options *options) []records.Message {
	if len(found) <= options.maxMatches {
		if options.newestFirst {
			reverse(found)
		}

		return found
	}

	if options.newestFirst {
		found = found[len(found)-options.maxMatches:]
		reverse(found)

		return found
	}

	return found[:options.maxMatches]
}

func reverse(messages []records.Message) {
	for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
		messages[i], messages[j] = messages[j], messages[i]
	}
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
	byPartition map[int32]int,
	options *options,
) {
	for _, rng := range scanned {
		out.ScannedRanges = append(out.ScannedRanges, *rng)
	}

	// Map order is random, so sort for a stable report.
	sort.Slice(out.ScannedRanges, func(i, j int) bool {
		return out.ScannedRanges[i].Partition < out.ScannedRanges[j].Partition
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

	// Matches arrive in the order the chunks were read. Present them the way
	// the caller asked to search: newest first means highest offset first.
	sort.SliceStable(out.Matches, func(i, j int) bool {
		if out.Matches[i].Partition != out.Matches[j].Partition {
			return out.Matches[i].Partition < out.Matches[j].Partition
		}

		if options.newestFirst {
			return out.Matches[i].Offset > out.Matches[j].Offset
		}

		return out.Matches[i].Offset < out.Matches[j].Offset
	})

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

	if input.CountOnly && input.OutputFile != "" {
		return nil, fmt.Errorf(
			"count_only and output_file cannot be combined: counting returns no messages to write")
	}

	o := &options{
		source:        input.Script,
		countOnly:     input.CountOnly,
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
	if o.source != "" {
		compiled, err := compileScript(o.source)
		if err != nil {
			return nil, err
		}

		compiled.close()
	}

	return o, nil
}

// newScript builds a script for one reader. Each reader needs its own,
// because a goja runtime cannot be used from two goroutines at once.
func (o *options) newScript() (*script, error) {
	if o.source == "" {
		return nil, nil
	}

	return compileScript(o.source)
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
