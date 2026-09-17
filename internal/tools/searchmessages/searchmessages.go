// Package searchmessages implements the search_messages MCP tool.
package searchmessages

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
)

// Input is the argument set accepted by the search_messages tool.
type Input struct {
	Topic         string         `json:"topic" jsonschema:"Topic to search. Matched exactly and case-sensitively."`
	Query         string         `json:"query,omitempty" jsonschema:"Text to look for, interpreted according to match. Either query or filter is required, and giving both requires a message to satisfy both."`
	Filter        map[string]any `json:"filter,omitempty" jsonschema:"Optional structured filter over JSON message values. A node is {\"and\":[...]}, {\"or\":[...]}, {\"not\":{...}} or a leaf {\"field\":\"payload.amount\",\"op\":\"gte\",\"value\":500}. Paths are dotted and may index arrays as items[0] or match any element as items[*]. Operators: eq, ne, gt, gte, lt, lte, contains, starts_with, ends_with, regex, in, exists, is_null, is_not_null, is_true, is_false. The last five take no value. Messages whose value is not JSON never match a filter."`
	SearchIn      []string       `json:"search_in,omitempty" jsonschema:"Optional parts of the message the query searches: value, key, headers. Defaults to value and key. Searching the key alone is far more precise when the key is the identifier, because a bare id also occurs inside unrelated numbers in the value."`
	Match         string         `json:"match,omitempty" jsonschema:"Optional match mode for query: contains (default, case-insensitive substring), exact (whole field equals the query, case-sensitive), or regex (RE2 pattern)."`
	Partitions    []int32        `json:"partitions,omitempty" jsonschema:"Optional partitions to restrict the search to. Defaults to every partition. Do not guess a partition from a message key: producers may set the partition explicitly, so the key does not determine it."`
	FromOffset    *int64         `json:"from_offset,omitempty" jsonschema:"Optional inclusive offset to start scanning from, applied to every searched partition."`
	ToOffset      *int64         `json:"to_offset,omitempty" jsonschema:"Optional exclusive offset to stop scanning at, applied to every searched partition."`
	FromTimestamp *time.Time     `json:"from_timestamp,omitempty" jsonschema:"Optional inclusive start time (RFC3339). Resolved to the first offset at or after this time."`
	ToTimestamp   *time.Time     `json:"to_timestamp,omitempty" jsonschema:"Optional exclusive end time (RFC3339). Resolved to the first offset at or after this time."`
	Direction     string         `json:"direction,omitempty" jsonschema:"Optional scan direction: newest_first (default) or oldest_first. Decides which matches are found first when max_matches cuts the search short."`
	MaxMatches    int            `json:"max_matches,omitempty" jsonschema:"Optional maximum number of matches to return. Defaults to 10."`
	MaxScanned    int            `json:"max_messages_scanned,omitempty" jsonschema:"Optional maximum number of messages to read before giving up. Defaults to 10000."`
	MaxValueBytes int            `json:"max_value_bytes,omitempty" jsonschema:"Optional maximum value bytes to return per match. Defaults to 512. Longer values are cut and flagged with truncated=true."`
	TimeoutSecond int            `json:"timeout_seconds,omitempty" jsonschema:"Optional wall-clock limit for the scan in seconds. Defaults to 30."`
	CountOnly     bool           `json:"count_only,omitempty" jsonschema:"Optional. When true, scan the whole range and return only how many messages matched, with a per-partition breakdown and no message bodies. Use this first when a query may match a great many messages, then ask the user how they want them before fetching any."`
	OutputFile    string         `json:"output_file,omitempty" jsonschema:"Optional file name to write every match to, as one JSON message per line. Use this instead of returning thousands of messages. A name only, not a path: the server chooses the directory. The response reports the path, the number written and a short preview."`
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
	NonJSONSkipped  int               `json:"non_json_skipped,omitempty"`
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
Search a Kafka topic for messages matching a text query, a structured filter
over JSON values, or both, and return the matches with their partition, offset,
timestamp, key, value and headers.

Kafka cannot search server-side, so this reads messages and filters them
client-side. Every search is therefore bounded, and the result reports what was
actually covered: "scanned_messages", "scanned_ranges" and "stopped_reason".

"stopped_reason" is one of:
  range_exhausted - the whole requested range was read
  max_matches     - stopped after enough matches were found
  max_scanned     - hit the max_messages_scanned ceiling
  timeout         - hit the timeout_seconds ceiling

"complete" is true only when the range was exhausted. An empty match list is
only conclusive when complete is true; otherwise the message may exist outside
the part that was scanned. Narrow the search with partitions, an offset range
or a time range and try again.

Prefer the narrowest query available. When the key identifies the message, use
search_in ["key"] with match "exact": a bare id such as 123 also occurs inside
unrelated numbers in the value, and those false positives can fill max_matches
and hide the message actually wanted. Use sample_messages first to learn
whether the key carries the identifier.

The "filter" argument matches structure rather than text, for example every
message whose event type is NEW and whose amount is at least 500:

  {"and": [
    {"field": "eventType", "op": "eq", "value": "NEW"},
    {"field": "payload.amount", "op": "gte", "value": 500}
  ]}

A missing field never matches. "is_null" requires the field to be present and
null, while {"not": {... "is_null"}} also matches messages lacking the field.
Messages whose value is not JSON are counted in "non_json_skipped", so a zero
match count over a non-JSON topic is not mistaken for a real answer.

When a query may match a great many messages, set "count_only" first to learn
how many there are without fetching any, then ask the user what they want
before returning bodies. For large result sets, "output_file" writes every
match to a file instead of returning them.

Use describe_topic first to see how large the topic is, and get_message
afterwards to read a match in full, since values here are truncated to
max_value_bytes.
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

	// One connection serves every chunk. Opening a client per chunk would cost
	// a connection per 500 offsets, so a default-sized scan would open twenty.
	session, err := reader.Session(input.Topic)
	if err != nil {
		return Output{}, fmt.Errorf("search %s: %w", input.Topic, err)
	}

	defer session.Close()

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

	scanned := make(map[int32]*ScannedRange, len(windows))
	byPartition := make(map[int32]int)

	// Counting and exporting both walk the whole range and keep no bodies, so
	// the match ceiling does not apply to them.
	collecting := !options.countOnly && export == nil

	var exportErr error

	for _, chunk := range chunks(windows, options.newestFirst) {
		if out.StoppedReason != reasonExhausted {
			break
		}

		// Records inside a chunk always arrive oldest first, because Kafka
		// only reads forward. A newest-first search therefore cannot stop at
		// the first match in a chunk: that is the chunk's oldest match. It
		// reads the whole chunk and keeps the newest matches instead, which is
		// affordable because a chunk is bounded to chunkSize records.
		found := make([]records.Message, 0, options.maxMatches)

		err := session.Scan(
			ctx,
			[]records.Range{chunk},
			func(record *kgo.Record) bool {
				out.ScannedMessages++

				track(scanned, record)

				switch options.verdict(record) {
				case verdictMatch:
					out.MatchCount++
					byPartition[record.Partition]++

					switch {
					case export != nil:
						if err := export.write(
							records.Render(record, options.maxValueBytes),
						); err != nil {
							exportErr = err

							return false
						}

					case collecting:
						found = append(found, records.Render(record, options.maxValueBytes))

						if !options.newestFirst &&
							len(out.Matches)+len(found) >= options.maxMatches {
							out.StoppedReason = reasonMaxMatches

							return false
						}
					}

				case verdictNonJSON:
					out.NonJSONSkipped++
				}

				if out.ScannedMessages >= options.maxScanned {
					out.StoppedReason = reasonMaxScanned

					return false
				}

				return true
			},
		)
		if exportErr != nil {
			return Output{}, exportErr
		}

		if err != nil {
			// A timeout is a bounded-search outcome, not a failure: the caller
			// still gets the matches found so far and is told why it stopped.
			if ctx.Err() != nil {
				out.StoppedReason = reasonTimeout

				if collecting {
					out.Matches = append(out.Matches, keep(found, options)...)
				}

				break
			}

			return Output{}, fmt.Errorf("search %s: %w", input.Topic, err)
		}

		if !collecting {
			continue
		}

		out.Matches = append(out.Matches, keep(found, options)...)

		if len(out.Matches) >= options.maxMatches {
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

	finish(&out, scanned, byPartition, options)

	return out, nil
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

	query     string
	hasQuery  bool
	lowered   string
	pattern   *regexp.Regexp
	exact     bool
	inValue   bool
	inKey     bool
	inHeader  bool
	filter    *compiledFilter
	hasFilter bool
}

// verdict is what a scanned record amounted to.
type verdict int

const (
	verdictNoMatch verdict = iota
	verdictMatch

	// verdictNonJSON is a record a filter could not even be applied to,
	// counted separately so that "no matches" over a non-JSON topic cannot be
	// mistaken for "no message satisfied the condition".
	verdictNonJSON
)

func newOptions(input Input) (*options, error) {
	if input.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}

	hasQuery := input.Query != ""
	hasFilter := len(input.Filter) > 0

	if !hasQuery && !hasFilter {
		return nil, fmt.Errorf(
			"query or filter is required, because a search with neither would match every message")
	}

	if input.CountOnly && input.OutputFile != "" {
		return nil, fmt.Errorf(
			"count_only and output_file cannot be combined: counting returns no messages to write")
	}

	o := &options{
		query:         input.Query,
		hasQuery:      hasQuery,
		hasFilter:     hasFilter,
		lowered:       strings.ToLower(input.Query),
		countOnly:     input.CountOnly,
		outputFile:    input.OutputFile,
		maxMatches:    input.MaxMatches,
		maxScanned:    input.MaxScanned,
		maxValueBytes: input.MaxValueBytes,
		timeout:       time.Duration(input.TimeoutSecond) * time.Second,
	}

	if hasFilter {
		// The filter arrives as a decoded object so that MCP clients can send
		// it as JSON rather than as a string. The compiler works on raw JSON,
		// so encode it back.
		raw, err := json.Marshal(input.Filter)
		if err != nil {
			return nil, fmt.Errorf("filter: %w", err)
		}

		filter, err := compileFilter(raw)
		if err != nil {
			return nil, fmt.Errorf("filter: %w", err)
		}

		o.filter = filter
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

	switch input.Direction {
	case "", "newest_first":
		o.newestFirst = true
	case "oldest_first":
		o.newestFirst = false
	default:
		return nil, fmt.Errorf(
			"direction must be newest_first or oldest_first, got %q", input.Direction)
	}

	switch input.Match {
	case "", "contains":
	case "exact":
		o.exact = true
	case "regex":
		if !hasQuery {
			return nil, fmt.Errorf("match regex needs a query")
		}

		pattern, err := regexp.Compile(input.Query)
		if err != nil {
			return nil, fmt.Errorf("query is not a valid regular expression: %w", err)
		}

		o.pattern = pattern
	default:
		return nil, fmt.Errorf(
			"match must be contains, exact or regex, got %q", input.Match)
	}

	if len(input.SearchIn) == 0 {
		o.inValue = true
		o.inKey = true

		return o, nil
	}

	for _, field := range input.SearchIn {
		switch strings.ToLower(field) {
		case "value":
			o.inValue = true
		case "key":
			o.inKey = true
		case "headers":
			o.inHeader = true
		default:
			return nil, fmt.Errorf(
				"search_in must contain only value, key or headers, got %q", field)
		}
	}

	return o, nil
}

// verdict decides what one record amounts to. A query and a filter given
// together must both hold, so a caller can combine a cheap text match with a
// precise structural one.
func (o *options) verdict(record *kgo.Record) verdict {
	if o.hasQuery && !o.matches(record) {
		return verdictNoMatch
	}

	if !o.hasFilter {
		return verdictMatch
	}

	if !json.Valid(record.Value) {
		return verdictNonJSON
	}

	if !o.filter.Match(record.Value) {
		return verdictNoMatch
	}

	return verdictMatch
}

func (o *options) matches(record *kgo.Record) bool {
	if o.inValue && o.hit(record.Value) {
		return true
	}

	if o.inKey && o.hit(record.Key) {
		return true
	}

	if o.inHeader {
		for _, header := range record.Headers {
			if o.hit([]byte(header.Key)) || o.hit(header.Value) {
				return true
			}
		}
	}

	return false
}

func (o *options) hit(field []byte) bool {
	if len(field) == 0 {
		return false
	}

	switch {
	case o.pattern != nil:
		return o.pattern.Match(field)
	case o.exact:
		return string(field) == o.query
	default:
		return strings.Contains(strings.ToLower(string(field)), o.lowered)
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
