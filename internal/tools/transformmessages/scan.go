package transformmessages

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"unicode/utf8"

	"github.com/dop251/goja"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/outputfile"
	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
	"github.com/denizgursoy/kafka-mcp/internal/domain/script"
	"github.com/denizgursoy/kafka-mcp/internal/domain/serde"
)

// fileItem is one line of output_file. Its fields and tags are produce_message's
// item, so the file can be sent with produce_message input_file; the test suite
// sends one to prove the two still agree.
type fileItem struct {
	Topic              string            `json:"topic"`
	Value              string            `json:"value"`
	Key                string            `json:"key,omitempty"`
	Headers            map[string]string `json:"headers,omitempty"`
	Partition          *int32            `json:"partition,omitempty"`
	Encoding           string            `json:"encoding,omitempty"`
	DestinationCluster string            `json:"destination_cluster,omitempty"`
}

// runState is everything one run shares while it reads.
type runState struct {
	input       Input
	window      records.Window
	source      *kafkaclient.Client
	destination *kafkaclient.Client
	destTopic   string
	filter      *script.Script
	transform   *script.Script
	file        *outputfile.Writer
	out         *Output
	maxMessages int
	provenance  []kgo.RecordHeader

	// writeCtx is the caller's context rather than the scan's, so a write
	// already started when the timeout fires is finished and reported rather
	// than abandoned halfway.
	writeCtx context.Context

	pending []pendingRecord
	errors  map[string]*ErrorGroup

	// firstFailed is the source offset of the first message whose write
	// failed. A resumed run must start there.
	firstFailed *int64

	stopped bool
}

type pendingRecord struct {
	record *kgo.Record
	from   Address
}

// scan reads each partition in turn, oldest first, transforming as it goes.
func (s *runState) scan(ctx context.Context, windows []records.Range) error {
	for _, window := range windows {
		if s.stopped {
			break
		}

		if err := s.scanPartition(ctx, window); err != nil {
			return err
		}
	}

	return nil
}

func (s *runState) scanPartition(ctx context.Context, window records.Range) error {
	session, err := s.source.Reader().Session(ctx, s.input.Topic)
	if err != nil {
		return err
	}
	defer session.Close()

	covered := ScannedRange{Partition: window.Partition, Start: window.Start, End: window.Start}

	defer func() {
		s.out.ScannedRanges = append(s.out.ScannedRanges, covered)
	}()

	err = session.Scan(ctx, []records.Range{window}, func(record *kgo.Record) bool {
		if ctx.Err() != nil {
			return false
		}

		if !s.visit(ctx, record) {
			return false
		}

		covered.End = record.Offset + 1

		return true
	})

	if ctx.Err() != nil {
		// The timeout is a stopping point the caller asked for, not a
		// failure: what was transformed so far is reported.
		s.stop(reasonTimeout)

		return nil
	}

	if err != nil {
		return fmt.Errorf("read %s partition %d: %w", s.input.Topic, window.Partition, err)
	}

	if !s.stopped {
		// The scan ends without reaching End only when the offsets left hold
		// no records, because retention or compaction removed them.
		covered.End = window.End
	}

	// Writing what this partition produced before reporting its range is what
	// lets the range be trusted: a range reported first and written later
	// would claim messages that a failed write never delivered.
	if failed, ok := s.flushFailed(); ok && failed < covered.End {
		covered.End = failed
	}

	return nil
}

func (s *runState) stop(reason string) {
	if !s.stopped {
		s.stopped = true
		s.out.StoppedReason = reason
	}
}

// visit handles one record and reports whether reading should go on.
func (s *runState) visit(ctx context.Context, record *kgo.Record) bool {
	if !s.window.Contains(record.Timestamp) {
		return true
	}

	s.out.Scanned++

	address := Address{Partition: record.Partition, Offset: record.Offset}

	reader := s.source.Reader()
	value := reader.Decode(ctx, record, serde.Value)
	key := reader.Decode(ctx, record, serde.Key)

	if s.filter != nil {
		arguments, err := script.RecordArguments(s.filter, record, value, key)
		if err == nil {
			var matched bool

			matched, err = s.filter.Call(arguments...)
			if err == nil && !matched {
				return true
			}
		}

		if err != nil {
			if ctx.Err() != nil {
				return s.interrupted()
			}

			s.fail("script: "+err.Error(), address)

			return true
		}
	}

	if s.out.Matched >= s.maxMessages {
		// This message is left for a resumed run, so it is not counted as
		// read either.
		s.out.Scanned--
		s.stop(reasonMaxMessages)

		return false
	}

	s.out.Matched++

	// The script sees the record exactly as search_messages does, so a count
	// taken there matches what is selected here. Only the transform, whose
	// output is written, gets integers beyond 2^53 as exact BigInts.
	exactValue, bigValue := exact(value)
	exactKey, bigKey := exact(key)

	result, err := s.apply(record, exactValue, exactKey)
	if err != nil {
		if ctx.Err() != nil {
			s.out.Matched--

			return s.interrupted()
		}

		s.fail(err.Error(), address)

		return true
	}

	if result != nil && (bigValue || bigKey) {
		if err := result.checkRounded(); err != nil {
			s.fail(err.Error(), address)

			return true
		}
	}

	if result == nil {
		s.out.Skipped++

		return true
	}

	written, err := s.build(ctx, record, exactValue, exactKey, result)
	if err != nil {
		s.fail(err.Error(), address)

		return true
	}

	if s.file != nil {
		line, err := s.fileLine(written.record, written.plain)
		if err != nil {
			s.fail(err.Error(), address)

			return true
		}

		if err := s.file.Write(line); err != nil {
			// A file that cannot be written cannot be trusted to hold the
			// set, so the run stops here rather than saving half of it.
			s.fail(err.Error(), address)
			s.stop(reasonWriteFailed)

			return false
		}
	}

	s.out.Transformed++

	if len(s.out.Examples) < maxExamples {
		s.out.Examples = append(s.out.Examples, Example{
			Before: records.RenderDecoded(record, key, value, s.input.MaxValueBytes),
			After:  s.destination.Reader().Render(ctx, written.record, s.input.MaxValueBytes),
		})
	}

	if !s.input.Confirm {
		return true
	}

	s.pending = append(s.pending, pendingRecord{record: written.record, from: address})

	if len(s.pending) >= flushSize {
		if _, failed := s.flushFailed(); failed {
			// This record was in the batch, so the range ends at the first
			// failure, which scanPartition applies.
			return false
		}
	}

	return true
}

// flushFailed produces the buffered records and returns the source offset of
// the first one that failed, if any did. A batch with any failure stops the
// run: the same refusal would usually meet every later message.
//
// Every buffered record comes from the partition being scanned, because
// scanPartition flushes before it returns, so one offset describes the batch.
func (s *runState) flushFailed() (int64, bool) {
	if s.firstFailed != nil {
		return *s.firstFailed, true
	}

	if len(s.pending) == 0 {
		return 0, false
	}

	batch := make([]*kgo.Record, len(s.pending))
	for index, pending := range s.pending {
		batch[index] = pending.record
	}

	producer, err := s.destination.BulkProducer(s.input.KeepPartition)

	var results kgo.ProduceResults

	if err != nil {
		results = make(kgo.ProduceResults, len(batch))
		for index, record := range batch {
			results[index] = kgo.ProduceResult{Record: record, Err: err}
		}
	} else {
		results = producer.ProduceSync(s.writeCtx, batch...)
	}

	for index, result := range results {
		from := s.pending[index].from

		if result.Err != nil {
			s.out.Transformed--
			s.fail("write: "+result.Err.Error(), from)

			if s.firstFailed == nil || from.Offset < *s.firstFailed {
				offset := from.Offset
				s.firstFailed = &offset
			}

			continue
		}

		s.out.Written++
	}

	s.pending = s.pending[:0]

	if s.firstFailed != nil {
		s.stop(reasonWriteFailed)

		return *s.firstFailed, true
	}

	return 0, false
}

// interrupted handles a script stopped by the timeout. The message it was
// running on was not transformed and did not fail, so it is neither counted
// nor covered: resuming from the reported end must reach it again.
func (s *runState) interrupted() bool {
	s.out.Scanned--
	s.stop(reasonTimeout)

	return false
}

func (s *runState) fail(reason string, address Address) {
	s.out.Failed++

	group := s.errors[reason]
	if group == nil {
		if len(s.errors) >= maxErrorGroups*2 {
			// A transform whose error text varies per message would grow
			// this without bound; the count is still kept.
			s.out.ErrorsTruncated = true

			return
		}

		group = &ErrorGroup{Reason: reason, Examples: []Address{}}
		s.errors[reason] = group
	}

	group.Count++

	if len(group.Examples) < maxErrorExamples {
		group.Examples = append(group.Examples, address)
	}
}

// change is what a transform returned. A nil field was left out and is kept
// from the original.
type change struct {
	value      any
	hasValue   bool
	key        any
	hasKey     bool
	headers    map[string]any
	hasHeaders bool
}

// checkRounded refuses a value or key that lost precision in the transform.
func (c *change) checkRounded() error {
	if c.hasValue {
		if err := checkRounded(c.value, "value"); err != nil {
			return err
		}
	}

	if c.hasKey {
		return checkRounded(c.key, "key")
	}

	return nil
}

// apply runs the transform. A nil change means the message is skipped.
func (s *runState) apply(record *kgo.Record, value, key serde.Decoded) (*change, error) {
	arguments, err := script.RecordArguments(s.transform, record, value, key)
	if err != nil {
		return nil, err
	}

	result, err := s.transform.Evaluate(arguments...)
	if err != nil {
		return nil, fmt.Errorf("transform: %w", err)
	}

	if goja.IsNull(result) || goja.IsUndefined(result) {
		return nil, nil
	}

	object, ok := result.(*goja.Object)
	if !ok || object.ClassName() == "Array" {
		return nil, fmt.Errorf(
			"transform must return an object with any of value, key and headers, or null to skip the message; it returned %s",
			result.String())
	}

	out := &change{}

	for _, field := range object.Keys() {
		item := object.Get(field)

		switch field {
		case "value":
			if goja.IsNull(item) || goja.IsUndefined(item) {
				return nil, fmt.Errorf(
					"transform returned a null value, which would write a tombstone and delete the key on a compacted topic; return null instead to skip the message")
			}

			out.value, out.hasValue = item.Export(), true

		case "key":
			out.hasKey = true

			if !goja.IsNull(item) && !goja.IsUndefined(item) {
				out.key = item.Export()
			}

		case "headers":
			headers, ok := item.Export().(map[string]any)
			if !ok {
				return nil, fmt.Errorf("transform returned headers that are not an object of name to string")
			}

			out.headers, out.hasHeaders = headers, true

		default:
			return nil, fmt.Errorf(
				"transform returned unknown field %q: only value, key and headers are written, so a misspelt field would leave the original unchanged",
				field)
		}
	}

	return out, nil
}

// built is the record to write, and whether each part was written as given
// rather than encoded to a schema or configured format.
type built struct {
	record *kgo.Record
	plain  bool
}

// build makes the record to write from the original and the change.
func (s *runState) build(
	ctx context.Context,
	original *kgo.Record,
	value, key serde.Decoded,
	result *change,
) (*built, error) {

	record := &kgo.Record{
		Topic: s.destTopic,
		Key:   original.Key,
		Value: original.Value,
	}

	if s.input.KeepPartition {
		record.Partition = original.Partition
	}

	plainValue, plainKey := value.SchemaID == 0, key.SchemaID == 0

	if result.hasValue {
		data, plain, err := s.encode(ctx, serde.Value, result.value, value, s.input.ValueSchema)
		if err != nil {
			return nil, err
		}

		record.Value, plainValue = data, plain
	} else if s.input.ValueSchema != nil {
		return nil, fmt.Errorf("value_schema was set but the transform did not return a value to encode")
	}

	if result.hasKey {
		if result.key == nil {
			record.Key, plainKey = nil, true
		} else {
			data, plain, err := s.encode(ctx, serde.Key, result.key, key, s.input.KeySchema)
			if err != nil {
				return nil, err
			}

			record.Key, plainKey = data, plain
		}
	}

	headers := original.Headers

	if result.hasHeaders {
		headers = make([]kgo.RecordHeader, 0, len(result.headers))

		names := make([]string, 0, len(result.headers))
		for name := range result.headers {
			names = append(names, name)
		}

		// Go map order is random, and headers that change order between runs
		// make two identical repairs produce different records.
		sort.Strings(names)

		for _, name := range names {
			switch typed := result.headers[name].(type) {
			case string:
				headers = append(headers, kgo.RecordHeader{Key: name, Value: []byte(typed)})
			case []byte:
				headers = append(headers, kgo.RecordHeader{Key: name, Value: typed})
			case nil:
				// null or undefined removes the header, so a spread of the
				// originals can drop one: { ...headers, 'error-reason': null }.
			case int64, float64, bool, *big.Int:
				headers = append(headers, kgo.RecordHeader{Key: name, Value: []byte(fmt.Sprint(typed))})
			default:
				return nil, fmt.Errorf("transform returned header %q that is not a string", name)
			}
		}
	}

	record.Headers = withProvenance(headers, s.provenance, original)

	return &built{record: record, plain: plainValue && plainKey}, nil
}

// encode turns what the transform returned for one part into bytes.
//
// The first that applies wins: an explicit schema; the schema the original was
// read with, so an Avro topic stays Avro; the destination topic's configured
// format; and otherwise the value as given, a string as its text and anything
// else as JSON. plain reports the last case.
func (s *runState) encode(
	ctx context.Context,
	part serde.Part,
	returned any,
	original serde.Decoded,
	ref *SchemaRef,
) ([]byte, bool, error) {

	codec := s.destination.Codec()

	if ref != nil {
		document, err := jsonDocument(returned)
		if err != nil {
			return nil, false, fmt.Errorf("%s: %w", part, err)
		}

		data, _, err := codec.Encode(ctx, s.destTopic, part, document, encodeTarget(ref))
		if err != nil {
			return nil, false, fmt.Errorf("encode %s: %w", part, err)
		}

		return data, false, nil
	}

	if original.SchemaID != 0 {
		if !serde.SameRegistry(s.source.Codec(), codec) {
			return nil, false, fmt.Errorf(
				"the %s was read with schema id %d, which the destination cluster's schema registry may not have or may map to another schema; set %s_schema to choose the schema to encode to",
				part, original.SchemaID, part)
		}

		document, err := jsonDocument(returned)
		if err != nil {
			return nil, false, fmt.Errorf("%s: %w", part, err)
		}

		data, _, err := codec.Encode(ctx, s.destTopic, part, document, serde.Target{
			ID:          original.SchemaID,
			MessageType: original.MessageType,
		})
		if err != nil {
			return nil, false, fmt.Errorf("encode %s to schema id %d it was read with: %w", part, original.SchemaID, err)
		}

		return data, false, nil
	}

	text, err := textOf(returned)
	if err != nil {
		return nil, false, fmt.Errorf("%s: %w", part, err)
	}

	data, _, configured, err := codec.EncodeConfigured(s.destTopic, part, text)
	if err != nil {
		return nil, false, err
	}

	if configured {
		return data, false, nil
	}

	if raw, ok := returned.([]byte); ok {
		return raw, true, nil
	}

	return []byte(text), true, nil
}

// textOf renders a returned part as text: a string as itself, bytes as
// base64, anything else as JSON.
func textOf(returned any) (string, error) {
	switch typed := returned.(type) {
	case string:
		return typed, nil
	case []byte:
		return base64.StdEncoding.EncodeToString(typed), nil
	}

	return jsonDocument(returned)
}

func jsonDocument(returned any) (string, error) {
	encoded, err := json.Marshal(returned)
	if err != nil {
		return "", fmt.Errorf("the transform returned something that is not JSON: %w", err)
	}

	return string(encoded), nil
}

// fileLine renders a record as a produce_message item.
//
// Bytes that were encoded, or are not text, are saved as base64, so
// produce_message writes them exactly instead of encoding them a second time.
func (s *runState) fileLine(record *kgo.Record, plain bool) (fileItem, error) {
	item := fileItem{Topic: record.Topic}

	if s.input.DestinationCluster != "" && s.input.DestinationCluster != s.source.Config().Name {
		item.DestinationCluster = s.input.DestinationCluster
	}

	if s.input.KeepPartition {
		partition := record.Partition
		item.Partition = &partition
	}

	if plain && utf8.Valid(record.Value) && utf8.Valid(record.Key) {
		item.Value = string(record.Value)
		item.Key = string(record.Key)
	} else {
		item.Encoding = "base64"
		item.Value = base64.StdEncoding.EncodeToString(record.Value)

		if record.Key != nil {
			item.Key = base64.StdEncoding.EncodeToString(record.Key)
		}
	}

	if item.Value == "" {
		return fileItem{}, fmt.Errorf("output_file: an empty value cannot be saved, because produce_message refuses one")
	}

	if len(record.Headers) > 0 {
		item.Headers = make(map[string]string, len(record.Headers))

		for _, header := range record.Headers {
			if !utf8.Valid(header.Value) {
				return fileItem{}, fmt.Errorf(
					"output_file: header %q is not valid UTF-8, and produce_message headers are text", header.Key)
			}

			item.Headers[header.Key] = string(header.Value)
		}
	}

	return item, nil
}

// topicPartitions returns how many partitions a destination topic has, and
// fails when it does not exist, so a mistyped name is not auto-created.
func topicPartitions(ctx context.Context, kafka *kafkaclient.Client, topic string) (int, error) {
	details, err := kafka.Admin().ListTopics(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("list topic %q: %w", topic, err)
	}

	detail, ok := details[topic]
	if !ok || detail.Err != nil {
		return 0, fmt.Errorf(
			"destination topic %q does not exist: create it first, so a mistyped name cannot scatter messages into a topic nobody meant to make",
			topic)
	}

	return len(detail.Partitions), nil
}
