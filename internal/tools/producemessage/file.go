package producemessage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
)

// flushSize is how many records are produced together when sending a file.
// Producing them one at a time would cost a round trip each, and a file of a
// million repaired messages would take hours.
const flushSize = 1000

// runUpload reads items from an uploaded file, one JSON item per line, and
// with confirm produces the valid ones as it reads.
//
// The file is never stored, so it is read once: validating every line before
// writing any, as a preview does, would need it twice. A send therefore
// writes each valid line in file order and reports the invalid ones by line
// number, exactly as a confirmed file always has; a preview upload first
// shows what a send would do.
func runUpload(
	ctx context.Context,
	clusters *kafkaclient.Registry,
	own string,
	r io.Reader,
	confirm bool,
	client string,
) (*FileOutput, error) {

	out := &FileOutput{
		Examples: []Output{},
		Errors:   []LineError{},
	}
	topics := &topicCache{}
	sender := &fileSender{ctx: ctx, out: out}

	err := eachLine(r, func(line int, item Item, parseErr error) error {
		out.Lines++

		var ready *prepared
		if parseErr == nil {
			ready, parseErr = prepare(ctx, clusters, own, item, client, topics)
		}

		if parseErr != nil {
			// A read-only destination refuses every line for the same reason,
			// so it is a refusal of the call rather than a million line errors.
			var refused readOnlyError
			if errors.As(parseErr, &refused) {
				return parseErr
			}

			out.Invalid++
			out.addError(line, parseErr)

			return nil
		}

		out.Valid++
		if len(out.Examples) < maxFileExamples {
			example := ready.out
			// The summary's own note says what was written; the per-message
			// preview note would claim nothing was.
			if confirm {
				example.Note = ""
			}
			out.Examples = append(out.Examples, example)
		}

		if !confirm {
			return nil
		}

		return sender.add(line, ready)
	})

	var refused readOnlyError
	if errors.As(err, &refused) {
		return nil, err
	}

	if err == nil && confirm {
		err = sender.flush()
	}

	if err == nil && out.Lines == 0 {
		return nil, fmt.Errorf("the uploaded file holds no messages")
	}

	out.Applied = out.Written > 0

	switch {
	case err != nil:
		// Context cancellation or a broken upload mid-send: what was written
		// stays, and the caller must see how far it got.
		out.Note = fmt.Sprintf("stopped after producing %d of %d lines read: %v. Messages already written stay.",
			out.Written, out.Lines, err)
	case !confirm:
		out.Note = fmt.Sprintf(
			"nothing was written. %d of %d lines are valid. Call produce_message again with upload and confirm true for a link that writes them.",
			out.Valid, out.Lines)
	default:
		out.Note = fmt.Sprintf(
			"produced %d of %d lines. Writing is not atomic: messages written before a failure stay, and a produced message cannot be deleted until retention removes it.",
			out.Written, out.Lines)
	}

	return out, nil
}

// eachLine calls fn with every non-blank line of r decoded as an Item. A line
// that does not decode is passed with its error, so the caller reports it by
// line number rather than stopping at the first bad line.
func eachLine(r io.Reader, fn func(line int, item Item, err error) error) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineBytes)

	line := 0

	for scanner.Scan() {
		line++

		text := bytes.TrimSpace(scanner.Bytes())
		if len(text) == 0 {
			continue
		}

		if line > maxFileLines {
			return fmt.Errorf("the file has more than %d lines", maxFileLines)
		}

		var item Item

		decoder := json.NewDecoder(bytes.NewReader(text))
		// A misspelt field would otherwise be dropped silently, and the line
		// written without what the caller meant it to carry.
		decoder.DisallowUnknownFields()

		err := decoder.Decode(&item)
		if err != nil {
			err = fmt.Errorf("not a produce_message item: %w", err)
		}

		if err := fn(line, item, err); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return fmt.Errorf("line %d is longer than %d bytes", line+1, maxLineBytes)
		}

		return fmt.Errorf("read the file: %w", err)
	}

	return nil
}

func (o *FileOutput) addError(line int, err error) {
	if len(o.Errors) >= maxFileErrors {
		o.ErrorsTruncated = true

		return
	}

	o.Errors = append(o.Errors, LineError{Line: line, Error: err.Error()})
}

// fileSender produces prepared records in groups, keeping file order.
//
// A group only ever holds records for one producer: the shared one, which
// partitions by key, or the manual one, which honours an explicit partition.
// Switching producer flushes first, so two lines for one partition are never
// sent by different producers at once and reordered.
type fileSender struct {
	ctx     context.Context
	out     *FileOutput
	pending []*prepared
	lines   []int
}

func (s *fileSender) add(line int, ready *prepared) error {
	if len(s.pending) > 0 {
		first := s.pending[0]
		if first.destination != ready.destination || first.manual != ready.manual {
			if err := s.flush(); err != nil {
				return err
			}
		}
	}

	s.pending = append(s.pending, ready)
	s.lines = append(s.lines, line)

	if len(s.pending) >= flushSize {
		return s.flush()
	}

	return nil
}

func (s *fileSender) flush() error {
	if len(s.pending) == 0 {
		return nil
	}

	defer func() {
		s.pending = s.pending[:0]
		s.lines = s.lines[:0]
	}()

	if err := s.ctx.Err(); err != nil {
		return err
	}

	first := s.pending[0]

	producer, err := first.destination.BulkProducer(first.manual)
	if err != nil {
		return err
	}

	batch := make([]*kgo.Record, len(s.pending))
	for index, ready := range s.pending {
		batch[index] = ready.record
	}

	results := producer.ProduceSync(s.ctx, batch...)

	for index, result := range results {
		if result.Err != nil {
			s.out.Failed++
			s.out.addError(s.lines[index], produceError(result.Record.Topic, result.Err))

			continue
		}

		s.out.Written++
	}

	return nil
}
