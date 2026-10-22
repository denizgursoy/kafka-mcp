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
	"github.com/denizgursoy/kafka-mcp/internal/domain/outputfile"
)

// flushSize is how many records are produced together when sending a file.
// Producing them one at a time would cost a round trip each, and a file of a
// million repaired messages would take hours.
const flushSize = 1000

// runFile writes the items held in a file, one JSON item per line.
//
// The file is read twice. The first pass validates every line, so the preview
// reports every line that would fail before anything is written, and a file of
// any size never has to be held in memory. With confirm, the second pass writes
// the lines that were valid, in file order.
func runFile(
	ctx context.Context,
	clusters *kafkaclient.Registry,
	own string,
	dir string,
	name string,
	confirm bool,
	client string,
) (BatchOutput, error) {

	file, err := outputfile.Open(dir, "input_file", name)
	if err != nil {
		return BatchOutput{}, err
	}
	defer file.Close()

	out := &FileOutput{
		Path:     file.Name(),
		Examples: []Output{},
		Errors:   []LineError{},
	}
	topics := &topicCache{}
	invalid := make(map[int]bool)

	err = eachLine(file, func(line int, item Item, parseErr error) error {
		out.Lines++

		if parseErr == nil {
			var ready *prepared

			ready, parseErr = prepare(ctx, clusters, own, item, client, topics)
			if parseErr == nil {
				out.Valid++

				if len(out.Examples) < maxFileExamples {
					out.Examples = append(out.Examples, ready.out)
				}

				return nil
			}
		}

		// A read-only destination refuses every line for the same reason, so
		// it is a refusal of the call rather than a million line errors.
		var refused readOnlyError
		if errors.As(parseErr, &refused) {
			return parseErr
		}

		out.Invalid++
		invalid[line] = true
		out.addError(line, parseErr)

		return nil
	})
	if err != nil {
		return BatchOutput{}, err
	}

	if out.Lines == 0 {
		return BatchOutput{}, fmt.Errorf("input_file %q holds no messages", name)
	}

	if !confirm || out.Valid == 0 {
		out.Note = fmt.Sprintf(
			"nothing was written. %d of %d lines are valid. Call again with confirm true to produce them.",
			out.Valid, out.Lines)

		return BatchOutput{File: out}, nil
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return BatchOutput{}, fmt.Errorf("rewind input_file: %w", err)
	}

	sender := &fileSender{ctx: ctx, out: out}

	err = eachLine(file, func(line int, item Item, parseErr error) error {
		if invalid[line] || parseErr != nil {
			return nil
		}

		ready, err := prepare(ctx, clusters, own, item, client, topics)
		if err != nil {
			// The file changed between the passes, or the topic did.
			out.Failed++
			out.addError(line, err)

			return nil
		}

		return sender.add(line, ready)
	})
	if err == nil {
		err = sender.flush()
	}

	out.Applied = out.Written > 0
	out.Note = fmt.Sprintf(
		"produced %d of %d lines. Writing is not atomic: messages written before a failure stay, and a produced message cannot be deleted until retention removes it.",
		out.Written, out.Lines)

	if err != nil {
		// Context cancellation or a broken file mid-send: what was written
		// stays, and the caller must see how far it got.
		out.Note = fmt.Sprintf("stopped after producing %d of %d lines: %v. Messages already written stay.",
			out.Written, out.Lines, err)
	}

	return BatchOutput{File: out}, nil
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
			return fmt.Errorf("input_file has more than %d lines", maxFileLines)
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
			return fmt.Errorf("input_file line %d is longer than %d bytes", line+1, maxLineBytes)
		}

		return fmt.Errorf("read input_file: %w", err)
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
