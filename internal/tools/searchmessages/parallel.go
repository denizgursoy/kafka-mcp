package searchmessages

import (
	"context"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
)

// chunkResult is what one reader found in one chunk of a split partition.
type chunkResult struct {
	found []records.Message
	// seen and last describe how far a chunk cut short got, so the report
	// claims only offsets that were actually read.
	seen bool
	last int64
	// done is true when the whole chunk was read.
	done bool
}

// scanParallel reads one partition with several concurrent readers.
//
// The partition is cut into chunks in the order the caller asked for — newest
// chunk first for a newest-first search — and readers take chunks from that
// queue as they become free. Readers finish out of order, so which matches
// count as the newest cannot be decided by whoever finished first: a
// max_matches stop is only declared once the chunks at the head of the queue
// are all complete and hold enough matches between them. Everything a reader
// found further back is older by construction and is dropped.
//
// Each reader has its own session and its own script runtime, because a goja
// runtime cannot be shared between goroutines.
func (s *scanState) scanParallel(
	parent context.Context,
	reader *records.Reader,
	topic string,
	window records.Range,
) error {
	queue := chunks([]records.Range{window}, s.options.newestFirst)
	if len(queue) == 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	results := make([]chunkResult, len(queue))
	next := 0
	var firstErr error

	// take hands out the next chunk, or -1 once the queue is empty or the
	// search has stopped.
	take := func() int {
		s.mu.Lock()
		defer s.mu.Unlock()

		if next >= len(queue) || s.out.StoppedReason != reasonExhausted || firstErr != nil {
			return -1
		}
		next++

		return next - 1
	}

	// complete records a finished chunk and decides whether the head of the
	// queue now holds enough matches to stop.
	complete := func(index int, result chunkResult) {
		s.mu.Lock()
		defer s.mu.Unlock()

		results[index] = result
		if !result.done || !s.collecting {
			return
		}

		matched := 0
		for i := range results {
			if !results[i].done {
				break
			}
			matched += len(results[i].found)
		}

		if matched >= s.options.maxMatches && s.out.StoppedReason == reasonExhausted {
			s.out.StoppedReason = reasonMaxMatches
			cancel()
		}
	}

	fail := func(err error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		if firstErr == nil {
			firstErr = err
		}
		cancel()
	}

	readers := min(s.options.parallelism, len(queue))
	done := make(chan struct{}, readers)

	for range readers {
		go func() {
			defer func() { done <- struct{}{} }()

			compiled, err := s.options.newScripts()
			if err != nil {
				fail(err)
				return
			}
			stop := compiled.guard(ctx)
			defer stop()
			defer compiled.close()

			session, err := reader.Session(topic)
			if err != nil {
				fail(fmt.Errorf("search %s: %w", topic, err))
				return
			}
			defer session.Close()

			for index := take(); index >= 0; index = take() {
				chunk := queue[index]
				result := chunkResult{found: make([]records.Message, 0)}

				var visitErr error
				cut := false
				err := session.Scan(ctx, []records.Range{chunk}, func(record *kgo.Record) bool {
					result.seen, result.last = true, record.Offset
					more, err := s.visit(ctx, reader, record, compiled, &result.found)
					if err != nil {
						visitErr = err
						return false
					}
					cut = !more
					return more
				})

				switch {
				case visitErr != nil:
					fail(visitErr)
					return
				case err != nil && ctx.Err() == nil:
					fail(fmt.Errorf("search %s: %w", topic, err))
					return
				}

				// Complete only when the scan ran to the chunk's end: a stop
				// for max_messages_scanned or a cancellation leaves it partial.
				result.done = err == nil && !cut
				complete(index, result)
			}
		}()
	}

	for range readers {
		<-done
	}

	if firstErr != nil {
		return firstErr
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if parent.Err() != nil && s.out.StoppedReason == reasonExhausted {
		s.out.StoppedReason = reasonTimeout
	}

	s.mergeParallel(queue, results)

	return nil
}

// mergeParallel turns the per-chunk results into the search's matches and
// scanned ranges. The caller holds s.mu.
func (s *scanState) mergeParallel(queue []records.Range, results []chunkResult) {
	if s.collecting {
		// Only the head of the queue up to the first unfinished chunk is
		// used. Chunks are ordered as the caller asked, so a match beyond a
		// gap could rank ahead of one in the gap that was never read. On a
		// max_matches stop the head already holds enough, and the partial
		// chunk after it is older and trimmed by keep.
		found := make([]records.Message, 0)
		for i := range results {
			found = append(found, results[i].found...)
			if !results[i].done {
				break
			}
		}
		s.out.Matches = keep(append(s.out.Matches, found...), s.options)
	}

	// Chunks finish out of order, so what was read need not be contiguous.
	// Each covered run is reported separately rather than as one span that
	// would claim the gaps were read.
	covered := make([]ScannedRange, 0, len(results))
	for i, result := range results {
		chunk := queue[i]
		switch {
		case result.done:
			covered = append(covered, ScannedRange{Partition: chunk.Partition, Start: chunk.Start, End: chunk.End})
		case result.seen:
			covered = append(covered, ScannedRange{Partition: chunk.Partition, Start: chunk.Start, End: result.last + 1})
		}
	}

	sort.Slice(covered, func(i, j int) bool { return covered[i].Start < covered[j].Start })

	merged := make([]ScannedRange, 0, len(covered))
	for _, rng := range covered {
		last := len(merged) - 1
		if last >= 0 && merged[last].End >= rng.Start {
			merged[last].End = max(merged[last].End, rng.End)
			continue
		}
		merged = append(merged, rng)
	}

	// visit tracked a single min..max span for the partition, which would
	// claim the gaps were read. The covered runs replace it.
	delete(s.scanned, queue[0].Partition)
	s.split = merged
}
