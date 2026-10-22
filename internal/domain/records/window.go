package records

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// Window narrows a topic to the part a scan should read. Every field is
// optional; an empty Window is the whole of every partition.
type Window struct {
	Partitions    []int32
	FromOffset    *int64
	ToOffset      *int64
	FromTimestamp *time.Time
	ToTimestamp   *time.Time
}

// Contains reports whether a record's own timestamp is inside the window's
// time bounds: at or after FromTimestamp and before ToTimestamp.
//
// ResolveWindows can only turn the bounds into offsets, which is exact only
// when timestamps grow with offsets. Producers set timestamps themselves, so
// near either bound records from outside the range can sit inside the offsets;
// a scan skips them with this.
func (w Window) Contains(timestamp time.Time) bool {
	if w.FromTimestamp != nil && timestamp.Before(*w.FromTimestamp) {
		return false
	}

	return w.ToTimestamp == nil || timestamp.Before(*w.ToTimestamp)
}

// ResolveWindows turns the requested partitions, offsets and timestamps into
// the concrete offset range to scan in each partition, sorted by partition.
//
// The end of each range is the partition's end offset when this is called, so
// records produced afterwards are not part of the scan. A tool that writes back
// into the topic it reads relies on that to never read its own output.
func ResolveWindows(
	ctx context.Context,
	admin *kadm.Client,
	topic string,
	input Window,
) ([]Range, error) {

	details, err := admin.ListTopics(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list topic %q: %w", topic, err)
	}

	detail, ok := details[topic]
	if !ok || errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
		// The broker reports an absent topic either by omitting it or by
		// returning it with UNKNOWN_TOPIC_OR_PARTITION, and both mean the same
		// thing to the caller.
		return nil, fmt.Errorf("topic %q does not exist", topic)
	}

	if detail.Err != nil {
		return nil, fmt.Errorf("topic %q: %w", topic, detail.Err)
	}

	wanted := make(map[int32]bool, len(input.Partitions))

	for _, partition := range input.Partitions {
		if _, ok := detail.Partitions[partition]; !ok {
			return nil, fmt.Errorf(
				"topic %q has no partition %d", topic, partition)
		}

		wanted[partition] = true
	}

	starts, err := admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list start offsets for %q: %w", topic, err)
	}

	ends, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets for %q: %w", topic, err)
	}

	fromTime, err := offsetsAt(ctx, admin, topic, input.FromTimestamp)
	if err != nil {
		return nil, err
	}

	toTime, err := offsetsAt(ctx, admin, topic, input.ToTimestamp)
	if err != nil {
		return nil, err
	}

	windows := make([]Range, 0, len(detail.Partitions))

	for id := range detail.Partitions {
		if len(wanted) > 0 && !wanted[id] {
			continue
		}

		start, hasStart := starts.Lookup(topic, id)
		end, hasEnd := ends.Lookup(topic, id)

		if !hasStart || !hasEnd {
			continue
		}

		window := Range{
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
