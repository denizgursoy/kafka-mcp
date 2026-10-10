package consumerlag

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
	"github.com/denizgursoy/kafka-mcp/internal/domain/topicconfig"
)

// retentionRiskShare is how much of the topic's retention the oldest
// unconsumed message may have used before the backlog is flagged. Past it, a
// consumer that does not speed up loses messages to retention rather than
// processing them.
const retentionRiskShare = 0.5

// markExpired fills each partition's start offset and flags commits that
// retention has already deleted.
func markExpired(measured *GroupLag, topic string, starts kadm.ListedOffsets) {
	expired := make([]int32, 0)

	for i := range measured.Partitions {
		partition := &measured.Partitions[i]

		start, ok := starts.Lookup(topic, partition.Partition)
		if !ok || start.Err != nil {
			continue
		}

		partition.StartOffset = start.Offset

		// A negative commit means none exists for this partition, which is a
		// reset waiting to happen for a different reason and not an expiry.
		if partition.Error == "" && partition.CommittedOffset >= 0 && partition.CommittedOffset < start.Offset {
			partition.OffsetExpired = true
			expired = append(expired, partition.Partition)
		}
	}

	if len(expired) > 0 {
		measured.Warnings = append(measured.Warnings, fmt.Sprintf(
			"the committed offset of partitions %v is below the log start: retention deleted those messages, so the group's position no longer exists and the consumer will reset by auto.offset.reset (earliest or latest) instead of resuming. Messages between the commit and the start are gone",
			expired))
	}
}

// measureAge reads the next unconsumed message of every lagging partition to
// report how old each group's backlog is, and compares it with retention.
func measureAge(ctx context.Context, admin *kadm.Client, reader *records.Reader, out *Output) error {
	retention, err := retentionMs(ctx, admin, out.Topic)
	if err != nil {
		return err
	}
	out.RetentionMs = &retention

	// One read per distinct (partition, offset), shared by every group that
	// committed the same position.
	wanted := make(map[int32]map[int64]struct{})
	for _, group := range out.Groups {
		for _, partition := range group.Partitions {
			if partition.Error != "" || partition.Lag <= 0 || partition.OffsetExpired {
				continue
			}
			if wanted[partition.Partition] == nil {
				wanted[partition.Partition] = make(map[int64]struct{})
			}
			wanted[partition.Partition][partition.CommittedOffset] = struct{}{}
		}
	}

	timestamps := make(map[int32]map[int64]time.Time)
	for partition, offsets := range wanted {
		timestamps[partition] = make(map[int64]time.Time)
		for offset := range offsets {
			at, ok, err := timestampAt(ctx, reader, out.Topic, partition, offset)
			if err != nil {
				return err
			}
			if ok {
				timestamps[partition][offset] = at
			}
		}
	}

	now := time.Now()

	for g := range out.Groups {
		group := &out.Groups[g]

		for p := range group.Partitions {
			partition := &group.Partitions[p]

			at, ok := timestamps[partition.Partition][partition.CommittedOffset]
			if !ok {
				continue
			}

			age := round(now.Sub(at).Seconds(), 3)
			partition.CommittedTimestamp = &at
			partition.LagSeconds = &age

			if group.OldestUnconsumedAt == nil || at.Before(*group.OldestUnconsumedAt) {
				oldest := at
				group.OldestUnconsumedAt = &oldest
			}
		}

		if group.OldestUnconsumedAt == nil || retention <= 0 {
			continue
		}

		used := now.Sub(*group.OldestUnconsumedAt)
		if used.Seconds() >= retentionRiskShare*float64(retention)/1000 {
			group.RetentionRisk = true
			group.Warnings = append(group.Warnings, fmt.Sprintf(
				"the oldest unconsumed message is %s old and the topic keeps messages for %s: unless the group catches up, retention deletes the backlog before it is consumed",
				humanDuration(used.Seconds()), humanDuration(float64(retention)/1000)))
		}
	}

	return nil
}

// timestampAt reads one record's timestamp. It reports false when the offset
// holds no record, which happens when it is the end of the partition or a
// transaction marker sits there.
func timestampAt(
	ctx context.Context,
	reader *records.Reader,
	topic string,
	partition int32,
	offset int64,
) (time.Time, bool, error) {
	var found *kgo.Record

	// A short window rather than one offset: a commit can point at a control
	// record, and the next data record is then the next message to consume.
	err := reader.Scan(ctx, topic, []records.Range{{Partition: partition, Start: offset, End: offset + 10}},
		func(record *kgo.Record) bool {
			found = record
			return false
		})
	if err != nil {
		return time.Time{}, false, fmt.Errorf("read %s partition %d offset %d: %w", topic, partition, offset, err)
	}

	if found == nil {
		return time.Time{}, false, nil
	}

	return found.Timestamp.UTC(), true, nil
}

func retentionMs(ctx context.Context, admin *kadm.Client, topic string) (int64, error) {
	entries, err := topicconfig.For(ctx, admin, topic)
	if err != nil {
		return 0, err
	}

	for _, entry := range entries {
		if entry.Key != "retention.ms" {
			continue
		}

		value, err := strconv.ParseInt(entry.Value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("topic %q has a retention.ms that is not a number: %q", topic, entry.Value)
		}

		return value, nil
	}

	return -1, nil
}
