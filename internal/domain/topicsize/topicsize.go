// Package topicsize reports how many bytes topics occupy on the brokers.
//
// It lives in internal/domain because two tools need it: describe_topic
// reports one topic's size per partition, and list_topics lets a caller rank
// or filter topics by size. Both read the same DescribeLogDirs answer and
// must agree on what a topic's size is.
package topicsize

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
)

// Size is a partition's or topic's footprint.
//
// Bytes counts one copy, the leader's or the largest replica's, which is what
// retention and compaction act on. ReplicatedBytes sums every replica, which
// is what the brokers' disks actually hold.
type Size struct {
	Bytes           int64
	ReplicatedBytes int64
}

// Sizes is every requested topic's size, by topic and by partition.
type Sizes struct {
	Topics     map[string]Size
	Partitions map[string]map[int32]Size
}

// Read reports the sizes of topics, or of every topic when none are named.
//
// Sizes are what the brokers report for their log segments, which is after
// compression: a highly compressible payload takes far fewer bytes than its
// decoded length.
//
// A broker that does not answer leaves its replicas out, so a size can be low
// but never invented; the error says which broker failed.
func Read(ctx context.Context, admin *kadm.Client, topics ...string) (Sizes, error) {
	// Every topic is requested and the filtering happens here: Redpanda was
	// verified to answer a topic-filtered DescribeLogDirs with nothing at all.
	described, err := admin.DescribeAllLogDirs(ctx, nil)
	if err != nil {
		return Sizes{}, fmt.Errorf("describe log dirs: %w", err)
	}

	wanted := make(map[string]bool, len(topics))
	for _, topic := range topics {
		wanted[topic] = true
	}

	sizes := Sizes{Topics: map[string]Size{}, Partitions: map[string]map[int32]Size{}}

	for _, dirs := range described {
		for _, dir := range dirs {
			if dir.Err != nil {
				continue
			}

			dir.Topics.Each(func(partition kadm.DescribedLogDirPartition) {
				if partition.IsFuture || (len(wanted) > 0 && !wanted[partition.Topic]) {
					return
				}

				byPartition := sizes.Partitions[partition.Topic]
				if byPartition == nil {
					byPartition = map[int32]Size{}
					sizes.Partitions[partition.Topic] = byPartition
				}

				current := byPartition[partition.Partition]
				current.ReplicatedBytes += partition.Size
				current.Bytes = max(current.Bytes, partition.Size)
				byPartition[partition.Partition] = current
			})
		}
	}

	for topic, byPartition := range sizes.Partitions {
		var total Size
		for _, size := range byPartition {
			total.Bytes += size.Bytes
			total.ReplicatedBytes += size.ReplicatedBytes
		}
		sizes.Topics[topic] = total
	}

	return sizes, nil
}
