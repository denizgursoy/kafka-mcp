// Package consumerlag implements the consumer_lag MCP tool.
package consumerlag

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
)

// Input is the argument set accepted by the consumer_lag tool.
type Input struct {
	Topic           string `json:"topic" jsonschema:"Topic to measure lag on. Matched exactly and case-sensitively."`
	Group           string `json:"group,omitempty" jsonschema:"Optional consumer group. Defaults to every group that consumes or holds committed offsets for the topic."`
	SampleSeconds   int    `json:"sample_seconds,omitempty" jsonschema:"Optional number of seconds to sample the consume rate over. Defaults to 5. The call blocks for this long, because Kafka stores no history of past commits and the rate can only be measured by comparing two readings."`
	SkipConsumeRate bool   `json:"skip_consume_rate,omitempty" jsonschema:"Optional. When true, return immediately without sampling the consume rate. No completion estimate can be produced, because there is no rate to divide the lag by."`
}

// PartitionLag is the lag of one partition within a group.
type PartitionLag struct {
	Partition       int32  `json:"partition"`
	CommittedOffset int64  `json:"committed_offset"`
	EndOffset       int64  `json:"end_offset"`
	Lag             int64  `json:"lag"`
	Error           string `json:"error,omitempty"`
}

// GroupLag is everything measured about one consumer group.
type GroupLag struct {
	Group      string         `json:"group"`
	State      string         `json:"state"`
	Members    int            `json:"members"`
	Lag        int64          `json:"lag"`
	Partitions []PartitionLag `json:"partitions"`

	ConsumeRate *SampledRate `json:"consume_rate,omitempty"`

	// DrainPerSecond is how fast the lag is shrinking: the consume rate minus
	// the produce rate. A negative value means the lag is growing.
	DrainPerSecond *float64 `json:"drain_per_second,omitempty"`
	GrowingPerMin  *float64 `json:"growing_by_per_minute,omitempty"`

	ETASeconds *float64   `json:"eta_seconds,omitempty"`
	ETAHuman   string     `json:"eta_human,omitempty"`
	ETAAt      *time.Time `json:"eta_at,omitempty"`

	// Status says what the estimate means, including the cases where no
	// estimate is possible.
	Status string `json:"status"`
}

// Output is the result returned by the consumer_lag tool.
type Output struct {
	Topic       string       `json:"topic"`
	TotalLag    int64        `json:"total_lag"`
	ProduceRate *ProduceRate `json:"produce_rate,omitempty"`
	Groups      []GroupLag   `json:"groups"`
}

// Statuses reported for a group.
const (
	statusCaughtUp    = "caught_up"
	statusDraining    = "draining"
	statusGrowing     = "growing"
	statusStalled     = "stalled"
	statusNoConsumers = "no_active_consumers"
	statusNotMeasured = "not_measured"
)

const defaultSampleSeconds = 5

const description = `
Measure how far behind a topic's consumers are, how fast messages are being
produced and consumed, and when the backlog will clear.

Kafka stores no history of consumption, so the two rates are measured very
differently, and the difference matters when reading the result:

- "produce_rate" is measured from message timestamps over real windows: the
  last second, the last minute and the last hour. It is historical fact.
- "consume_rate" is sampled by reading the group's committed offset, waiting
  "sample_seconds", and reading it again. It is a short extrapolation, not a
  historical average, and the call blocks while it is taken. Set
  "skip_consume_rate" to avoid the wait, at the cost of any estimate.

The backlog does not drain at the consume rate. It drains at the consume rate
minus the produce rate, because producers keep adding to it. "status" says what
the numbers mean:

  caught_up            there is no lag
  draining             lag is shrinking, and eta_seconds says when it clears
  growing              consumers cannot keep up, so the lag will never clear;
                       growing_by_per_minute says how fast it is getting worse
  stalled              members are present but nothing is being consumed
  no_active_consumers  the group has no members, so nothing will drain it
  not_measured         the consume rate was not sampled, so no estimate exists

An estimate is reported only when the lag is genuinely shrinking. In every
other case the reason is named instead, because a completion time that will
never arrive is worse than no completion time at all.
`

// Register adds the consumer_lag tool to the MCP server.
func Register(server *mcp.Server, admin *kadm.Client) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "consumer_lag",
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, Output, error) {

			out, err := Run(ctx, admin, input)
			if err != nil {
				return nil, Output{}, fmt.Errorf("consumer lag: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run measures lag, throughput and the time remaining for a topic's consumers.
func Run(
	ctx context.Context,
	admin *kadm.Client,
	input Input,
) (Output, error) {

	if input.Topic == "" {
		return Output{}, fmt.Errorf("topic is required")
	}

	if input.SampleSeconds < 0 {
		return Output{}, fmt.Errorf(
			"sample_seconds must not be negative, got %d", input.SampleSeconds)
	}

	details, err := admin.ListTopics(ctx, input.Topic)
	if err != nil {
		return Output{}, fmt.Errorf("list topic %q: %w", input.Topic, err)
	}

	detail, ok := details[input.Topic]
	if !ok {
		return Output{}, fmt.Errorf("topic %q does not exist", input.Topic)
	}

	if detail.Err != nil {
		return Output{}, fmt.Errorf("topic %q: %w", input.Topic, detail.Err)
	}

	groups, err := resolveGroups(ctx, admin, input)
	if err != nil {
		return Output{}, err
	}

	out := Output{Topic: input.Topic, Groups: []GroupLag{}}

	produce, err := measureProduceRate(ctx, admin, input.Topic)
	if err != nil {
		return Output{}, err
	}

	out.ProduceRate = produce

	if len(groups) == 0 {
		return out, nil
	}

	before, err := admin.Lag(ctx, groups...)
	if err != nil {
		return Output{}, fmt.Errorf("measure lag for %q: %w", input.Topic, err)
	}

	sampleSeconds := input.SampleSeconds
	if sampleSeconds == 0 {
		sampleSeconds = defaultSampleSeconds
	}

	var after kadm.DescribedGroupLags

	if !input.SkipConsumeRate {
		// The consume rate is the change in committed offset over a known
		// interval. Kafka keeps no record of where a group was a minute ago,
		// so the only way to learn the rate is to look twice.
		select {
		case <-ctx.Done():
			return Output{}, ctx.Err()
		case <-time.After(time.Duration(sampleSeconds) * time.Second):
		}

		after, err = admin.Lag(ctx, groups...)
		if err != nil {
			return Output{}, fmt.Errorf("re-measure lag for %q: %w", input.Topic, err)
		}
	}

	for _, group := range groups {
		measured, err := measureGroup(
			input.Topic, group, before, after, produce, sampleSeconds, input.SkipConsumeRate)
		if err != nil {
			return Output{}, err
		}

		out.Groups = append(out.Groups, measured)
		out.TotalLag += measured.Lag
	}

	sort.Slice(out.Groups, func(i, j int) bool {
		return out.Groups[i].Group < out.Groups[j].Group
	})

	return out, nil
}

// resolveGroups returns the groups to measure: the one named, or every group
// with an interest in the topic.
func resolveGroups(
	ctx context.Context,
	admin *kadm.Client,
	input Input,
) ([]string, error) {

	if input.Group != "" {
		// A named group that does not exist is a mistake worth reporting.
		// Silently measuring nothing would look like a group with no lag.
		described, err := admin.DescribeGroups(ctx, input.Group)
		if err != nil {
			return nil, fmt.Errorf("describe group %q: %w", input.Group, err)
		}

		group, ok := described[input.Group]
		if !ok {
			return nil, fmt.Errorf("consumer group %q does not exist", input.Group)
		}

		if group.Err != nil {
			return nil, fmt.Errorf("consumer group %q: %w", input.Group, group.Err)
		}

		if group.State == "Dead" {
			return nil, fmt.Errorf(
				"consumer group %q does not exist: the broker reports it as Dead", input.Group)
		}

		return []string{input.Group}, nil
	}

	listed, err := admin.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}

	names := listed.Groups()
	if len(names) == 0 {
		return nil, nil
	}

	// Kafka has no index from topic to group, so every group is fetched and
	// those with commits for this topic are kept.
	commits := admin.FetchManyOffsets(ctx, names...)

	interested := make([]string, 0, len(names))

	for _, name := range names {
		response, ok := commits[name]
		if !ok || response.Err != nil {
			continue
		}

		if _, ok := response.Fetched.Lookup(input.Topic, 0); ok {
			interested = append(interested, name)

			continue
		}

		// Partition 0 is the common case, but a group may hold commits for
		// only a higher partition.
		found := false

		response.Fetched.Each(func(offset kadm.OffsetResponse) {
			if offset.Topic == input.Topic {
				found = true
			}
		})

		if found {
			interested = append(interested, name)
		}
	}

	sort.Strings(interested)

	return interested, nil
}

// measureGroup turns the raw lag readings for one group into the reported
// result, including what the numbers mean.
func measureGroup(
	topic string,
	group string,
	before kadm.DescribedGroupLags,
	after kadm.DescribedGroupLags,
	produce *ProduceRate,
	sampleSeconds int,
	skipped bool,
) (GroupLag, error) {

	described, ok := before[group]
	if !ok {
		return GroupLag{}, fmt.Errorf("no lag reported for group %q", group)
	}

	if err := described.Error(); err != nil {
		return GroupLag{}, fmt.Errorf("group %q: %w", group, err)
	}

	measured := GroupLag{
		Group:      group,
		State:      described.State,
		Members:    len(described.Members),
		Partitions: []PartitionLag{},
	}

	committed := int64(0)

	for _, partition := range described.Lag[topic] {
		reported := PartitionLag{
			Partition:       partition.Partition,
			CommittedOffset: partition.Commit.At,
			EndOffset:       partition.End.Offset,
			Lag:             partition.Lag,
		}

		// kadm reports a lag of -1 when the commit or the end offset could not
		// be read. Counting that as lag would corrupt the total, and reporting
		// it as zero would claim a group is caught up when it is unknown.
		if partition.Err != nil {
			reported.Error = partition.Err.Error()
			measured.Partitions = append(measured.Partitions, reported)

			continue
		}

		measured.Lag += partition.Lag
		committed += partition.Commit.At

		measured.Partitions = append(measured.Partitions, reported)
	}

	sort.Slice(measured.Partitions, func(i, j int) bool {
		return measured.Partitions[i].Partition < measured.Partitions[j].Partition
	})

	if skipped {
		measured.Status = status(measured, nil, produce)

		return measured, nil
	}

	measured.ConsumeRate = sampleConsumeRate(
		topic, group, committed, after, sampleSeconds)

	estimate(&measured, produce)

	return measured, nil
}
