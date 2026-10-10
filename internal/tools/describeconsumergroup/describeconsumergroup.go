// Package describeconsumergroup implements the describe_consumer_group MCP tool.
package describeconsumergroup

import (
	"context"
	"fmt"
	"sort"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
)

// Item is one group to describe.
type Item struct {
	Group         string `json:"group" jsonschema:"Consumer group to describe. Matched exactly and case-sensitively."`
	SampleSeconds int    `json:"sample_seconds,omitempty" jsonschema:"Optional number of seconds, at most 60, to watch the group before describing it. The group is read every second; observation then reports every state it passed through, and the members that joined or left. Use it when a group seems to keep rebalancing. The call blocks this long. Defaults to 0, no observation."`
}

// Input is the argument set accepted by the describe_consumer_group tool.
type Input struct {
	Items []Item `json:"items" jsonschema:"The groups to describe, 1 to 100 of them. Describing one group is an array of length one. Results follow this order and a group that does not exist is reported against its own item."`
}

// Assignment is the partitions of one topic a member owns.
type Assignment struct {
	Topic      string  `json:"topic"`
	Partitions []int32 `json:"partitions"`
}

// Member is one running consumer in the group.
type Member struct {
	MemberID    string       `json:"member_id"`
	InstanceID  string       `json:"instance_id,omitempty"`
	ClientID    string       `json:"client_id"`
	Host        string       `json:"host"`
	Assignments []Assignment `json:"assignments"`
}

// Partition is the group's position on one partition, and who owns it.
type Partition struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`

	// HasCommit is false when the group owns the partition but never
	// committed on it. CommittedOffset and Lag are then not meaningful: where
	// the consumer starts is decided by its auto.offset.reset.
	HasCommit       bool   `json:"has_commit"`
	CommittedOffset int64  `json:"committed_offset"`
	EndOffset       int64  `json:"end_offset"`
	Lag             int64  `json:"lag"`
	MemberID        string `json:"member_id,omitempty"`
	ClientID        string `json:"client_id,omitempty"`
	Host            string `json:"host,omitempty"`
	Error           string `json:"error,omitempty"`
}

// Observation is what changed while the group was watched.
type Observation struct {
	Seconds int `json:"seconds"`
	Samples int `json:"samples"`
	// States is every state the group was seen in, in order, with
	// consecutive repeats collapsed: Stable, PreparingRebalance, Stable.
	States []string `json:"states"`
	// Joined and Left are members that appeared or disappeared by member id.
	// A member that restarts gets a new id, so it shows in both.
	Joined []Member `json:"joined"`
	Left   []Member `json:"left"`
	// Unstable is true when the state left Stable or membership changed.
	Unstable bool `json:"unstable"`
}

// Output is the description of one group.
type Output struct {
	Group        string      `json:"group"`
	State        string      `json:"state"`
	ProtocolType string      `json:"protocol_type"`
	Assignor     string      `json:"assignor,omitempty"`
	Coordinator  int32       `json:"coordinator"`
	Members      []Member    `json:"members"`
	Partitions   []Partition `json:"partitions"`
	TotalLag     int64       `json:"total_lag"`

	Observation *Observation `json:"observation,omitempty"`
}

type BatchOutput = batch.Output[Output]

const description = `
Describe 1 to 100 consumer groups in one call through items: state, assignor,
coordinator broker, every running member (member_id, client_id, host and the
partitions assigned to it), and for every partition the group owns or has
committed on, the committed offset, end offset, lag, and the member, client id
and host consuming it. Partitions and members are sorted.

Use it to find which consumer instance owns a stuck or lagging partition, so
the operator knows which pod or host to inspect. has_commit false means the
group owns the partition but never committed there, so its starting point is
decided by the consumer's auto.offset.reset rather than by an offset. An Empty
group has no members but keeps its commits.

sample_seconds watches the group for that long first and reports observation:
the states it passed through and the members that joined or left. A group that
keeps cycling through PreparingRebalance, or whose members keep changing, is
rebalancing; the departing members' client_id and host point at the consumer
that keeps leaving.

Results follow items order, each carrying index with result or error.
`

// Register adds the describe_consumer_group tool to the MCP server.
func Register(server *mcp.Server, admin *kadm.Client) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "describe_consumer_group",
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, BatchOutput, error) {

			out, err := Run(ctx, admin, input)
			if err != nil {
				return nil, BatchOutput{}, fmt.Errorf("describe consumer group: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run describes every requested group with bounded concurrency.
func Run(ctx context.Context, admin *kadm.Client, input Input) (BatchOutput, error) {
	return batch.Run(ctx, input.Items, batch.MaxItems, func(ctx context.Context, item Item) (Output, error) {
		if err := batch.Bounded("sample_seconds", item.SampleSeconds, maxSampleSeconds); err != nil {
			return Output{}, err
		}

		var observation *Observation
		if item.SampleSeconds > 0 {
			watched, err := observe(ctx, admin, item.Group, item.SampleSeconds)
			if err != nil {
				return Output{}, err
			}
			observation = watched
		}

		out, err := describe(ctx, admin, item.Group)
		if err != nil {
			return Output{}, err
		}
		out.Observation = observation

		return out, nil
	})
}

type owner struct {
	memberID, clientID, host string
}

type topicPartition struct {
	topic     string
	partition int32
}

func describe(ctx context.Context, admin *kadm.Client, group string) (Output, error) {
	if group == "" {
		return Output{}, fmt.Errorf("group is required")
	}

	described, err := admin.DescribeGroups(ctx, group)
	if err != nil {
		return Output{}, fmt.Errorf("describe group %q: %w", group, err)
	}

	detail, ok := described[group]

	// Kafka answers a group it has never heard of with state Dead rather than
	// an error, which would otherwise read as a group that exists and is idle.
	if !ok || detail.State == "Dead" {
		return Output{}, fmt.Errorf("consumer group %q does not exist", group)
	}

	if detail.Err != nil {
		return Output{}, fmt.Errorf("consumer group %q: %w", group, detail.Err)
	}

	out := Output{
		Group:        group,
		State:        detail.State,
		ProtocolType: detail.ProtocolType,
		Assignor:     detail.Protocol,
		Coordinator:  detail.Coordinator.NodeID,
		Members:      make([]Member, 0, len(detail.Members)),
		Partitions:   []Partition{},
	}

	owners := map[topicPartition]owner{}

	for _, described := range detail.Members {
		member := Member{
			MemberID:    described.MemberID,
			ClientID:    described.ClientID,
			Host:        described.ClientHost,
			Assignments: []Assignment{},
		}

		if described.InstanceID != nil {
			member.InstanceID = *described.InstanceID
		}

		// Only the consumer protocol has an assignment this server can read.
		// A Connect group's members are still reported, without partitions.
		if assigned, ok := described.Assigned.AsConsumer(); ok {
			for _, topic := range assigned.Topics {
				partitions := append([]int32{}, topic.Partitions...)
				sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

				member.Assignments = append(member.Assignments, Assignment{Topic: topic.Topic, Partitions: partitions})

				for _, partition := range partitions {
					owners[topicPartition{topic.Topic, partition}] = owner{
						memberID: member.MemberID,
						clientID: member.ClientID,
						host:     member.Host,
					}
				}
			}
		}

		sort.Slice(member.Assignments, func(i, j int) bool {
			return member.Assignments[i].Topic < member.Assignments[j].Topic
		})

		out.Members = append(out.Members, member)
	}

	sort.Slice(out.Members, func(i, j int) bool {
		if out.Members[i].ClientID != out.Members[j].ClientID {
			return out.Members[i].ClientID < out.Members[j].ClientID
		}

		return out.Members[i].MemberID < out.Members[j].MemberID
	})

	commits, err := admin.FetchOffsets(ctx, group)
	if err != nil {
		return Output{}, fmt.Errorf("fetch committed offsets for group %q: %w", group, err)
	}

	// A partition can be owned without a commit, or committed without an
	// owner once its consumer has gone. Both matter, so the set is the union.
	seen := map[topicPartition]bool{}
	for key := range owners {
		seen[key] = true
	}

	commits.Each(func(commit kadm.OffsetResponse) {
		seen[topicPartition{commit.Topic, commit.Partition}] = true
	})

	topics := map[string]bool{}
	for key := range seen {
		topics[key.topic] = true
	}

	ends, err := endOffsets(ctx, admin, topics)
	if err != nil {
		return Output{}, err
	}

	for key := range seen {
		partition := Partition{Topic: key.topic, Partition: key.partition, CommittedOffset: -1, EndOffset: -1}

		if who, ok := owners[key]; ok {
			partition.MemberID, partition.ClientID, partition.Host = who.memberID, who.clientID, who.host
		}

		if end, ok := ends.Lookup(key.topic, key.partition); ok {
			if end.Err != nil {
				partition.Error = fmt.Sprintf("end offset unavailable: %v", end.Err)
			} else {
				partition.EndOffset = end.Offset
			}
		}

		if commit, ok := commits.Lookup(key.topic, key.partition); ok {
			if commit.Err != nil {
				partition.Error = fmt.Sprintf("committed offset unavailable: %v", commit.Err)
			} else if commit.At >= 0 {
				partition.HasCommit = true
				partition.CommittedOffset = commit.At
			}
		}

		if partition.HasCommit && partition.EndOffset >= 0 {
			partition.Lag = max(partition.EndOffset-partition.CommittedOffset, 0)
			out.TotalLag += partition.Lag
		}

		out.Partitions = append(out.Partitions, partition)
	}

	sort.Slice(out.Partitions, func(i, j int) bool {
		if out.Partitions[i].Topic != out.Partitions[j].Topic {
			return out.Partitions[i].Topic < out.Partitions[j].Topic
		}

		return out.Partitions[i].Partition < out.Partitions[j].Partition
	})

	return out, nil
}

func endOffsets(ctx context.Context, admin *kadm.Client, topics map[string]bool) (kadm.ListedOffsets, error) {
	if len(topics) == 0 {
		return kadm.ListedOffsets{}, nil
	}

	names := make([]string, 0, len(topics))
	for topic := range topics {
		names = append(names, topic)
	}

	sort.Strings(names)

	ends, err := admin.ListEndOffsets(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}

	return ends, nil
}
