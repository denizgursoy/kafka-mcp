// Package listconsumergroups implements the list_consumer_groups MCP tool.
package listconsumergroups

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
)

// Input is the argument set accepted by the list_consumer_groups tool.
type Input struct {
	Topic  string   `json:"topic,omitempty" jsonschema:"Optional topic name. When given, only groups that consume or have committed offsets for this topic are returned. Matched exactly and case-sensitively."`
	States []string `json:"states,omitempty" jsonschema:"Optional group states to return, such as Stable, Empty, PreparingRebalance or Dead. Defaults to every state."`
}

// Group describes one consumer group.
type Group struct {
	Group        string   `json:"group"`
	State        string   `json:"state"`
	ProtocolType string   `json:"protocol_type"`
	Members      int      `json:"members"`
	Topics       []string `json:"topics"`
}

// Output is the result returned by the list_consumer_groups tool.
type Output struct {
	Groups []Group `json:"groups"`
	Count  int     `json:"count"`
}

const description = `
List the consumer groups on the cluster, with their state, member count and the
topics they consume.

Use this to find out who consumes a topic before measuring lag with
consumer_lag.

"state" matters when interpreting lag:

  Stable              the group has active members consuming
  Empty               the group exists and may still hold committed offsets,
                      but no member is consuming, so its lag will not shrink
  PreparingRebalance  members are joining or leaving
  Dead                the group is gone

A group in state Empty can still report lag, because committed offsets outlive
the consumers that made them. Lag that is not moving is explained by the state,
not by a slow consumer.

Kafka has no index from topic to group, so filtering by "topic" lists every
group on the cluster and describes each one. That is fine for ordinary
clusters, but it is not a free call on a cluster with very many groups.
`

// Register adds the list_consumer_groups tool to the MCP server.
func Register(server *mcp.Server, admin *kadm.Client) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "list_consumer_groups",
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, Output, error) {

			out, err := Run(ctx, admin, input)
			if err != nil {
				return nil, Output{}, fmt.Errorf("list consumer groups: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run lists consumer groups, optionally restricted to one topic or to given
// states.
func Run(
	ctx context.Context,
	admin *kadm.Client,
	input Input,
) (Output, error) {

	listed, err := admin.ListGroups(ctx, input.States...)
	if err != nil {
		return Output{}, fmt.Errorf("list groups: %w", err)
	}

	out := Output{Groups: []Group{}}

	names := listed.Groups()
	if len(names) == 0 {
		return out, nil
	}

	described, err := admin.DescribeGroups(ctx, names...)
	if err != nil {
		return Output{}, fmt.Errorf("describe groups: %w", err)
	}

	// A group may hold committed offsets for a topic none of its current
	// members are assigned, which is exactly the case when consumers have
	// stopped. Fetching commits as well as assignments is what makes those
	// groups findable by topic.
	commits := admin.FetchManyOffsets(ctx, names...)

	for _, group := range described {
		if group.Err != nil {
			// One unreadable group must not hide the rest, but silently
			// dropping it would misreport the cluster, so it is reported with
			// the error in place of its details.
			out.Groups = append(out.Groups, Group{
				Group: group.Group,
				State: fmt.Sprintf("error: %v", group.Err),
			})

			continue
		}

		topics := groupTopics(group, commits)

		if input.Topic != "" && !contains(topics, input.Topic) {
			continue
		}

		out.Groups = append(out.Groups, Group{
			Group:        group.Group,
			State:        group.State,
			ProtocolType: group.ProtocolType,
			Members:      len(group.Members),
			Topics:       topics,
		})
	}

	// kadm returns maps, and Go map iteration order is random, so sort.
	sort.Slice(out.Groups, func(i, j int) bool {
		return out.Groups[i].Group < out.Groups[j].Group
	})

	out.Count = len(out.Groups)

	return out, nil
}

// groupTopics returns every topic a group is involved with: those its members
// are assigned, and those it merely holds committed offsets for.
func groupTopics(group kadm.DescribedGroup, commits kadm.FetchOffsetsResponses) []string {
	seen := make(map[string]bool)

	for _, topic := range group.AssignedPartitions().Topics() {
		seen[topic] = true
	}

	for _, topic := range group.JoinTopics() {
		seen[topic] = true
	}

	if response, ok := commits[group.Group]; ok && response.Err == nil {
		response.Fetched.Each(func(offset kadm.OffsetResponse) {
			seen[offset.Topic] = true
		})
	}

	topics := make([]string, 0, len(seen))

	for topic := range seen {
		topics = append(topics, topic)
	}

	sort.Strings(topics)

	return topics
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if strings.EqualFold(value, want) {
			return true
		}
	}

	return false
}
