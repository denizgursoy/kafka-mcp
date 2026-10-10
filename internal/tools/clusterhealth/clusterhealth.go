// Package clusterhealth implements the cluster_health MCP tool.
package clusterhealth

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
)

// Input is the argument set accepted by the cluster_health tool.
//
// It takes no items: one call already covers the whole cluster, and wrapping
// a scan in a batch would only hide where the cost went.
type Input struct {
	Search          string `json:"search,omitempty" jsonschema:"Optional case-insensitive substring that a topic name must contain to be checked. Omit to check every topic. Brokers and the controller are always reported."`
	IncludeInternal bool   `json:"include_internal,omitempty" jsonschema:"Optional. When true, also check Kafka's internal topics such as __consumer_offsets. Defaults to false. An unhealthy internal topic breaks every consumer group, so include them when groups fail across the board."`
}

// Broker is one broker in the cluster.
type Broker struct {
	ID         int32  `json:"id"`
	Host       string `json:"host"`
	Port       int32  `json:"port"`
	Rack       string `json:"rack,omitempty"`
	Controller bool   `json:"controller"`
	// Leaders is how many of the checked partitions this broker leads. A
	// broker leading none while others lead many points at a broker that
	// restarted and was never given leadership back.
	Leaders int `json:"leaders"`
}

// Problem is one partition that is not fully healthy.
type Problem struct {
	Topic     string   `json:"topic"`
	Partition int32    `json:"partition"`
	Issues    []string `json:"issues"`
	Leader    int32    `json:"leader"`
	Replicas  []int32  `json:"replicas"`
	ISR       []int32  `json:"isr"`
	Offline   []int32  `json:"offline_replicas,omitempty"`
	MinISR    *int     `json:"min_insync_replicas,omitempty"`
	Error     string   `json:"error,omitempty"`

	// Reassigning is true when the partition is being moved between brokers.
	// A replica being added is out of sync until it catches up, so
	// under_replicated during a reassignment is expected rather than an
	// outage.
	Reassigning      bool    `json:"reassigning,omitempty"`
	AddingReplicas   []int32 `json:"adding_replicas,omitempty"`
	RemovingReplicas []int32 `json:"removing_replicas,omitempty"`
}

// Summary counts the checked partitions by condition. A partition with
// several issues is counted under each of them.
type Summary struct {
	Topics          int `json:"topics"`
	Partitions      int `json:"partitions"`
	Offline         int `json:"offline"`
	UnderReplicated int `json:"under_replicated"`
	UnderMinISR     int `json:"under_min_isr"`
	Errored         int `json:"errored"`
	// Reassigning counts problem partitions that are mid-reassignment.
	Reassigning int `json:"reassigning"`
}

// Output is the result returned by the cluster_health tool.
type Output struct {
	ClusterID  string    `json:"cluster_id"`
	Controller int32     `json:"controller"`
	Brokers    []Broker  `json:"brokers"`
	Summary    Summary   `json:"summary"`
	Problems   []Problem `json:"problems"`
	Healthy    bool      `json:"healthy"`

	// MinISRUnknown lists topics whose min.insync.replicas the broker did not
	// report, so under_min_isr could not be judged for them. Kafka always
	// reports it; some Kafka-compatible brokers do not.
	MinISRUnknown []string `json:"min_isr_unknown,omitempty"`
	Warnings      []string `json:"warnings"`
}

// Issues reported on a Problem.
const (
	IssueOffline         = "offline"
	IssueUnderReplicated = "under_replicated"
	IssueUnderMinISR     = "under_min_isr"
	IssueError           = "error"
)

const description = `
Check the health of the cluster this endpoint serves in one call: cluster id,
controller, every broker (id, host, port, rack, and how many partitions it
leads), a summary count, and every partition that is not fully healthy.

A problem partition lists its issues:
- offline: no leader, so nothing can be produced to or consumed from it.
- under_replicated: fewer in-sync replicas than replicas, so a broker is down
  or falling behind.
- under_min_isr: fewer in-sync replicas than the topic's min.insync.replicas,
  so producers using acks=all fail with NOT_ENOUGH_REPLICAS.
- error: the broker returned an error for the partition.

A problem partition with reassigning true is being moved between brokers;
adding_replicas catch up before joining the ISR, so under_replicated there is
expected while the move runs, not an outage.

healthy is true only when there are no problems. Use search to limit the check
to topics whose name contains a substring (case-insensitive); internal topics
are skipped unless include_internal is true. min_isr_unknown lists topics whose
min.insync.replicas the broker did not report, so under_min_isr was not judged
for them.
`

// Register adds the cluster_health tool to the MCP server.
func Register(server *mcp.Server, admin *kadm.Client) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "cluster_health",
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, Output, error) {

			out, err := Run(ctx, admin, input)
			if err != nil {
				return nil, Output{}, fmt.Errorf("cluster health: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run reads the cluster metadata and classifies every checked partition.
func Run(ctx context.Context, admin *kadm.Client, input Input) (Output, error) {
	metadata, err := admin.Metadata(ctx)
	if err != nil {
		return Output{}, fmt.Errorf("read cluster metadata: %w", err)
	}

	out := Output{
		ClusterID:  metadata.Cluster,
		Controller: metadata.Controller,
		Brokers:    make([]Broker, 0, len(metadata.Brokers)),
		Problems:   []Problem{},
		Warnings:   []string{},
	}

	search := strings.ToLower(input.Search)

	topics := make([]kadm.TopicDetail, 0, len(metadata.Topics))
	for _, topic := range metadata.Topics.Sorted() {
		if topic.IsInternal && !input.IncludeInternal {
			continue
		}

		if search != "" && !strings.Contains(strings.ToLower(topic.Topic), search) {
			continue
		}

		topics = append(topics, topic)
	}

	minISR, unknown, err := minInsyncReplicas(ctx, admin, topics)
	if err != nil {
		// Without min.insync.replicas the other checks still stand, so the
		// gap is reported rather than failing the whole report.
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"could not read min.insync.replicas, so under_min_isr was not judged: %v", err))
	}

	out.MinISRUnknown = unknown

	leaders := map[int32]int{}

	for _, topic := range topics {
		out.Summary.Topics++

		if topic.Err != nil {
			out.Summary.Errored++
			out.Problems = append(out.Problems, Problem{
				Topic: topic.Topic, Partition: -1, Leader: -1,
				Issues: []string{IssueError}, Replicas: []int32{}, ISR: []int32{},
				Error: topic.Err.Error(),
			})

			continue
		}

		for _, partition := range topic.Partitions.Sorted() {
			out.Summary.Partitions++

			if partition.Leader >= 0 {
				leaders[partition.Leader]++
			}

			var limit *int
			if value, ok := minISR[topic.Topic]; ok {
				limit = &value
			}

			problem, unhealthy := Classify(partition, limit)
			if !unhealthy {
				continue
			}

			for _, issue := range problem.Issues {
				switch issue {
				case IssueOffline:
					out.Summary.Offline++
				case IssueUnderReplicated:
					out.Summary.UnderReplicated++
				case IssueUnderMinISR:
					out.Summary.UnderMinISR++
				case IssueError:
					out.Summary.Errored++
				}
			}

			out.Problems = append(out.Problems, problem)
		}
	}

	if len(out.Problems) > 0 {
		reassignments, err := admin.ListPartitionReassignments(ctx, problemSet(out.Problems))
		if err != nil {
			out.Warnings = append(out.Warnings, fmt.Sprintf(
				"could not list partition reassignments, so a moving partition may look like an outage: %v", err))
		} else {
			out.Problems = MarkReassigning(out.Problems, reassignments)
			for _, problem := range out.Problems {
				if problem.Reassigning {
					out.Summary.Reassigning++
				}
			}
		}
	}

	for _, broker := range metadata.Brokers {
		entry := Broker{
			ID:         broker.NodeID,
			Host:       broker.Host,
			Port:       broker.Port,
			Controller: broker.NodeID == metadata.Controller,
			Leaders:    leaders[broker.NodeID],
		}

		if broker.Rack != nil {
			entry.Rack = *broker.Rack
		}

		out.Brokers = append(out.Brokers, entry)
	}

	sort.Slice(out.Brokers, func(i, j int) bool { return out.Brokers[i].ID < out.Brokers[j].ID })

	if metadata.Controller < 0 {
		out.Warnings = append(out.Warnings,
			"the cluster reports no controller, so topic creation, leader election and partition reassignment cannot happen")
	}

	out.Healthy = len(out.Problems) == 0 && metadata.Controller >= 0

	return out, nil
}

// Classify decides what is wrong with one partition. minISR is nil when the
// topic's min.insync.replicas is not known, and under_min_isr is then not
// judged rather than guessed.
func Classify(partition kadm.PartitionDetail, minISR *int) (Problem, bool) {
	problem := Problem{
		Topic:     partition.Topic,
		Partition: partition.Partition,
		Leader:    partition.Leader,
		Replicas:  sortedCopy(partition.Replicas),
		ISR:       sortedCopy(partition.ISR),
		Issues:    []string{},
		MinISR:    minISR,
	}

	if len(partition.OfflineReplicas) > 0 {
		problem.Offline = sortedCopy(partition.OfflineReplicas)
	}

	// A partition whose leader is unavailable comes back with an error code
	// and, depending on the broker, a leader of -1. Both mean the same thing
	// to a caller: the partition cannot be used.
	if partition.Err != nil {
		problem.Error = partition.Err.Error()

		if isLeaderError(partition.Err) {
			problem.Issues = append(problem.Issues, IssueOffline)
		} else {
			problem.Issues = append(problem.Issues, IssueError)
		}
	} else if partition.Leader < 0 {
		problem.Issues = append(problem.Issues, IssueOffline)
	}

	if len(partition.Replicas) > 0 && len(partition.ISR) < len(partition.Replicas) {
		problem.Issues = append(problem.Issues, IssueUnderReplicated)
	}

	if minISR != nil && len(partition.Replicas) > 0 && len(partition.ISR) < *minISR {
		problem.Issues = append(problem.Issues, IssueUnderMinISR)
	}

	return problem, len(problem.Issues) > 0
}

func isLeaderError(err error) bool {
	message := err.Error()

	return strings.Contains(message, "LEADER_NOT_AVAILABLE") ||
		strings.Contains(message, "NOT_LEADER") ||
		strings.Contains(message, "REPLICA_NOT_AVAILABLE")
}

// minInsyncReplicas reads min.insync.replicas for every checked topic in one
// request. Topics the broker reports no value for are returned separately, so
// an absent setting is never mistaken for a value of zero.
func minInsyncReplicas(
	ctx context.Context,
	admin *kadm.Client,
	topics []kadm.TopicDetail,
) (map[string]int, []string, error) {

	names := make([]string, 0, len(topics))
	for _, topic := range topics {
		if topic.Err == nil {
			names = append(names, topic.Topic)
		}
	}

	if len(names) == 0 {
		return map[string]int{}, nil, nil
	}

	described, err := admin.DescribeTopicConfigs(ctx, names...)
	if err != nil {
		return map[string]int{}, nil, err
	}

	values := make(map[string]int, len(names))
	var errs []error

	for _, resource := range described {
		if resource.Err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", resource.Name, resource.Err))

			continue
		}

		for _, config := range resource.Configs {
			if config.Key != "min.insync.replicas" {
				continue
			}

			if value, err := strconv.Atoi(config.MaybeValue()); err == nil {
				values[resource.Name] = value
			}
		}
	}

	unknown := []string{}
	for _, name := range names {
		if _, ok := values[name]; !ok {
			unknown = append(unknown, name)
		}
	}

	sort.Strings(unknown)

	if len(unknown) == 0 {
		unknown = nil
	}

	return values, unknown, errors.Join(errs...)
}

func sortedCopy(values []int32) []int32 {
	out := slices.Clone(values)
	if out == nil {
		out = []int32{}
	}

	slices.Sort(out)

	return out
}

// MarkReassigning labels the problems whose partition is being reassigned.
func MarkReassigning(problems []Problem, reassignments kadm.ListPartitionReassignmentsResponses) []Problem {
	for i := range problems {
		moving, ok := reassignments[problems[i].Topic][problems[i].Partition]
		if !ok || (len(moving.AddingReplicas) == 0 && len(moving.RemovingReplicas) == 0) {
			continue
		}

		problems[i].Reassigning = true
		problems[i].AddingReplicas = sortedCopy(moving.AddingReplicas)
		problems[i].RemovingReplicas = sortedCopy(moving.RemovingReplicas)
	}

	return problems
}

// problemSet names the partitions to ask about, so the request stays as small
// as the problem list rather than covering the whole cluster.
func problemSet(problems []Problem) kadm.TopicsSet {
	set := make(kadm.TopicsSet)
	for _, problem := range problems {
		if problem.Partition >= 0 {
			set.Add(problem.Topic, problem.Partition)
		}
	}

	return set
}
