// Package listtopics implements the list_topics MCP tool.
package listtopics

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/denizgursoy/kafka-mcp/internal/domain/script"
	"github.com/denizgursoy/kafka-mcp/internal/domain/topicconfig"
)

// scriptParameters are the variables a predicate sees, in the order match
// supplies them.
var scriptParameters = []string{
	"topic", "partitions", "replication_factor", "internal", "configs",
}

// defaultTimeout bounds how long predicates may run in total. It matches
// search_messages, so the one thing a caller has to remember about scripts is
// the same in both tools.
const defaultTimeout = 30 * time.Second

// Input is the argument set accepted by the list_topics tool.
type Input struct {
	Script        string `json:"script,omitempty" jsonschema:"Optional JavaScript that decides whether a topic is listed. Return true to keep it. In scope: topic (the name), partitions (number), replication_factor (number), internal (true for Kafka's own topics such as __consumer_offsets) and configs (an object of the values this topic sets for itself, such as configs['retention.ms']; inherited cluster defaults are not included). Examples: return topic.indexOf('orders') >= 0; return partitions > 6; return configs['cleanup.policy'] === 'compact'; return replication_factor === 1 && !internal. Omit to list every topic."`
	TimeoutSecond int    `json:"timeout_seconds,omitempty" jsonschema:"Optional wall-clock limit in seconds for evaluating the script. Defaults to 30. A topic whose evaluation is cut short is counted in script_errors rather than listed."`
}

// Topic is one topic and the shape a predicate filters on.
//
// The metadata comes from the same listing the names come from, so reporting it
// costs nothing and saves a describe_topic call per topic.
type Topic struct {
	Topic             string `json:"topic"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replication_factor"`
	// Internal is the broker's own flag rather than a guess from the name, so a
	// user topic beginning with an underscore is still an ordinary topic.
	Internal bool `json:"internal,omitempty"`
	// Configs holds only what the topic sets for itself. Inherited cluster
	// defaults are excluded: they are not choices anyone made, and including
	// them would make every topic look configured.
	Configs map[string]string `json:"configs,omitempty"`
}

// Output is the result returned by the list_topics tool.
type Output struct {
	Topics []Topic `json:"topics"`
	Count  int     `json:"count"`
	// ScriptErrors counts topics whose predicate threw or was cut short. They
	// are not listed, and a caller must not read their absence as a cluster
	// that does not hold them.
	ScriptErrors int `json:"script_errors,omitempty"`
}

const description = `
List the topics on the endpoint's cluster, sorted by name, each with its
partition count, replication factor and the configs it sets for itself.

Filter with an optional JavaScript predicate. A name match is
return topic.indexOf('orders') >= 0, and the predicate can also read partitions,
replication_factor, internal and configs — questions a substring filter cannot
express, such as which topics have more than six partitions, only one replica,
or a compacted cleanup policy.

Only configs a topic sets for itself are reported, because inherited cluster
defaults would make every topic look configured. Topics whose predicate throws
are counted in script_errors rather than listed, so a broken filter is never
mistaken for an empty cluster.
`

// Register adds the list_topics tool to the MCP server. The tool owns its own
// name, description and schema, so the server only has to make this one call.
func Register(server *mcp.Server, admin *kadm.Client) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "list_topics",
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, Output, error) {

			out, err := Run(ctx, admin, input)
			if err != nil {
				return nil, Output{}, fmt.Errorf("list topics: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run lists Kafka topics, optionally filtered by a JavaScript predicate.
// The returned topics are sorted by name so results are deterministic.
func Run(
	ctx context.Context,
	admin *kadm.Client,
	input Input,
) (Output, error) {

	listed, err := admin.ListTopics(ctx)
	if err != nil {
		return Output{}, fmt.Errorf("list topics: %w", err)
	}

	// Compiling before anything is read reports a malformed script once, rather
	// than failing identically on every topic.
	var filter *script.Script

	if input.Script != "" {
		filter, err = script.Compile(input.Script, scriptParameters...)
		if err != nil {
			return Output{}, err
		}

		defer filter.Close()

		timeout := time.Duration(input.TimeoutSecond) * time.Second
		if timeout <= 0 {
			timeout = defaultTimeout
		}

		bounded, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		// goja does not yield, so a predicate that never returns is never
		// preempted and the deadline is not observed until the call comes back.
		// Interrupting the runtime from outside is the only thing that stops it.
		stop := script.Guard(bounded, filter, "listing stopped")
		defer stop()
	}

	names := make([]string, 0, len(listed))

	for name, detail := range listed {
		if detail.Err != nil {
			// A topic that cannot be loaded would otherwise read as absent,
			// which is a different answer from the cluster not holding it.
			return Output{}, fmt.Errorf("topic %q: %w", name, detail.Err)
		}

		names = append(names, name)
	}

	// kadm returns a map and Go map order is random, so sort before reporting.
	sort.Strings(names)

	configs, err := topicconfig.ForTopics(ctx, admin, names...)
	if err != nil {
		return Output{}, err
	}

	out := Output{Topics: make([]Topic, 0, len(names))}

	for _, name := range names {
		detail := listed[name]

		topic := Topic{
			Topic:             name,
			Partitions:        len(detail.Partitions),
			ReplicationFactor: replicationFactor(detail),
			Internal:          detail.IsInternal,
			Configs:           topicconfig.Explicit(configs[name]),
		}

		if filter != nil {
			matched, err := match(filter, topic)
			if err != nil {
				// A predicate that failed reached no verdict, which is not the
				// same as deciding the topic does not match.
				out.ScriptErrors++

				continue
			}

			if !matched {
				continue
			}
		}

		out.Topics = append(out.Topics, topic)
	}

	out.Count = len(out.Topics)

	return out, nil
}

// match evaluates the predicate against one topic.
func match(filter *script.Script, topic Topic) (bool, error) {
	// A Go map reaches JavaScript as a plain object, so configs['retention.ms']
	// reads as a caller expects.
	configs := make(map[string]any, len(topic.Configs))

	for key, value := range topic.Configs {
		configs[key] = value
	}

	matched, err := filter.Call(
		filter.Value(topic.Topic),
		filter.Value(topic.Partitions),
		filter.Value(topic.ReplicationFactor),
		filter.Value(topic.Internal),
		filter.Value(configs),
	)
	if err != nil {
		return false, fmt.Errorf("script failed on topic %q: %w", topic.Topic, err)
	}

	return matched, nil
}

// replicationFactor reports the replica count of a topic's first partition.
//
// Kafka allows partitions of one topic to have different replica counts after a
// reassignment, so there is no single answer. The first partition is the one a
// caller means when asking how replicated a topic is.
func replicationFactor(detail kadm.TopicDetail) int {
	for _, partition := range detail.Partitions.Sorted() {
		return len(partition.Replicas)
	}

	return 0
}
