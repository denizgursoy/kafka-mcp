// Package compareclusters implements the compare_clusters MCP tool.
package compareclusters

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/topicconfig"
)

// Item is one cluster to compare this endpoint's cluster against.
//
// Search is a plain substring rather than the JavaScript predicate list_topics
// takes, and that difference is deliberate. This filter selects which topics are
// compared, and it runs once per cluster, so it must choose the same topics on
// both sides. A name does. A predicate reading partitions or configs would not:
// a topic with 1 partition here and 6 there would pass the filter on one side
// and fail it on the other, and the comparison would report a topic both
// clusters hold as missing from one — with creating it as the documented next
// step. Only properties that are equal on both sides can safely filter a
// comparison.
type Item struct {
	Cluster         string `json:"cluster" jsonschema:"Name of the other cluster to compare against. Use list_clusters to see which names are valid. Comparing this endpoint's own cluster is allowed and reports no differences."`
	Search          string `json:"search,omitempty" jsonschema:"Optional case-insensitive substring that a topic name must contain to be compared. Omit to compare every topic. Use it to limit a comparison to one team's prefix, since whole-cluster reports are long."`
	IncludeInternal bool   `json:"include_internal,omitempty" jsonschema:"Optional. When true, also compare Kafka's internal topics such as __consumer_offsets. Defaults to false, because they exist on every cluster and are never a difference worth acting on."`
}

// Input is the argument set accepted by the compare_clusters tool.
type Input struct {
	Items []Item `json:"items" jsonschema:"The clusters to compare against, 1 to 100 of them. Comparing one cluster is an array of length one. Results follow this order and an unreachable cluster is reported against its own item."`
}

// Cluster describes one side of a comparison.
type Cluster struct {
	Name string `json:"name"`
	// Brokers bounds the replication factor a topic can have, so it is part of
	// knowing whether a topic on one cluster can exist on the other at all.
	Brokers int `json:"brokers"`
	Topics  int `json:"topics"`
}

// Topic reports a topic present on one cluster and absent from the other.
//
// The shape travels with it so the report can be handed to create_topic without
// a second round of describe calls. Nothing is created here: creating stays in
// create_topic, which owns the broker-validated preview and the warning that a
// partition count can never be reduced.
type Topic struct {
	Topic             string            `json:"topic"`
	Partitions        int               `json:"partitions"`
	ReplicationFactor int               `json:"replication_factor"`
	Configs           map[string]string `json:"configs,omitempty"`
}

// Side is one cluster's view of a topic both clusters hold.
type Side struct {
	Partitions        int               `json:"partitions"`
	ReplicationFactor int               `json:"replication_factor"`
	Configs           map[string]string `json:"configs,omitempty"`
}

// Drift is a topic both clusters hold but do not agree on.
type Drift struct {
	Topic string `json:"topic"`
	Here  Side   `json:"here"`
	There Side   `json:"there"`
	// Differences names what disagrees — "partitions", "replication_factor" or
	// a config key — so a reader does not have to diff the two sides by eye.
	Differences []string `json:"differences"`
}

// Output is the result of comparing this endpoint's cluster with another.
type Output struct {
	// Here and There name the direction, because a comparison is asymmetric and
	// "missing" is meaningless without knowing missing from where.
	Here  string `json:"here"`
	There string `json:"there"`

	// HereCluster and ThereCluster carry each side's size, which is what says
	// whether a topic could exist on the other cluster at all.
	HereCluster  Cluster `json:"here_cluster"`
	ThereCluster Cluster `json:"there_cluster"`

	OnlyHere  []Topic `json:"only_here"`
	OnlyThere []Topic `json:"only_there"`
	Differing []Drift `json:"differing"`

	// InBoth is a count rather than a list: two clusters commonly share
	// hundreds of identical topics, and naming them would bury the differences.
	InBoth   int      `json:"in_both"`
	Warnings []string `json:"warnings,omitempty"`
	Note     string   `json:"note,omitempty"`
}

type BatchOutput = batch.Output[Output]

const description = `
Compare the topics of 1 to 100 other clusters against the one this endpoint
serves, through items. Reports which topics only this cluster has, which only
the other has, which exist on both but disagree, and how many brokers and topics
each side has. Comparing one cluster is an items array of length one.

Use it to find what preproduction has that production does not, or to check
whether two environments still match. Topics reported as only on the other
cluster carry their partition count, replication factor and explicitly-set
configs, so the report can be handed straight to create_topic.

This tool creates and changes nothing. To create the missing topics, pass them
to create_topic, which previews them against the broker first.

Only configs a topic sets for itself are compared. Two clusters may carry
different broker defaults, and comparing inherited values would report every
topic as different. Internal topics are excluded unless include_internal is set.

Topic listings come from the client's metadata cache, so a topic created within
the last few seconds may still be reported as missing. Repeat the comparison
after a moment rather than creating it twice.
`

// Register adds the compare_clusters tool to the MCP server.
//
// It takes the registry rather than one client because naming another cluster is
// the whole point of it, and it is registered on every endpoint — including
// read-only ones — since it only reads metadata and writes nothing anywhere.
func Register(server *mcp.Server, clusters *kafkaclient.Registry, own string) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "compare_clusters",
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, BatchOutput, error) {

			out, err := Run(ctx, clusters, own, input)
			if err != nil {
				return nil, BatchOutput{}, fmt.Errorf("compare clusters: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run compares this endpoint's cluster against every requested cluster.
func Run(
	ctx context.Context,
	clusters *kafkaclient.Registry,
	own string,
	input Input,
) (BatchOutput, error) {

	return batch.Run(ctx, input.Items, batch.MaxItems, func(ctx context.Context, item Item) (Output, error) {
		return compare(ctx, clusters, own, item)
	})
}

func compare(
	ctx context.Context,
	clusters *kafkaclient.Registry,
	own string,
	item Item,
) (Output, error) {

	here := clusters.Endpoint(own)
	if here == nil {
		// Direct package tests and callers predating explicit endpoints pass a
		// cluster name. Keep that API while production uses endpoint names.
		here = clusters.Get(own)
	}

	if here == nil {
		return Output{}, fmt.Errorf("unknown endpoint or cluster %q", own)
	}

	if item.Cluster == "" {
		return Output{}, fmt.Errorf(
			"cluster is required: use list_clusters to see which clusters this server serves")
	}

	there := clusters.Exposed(item.Cluster)
	if there == nil {
		return Output{}, fmt.Errorf(
			"unknown cluster %q: use list_clusters to see which clusters this server serves",
			item.Cluster)
	}

	hereName := here.Config().Name

	hereTopics, err := read(ctx, here.Admin(), item)
	if err != nil {
		return Output{}, fmt.Errorf("read cluster %q: %w", hereName, err)
	}

	thereTopics, err := read(ctx, there.Admin(), item)
	if err != nil {
		return Output{}, fmt.Errorf("read cluster %q: %w", item.Cluster, err)
	}

	hereBrokers, err := brokerCount(ctx, here.Admin())
	if err != nil {
		return Output{}, fmt.Errorf("count brokers on %q: %w", hereName, err)
	}

	thereBrokers, err := brokerCount(ctx, there.Admin())
	if err != nil {
		return Output{}, fmt.Errorf("count brokers on %q: %w", item.Cluster, err)
	}

	out := Output{
		Here:         hereName,
		There:        item.Cluster,
		HereCluster:  Cluster{Name: hereName, Brokers: hereBrokers, Topics: len(hereTopics)},
		ThereCluster: Cluster{Name: item.Cluster, Brokers: thereBrokers, Topics: len(thereTopics)},
		OnlyHere:     []Topic{},
		OnlyThere:    []Topic{},
		Differing:    []Drift{},
		Warnings:     []string{},
	}

	for _, name := range sortedNames(hereTopics) {
		mine := hereTopics[name]

		theirs, shared := thereTopics[name]
		if !shared {
			out.OnlyHere = append(out.OnlyHere, mine.topic())

			continue
		}

		out.InBoth++

		if differences := diff(mine, theirs); len(differences) > 0 {
			out.Differing = append(out.Differing, Drift{
				Topic:       name,
				Here:        mine.side(),
				There:       theirs.side(),
				Differences: differences,
			})
		}
	}

	for _, name := range sortedNames(thereTopics) {
		if _, shared := hereTopics[name]; !shared {
			out.OnlyThere = append(out.OnlyThere, thereTopics[name].topic())
		}
	}

	if hereBrokers != thereBrokers {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"%s has %d broker(s) and %s has %d, so a replication factor that works on one may be impossible on the other",
			hereName, hereBrokers, item.Cluster, thereBrokers))
	}

	out.Note = fmt.Sprintf(
		"%d topic(s) only on %s, %d only on %s, %d on both with %d differing. Nothing was changed; pass only_there to create_topic to create the missing topics",
		len(out.OnlyHere), hereName, len(out.OnlyThere), item.Cluster, out.InBoth, len(out.Differing))

	return out, nil
}

// details is one topic as both clusters are compared on it.
type details struct {
	name       string
	partitions int
	replicas   int
	configs    map[string]string
}

func (d details) topic() Topic {
	return Topic{
		Topic:             d.name,
		Partitions:        d.partitions,
		ReplicationFactor: d.replicas,
		Configs:           d.configs,
	}
}

func (d details) side() Side {
	return Side{
		Partitions:        d.partitions,
		ReplicationFactor: d.replicas,
		Configs:           d.configs,
	}
}

// read lists the topics of one cluster with the shape each comparison needs.
func read(
	ctx context.Context,
	admin *kadm.Client,
	item Item,
) (map[string]details, error) {

	listed, err := admin.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}

	search := strings.ToLower(item.Search)
	selected := make([]string, 0, len(listed))
	topics := make(map[string]details, len(listed))

	for name, detail := range listed {
		if detail.Err != nil {
			// A topic that cannot be loaded would otherwise read as absent,
			// which is the one conclusion a drift report must never reach by
			// accident.
			return nil, fmt.Errorf("topic %q: %w", name, detail.Err)
		}

		// IsInternal comes from the broker rather than from guessing at a
		// leading underscore, so a user topic named with one is still compared.
		if detail.IsInternal && !item.IncludeInternal {
			continue
		}

		if search != "" && !strings.Contains(strings.ToLower(name), search) {
			continue
		}

		topics[name] = details{
			name:       name,
			partitions: len(detail.Partitions),
			replicas:   replicationFactor(detail),
		}

		selected = append(selected, name)
	}

	if len(selected) == 0 {
		return topics, nil
	}

	// One request for every selected topic rather than one per topic: a whole
	// cluster otherwise costs a round trip per topic.
	configs, err := topicconfig.ForTopics(ctx, admin, selected...)
	if err != nil {
		return nil, err
	}

	for name, entries := range configs {
		if detail, ok := topics[name]; ok {
			detail.configs = topicconfig.Explicit(entries)
			topics[name] = detail
		}
	}

	return topics, nil
}

// replicationFactor reports the replica count of a topic's first partition.
//
// Kafka allows partitions of one topic to have different replica counts after a
// reassignment, so there is no single answer. The first partition is what
// create_topic would reproduce, which is what this report is read for.
func replicationFactor(detail kadm.TopicDetail) int {
	for _, partition := range detail.Partitions.Sorted() {
		return len(partition.Replicas)
	}

	return 0
}

func brokerCount(ctx context.Context, admin *kadm.Client) (int, error) {
	brokers, err := admin.ListBrokers(ctx)
	if err != nil {
		return 0, fmt.Errorf("list brokers: %w", err)
	}

	return len(brokers), nil
}

// diff names everything two sides of one topic disagree on.
func diff(here details, there details) []string {
	differences := make([]string, 0, 2)

	if here.partitions != there.partitions {
		differences = append(differences, "partitions")
	}

	if here.replicas != there.replicas {
		differences = append(differences, "replication_factor")
	}

	// Every key set on either side is considered, so a config present on one
	// cluster and absent from the other counts as a difference rather than
	// being silently skipped.
	keys := make(map[string]struct{}, len(here.configs)+len(there.configs))

	for key := range here.configs {
		keys[key] = struct{}{}
	}

	for key := range there.configs {
		keys[key] = struct{}{}
	}

	differing := make([]string, 0, len(keys))

	for key := range keys {
		if here.configs[key] != there.configs[key] {
			differing = append(differing, key)
		}
	}

	// Go map order is random, and a report whose reasons reorder between calls
	// cannot be compared against the previous one.
	sort.Strings(differing)

	return append(differences, differing...)
}

func sortedNames(topics map[string]details) []string {
	names := make([]string, 0, len(topics))

	for name := range topics {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}
