// Package topicconfig reads and renders Kafka topic configuration.
//
// It lives in internal/domain because two tools need it: describe_topic reports
// a topic's whole configuration, and compare_clusters decides whether two
// topics are configured the same way. Both turn on the same distinction, so it
// is defined once here — which value a topic sets deliberately, and which it
// merely inherits.
package topicconfig

import (
	"context"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Entry is one topic-level configuration value.
type Entry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	// Source names where the value comes from, so a caller can tell a
	// deliberate topic setting from an inherited cluster default.
	Source    string `json:"source"`
	IsDefault bool   `json:"is_default"`
	// Sensitive marks a config whose value the broker refuses to disclose.
	// The value is then empty because it is hidden, not because it is unset.
	Sensitive bool `json:"sensitive,omitempty"`
}

// sources names the ConfigSource values Kafka can report. The protocol sends a
// bare integer, which tells a caller nothing, so it is mapped to the name used
// in Kafka's own documentation and tooling.
var sources = map[kmsg.ConfigSource]string{
	kmsg.ConfigSourceDynamicTopicConfig:         "DYNAMIC_TOPIC_CONFIG",
	kmsg.ConfigSourceDynamicBrokerConfig:        "DYNAMIC_BROKER_CONFIG",
	kmsg.ConfigSourceDynamicDefaultBrokerConfig: "DYNAMIC_DEFAULT_BROKER_CONFIG",
	kmsg.ConfigSourceStaticBrokerConfig:         "STATIC_BROKER_CONFIG",
	kmsg.ConfigSourceDefaultConfig:              "DEFAULT_CONFIG",
	kmsg.ConfigSourceDynamicBrokerLoggerConfig:  "DYNAMIC_BROKER_LOGGER_CONFIG",
}

// For returns every configuration entry of one topic, sorted by key.
func For(ctx context.Context, admin *kadm.Client, topic string) ([]Entry, error) {
	byTopic, err := ForTopics(ctx, admin, topic)
	if err != nil {
		return nil, err
	}

	entries, ok := byTopic[topic]
	if !ok {
		return nil, fmt.Errorf("describe configs for %q: the broker returned no configuration", topic)
	}

	return entries, nil
}

// ForTopics returns the configuration of several topics in one request.
//
// Comparing two clusters asks for every shared topic at once, and one request
// per topic would make the call cost grow with the size of the cluster rather
// than with the work.
func ForTopics(
	ctx context.Context,
	admin *kadm.Client,
	topics ...string,
) (map[string][]Entry, error) {

	if len(topics) == 0 {
		return map[string][]Entry{}, nil
	}

	described, err := admin.DescribeTopicConfigs(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("describe configs: %w", err)
	}

	byTopic := make(map[string][]Entry, len(described))

	for _, resource := range described {
		// A per-resource error would otherwise surface as a topic with no
		// configuration at all, which reads as "nothing is configured" rather
		// than "the configuration could not be read".
		if resource.Err != nil {
			if resource.ErrMessage != "" {
				return nil, fmt.Errorf(
					"describe configs for %q: %w: %s", resource.Name, resource.Err, resource.ErrMessage)
			}

			return nil, fmt.Errorf("describe configs for %q: %w", resource.Name, resource.Err)
		}

		byTopic[resource.Name] = render(resource.Configs)
	}

	return byTopic, nil
}

func render(configs []kadm.Config) []Entry {
	entries := make([]Entry, 0, len(configs))

	for _, config := range configs {
		source, known := sources[config.Source]
		if !known {
			// Naming an unrecognised source honestly beats reporting one that
			// the broker did not send.
			source = fmt.Sprintf("UNKNOWN(%d)", config.Source)
		}

		entries = append(entries, Entry{
			Key:   config.Key,
			Value: config.MaybeValue(),
			// Only a value set on the topic itself is a deliberate choice.
			// Everything else is inherited, whatever level it comes from.
			IsDefault: config.Source != kmsg.ConfigSourceDynamicTopicConfig,
			Source:    source,
			Sensitive: config.Sensitive,
		})
	}

	// kadm returns configs in no guaranteed order, so sort for a stable report.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	return entries
}

// Explicit reduces entries to the values a topic sets for itself.
//
// Comparing two clusters must compare only these. Two brokers may carry
// different defaults for the same key, and comparing inherited values would
// report every topic as different — noise that hides the settings somebody
// actually chose.
func Explicit(entries []Entry) map[string]string {
	explicit := make(map[string]string, len(entries))

	for _, entry := range entries {
		if entry.IsDefault {
			continue
		}

		// A sensitive value is withheld by the broker, so its emptiness says
		// nothing about whether the two sides agree. Recording the key without
		// a value keeps "set here, unset there" visible while never claiming
		// the values match.
		explicit[entry.Key] = entry.Value
	}

	return explicit
}
