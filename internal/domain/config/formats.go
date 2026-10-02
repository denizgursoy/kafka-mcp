package config

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
)

// Formats a topic can be fixed to in topic_formats.
const (
	FormatAvro     = "avro"
	FormatProtobuf = "protobuf"
	FormatJSON     = "json"
	FormatMsgpack  = "msgpack"
	FormatText     = "text"
	FormatBinary   = "binary"
)

// SchemaRegistry is a Confluent-compatible schema registry, used to decode
// messages that carry a schema id and to encode messages produced against a
// registered schema.
type SchemaRegistry struct {
	URLs []string `cfg:"urls"`
	TLS  *TLS     `cfg:"tls"`

	// User and Password are HTTP basic authentication. Password accepts
	// {env:VAR} like the broker password.
	User         string `cfg:"user"`
	Password     string `cfg:"password" log:"false"`
	PasswordFile string `cfg:"password_file"`

	// BearerToken is the alternative to basic authentication.
	BearerToken string `cfg:"bearer_token" log:"false"`
}

// TopicFormat fixes how a topic's key and value are decoded, for topics whose
// messages carry no schema id. A part left nil is detected per message.
type TopicFormat struct {
	Key   *PartFormat `cfg:"key"`
	Value *PartFormat `cfg:"value"`
}

// PartFormat is the format of one part of a message, the key or the value.
type PartFormat struct {
	Format string `cfg:"format"`

	// SchemaFile is an Avro schema (.avsc) for format avro.
	SchemaFile string `cfg:"schema_file"`

	// ProtoFiles and ImportPaths locate .proto sources for format protobuf.
	// DescriptorSet is a compiled FileDescriptorSet (buf build -o, protoc
	// --descriptor_set_out) and replaces them.
	ProtoFiles    []string `cfg:"proto_files"`
	ImportPaths   []string `cfg:"import_paths"`
	DescriptorSet string   `cfg:"descriptor_set"`

	// MessageType is the fully qualified protobuf message, e.g. shop.Order.
	MessageType string `cfg:"message_type"`
}

// FormatFor returns the fixed format of a topic, or nil when none is
// configured. An exact name wins over a pattern, and among patterns the
// longest wins, so a specific rule can override a broad one; ties are broken
// alphabetically so the choice never depends on map order.
func (c *Cluster) FormatFor(topic string) *TopicFormat {
	if c == nil || len(c.TopicFormats) == 0 {
		return nil
	}

	if exact, ok := c.TopicFormats[topic]; ok {
		return exact
	}

	patterns := make([]string, 0, len(c.TopicFormats))

	for pattern := range c.TopicFormats {
		if matched, _ := path.Match(pattern, topic); matched {
			patterns = append(patterns, pattern)
		}
	}

	if len(patterns) == 0 {
		return nil
	}

	sort.Slice(patterns, func(i, j int) bool {
		if len(patterns[i]) != len(patterns[j]) {
			return len(patterns[i]) > len(patterns[j])
		}

		return patterns[i] < patterns[j]
	})

	return c.TopicFormats[patterns[0]]
}

func resolveSchemaRegistry(name string, registry *SchemaRegistry) (*SchemaRegistry, error) {
	if registry == nil {
		return nil, nil
	}

	urls := make([]string, 0, len(registry.URLs))

	for _, url := range registry.URLs {
		if trimmed := strings.TrimSpace(url); trimmed != "" {
			urls = append(urls, trimmed)
		}
	}

	if len(urls) == 0 {
		return nil, fmt.Errorf("config: cluster %q schema_registry needs at least one url in urls", name)
	}

	resolved := *registry
	resolved.URLs = urls

	if resolved.TLS != nil && resolved.TLS.Enabled &&
		(resolved.TLS.CertFile == "") != (resolved.TLS.KeyFile == "") {
		return nil, fmt.Errorf(
			"config: cluster %q schema_registry tls cert_file and key_file must be supplied together", name)
	}

	token, err := interpolate(resolved.BearerToken, name)
	if err != nil {
		return nil, err
	}

	resolved.BearerToken = token

	if resolved.User == "" {
		if resolved.Password != "" || resolved.PasswordFile != "" {
			return nil, fmt.Errorf("config: cluster %q schema_registry has a password but no user", name)
		}

		return &resolved, nil
	}

	if resolved.BearerToken != "" {
		return nil, fmt.Errorf(
			"config: cluster %q schema_registry sets both user and bearer_token; choose one", name)
	}

	password, err := resolveRegistryPassword(name, &resolved)
	if err != nil {
		return nil, err
	}

	if password == "" {
		return nil, fmt.Errorf(
			"config: cluster %q schema_registry user %q has no password: set password, password_file, or use {env:VAR}",
			name, resolved.User)
	}

	resolved.Password = password
	resolved.PasswordFile = ""

	return &resolved, nil
}

func resolveRegistryPassword(name string, registry *SchemaRegistry) (string, error) {
	if registry.PasswordFile == "" {
		return interpolate(registry.Password, name)
	}

	if registry.Password != "" {
		return "", fmt.Errorf(
			"config: cluster %q schema_registry sets both password and password_file, which is ambiguous", name)
	}

	contents, err := os.ReadFile(registry.PasswordFile)
	if err != nil {
		return "", fmt.Errorf("config: cluster %q schema_registry read password_file: %w", name, err)
	}

	return strings.TrimSpace(string(contents)), nil
}

func resolveTopicFormats(name string, formats map[string]*TopicFormat) (map[string]*TopicFormat, error) {
	if len(formats) == 0 {
		return nil, nil
	}

	resolved := make(map[string]*TopicFormat, len(formats))

	for pattern, format := range formats {
		if _, err := path.Match(pattern, ""); err != nil {
			return nil, fmt.Errorf("config: cluster %q topic_formats pattern %q is invalid: %w", name, pattern, err)
		}

		if format == nil || (format.Key == nil && format.Value == nil) {
			return nil, fmt.Errorf(
				"config: cluster %q topic_formats %q sets neither key nor value", name, pattern)
		}

		for part, settings := range map[string]*PartFormat{"key": format.Key, "value": format.Value} {
			if settings == nil {
				continue
			}

			if err := validatePart(settings); err != nil {
				return nil, fmt.Errorf("config: cluster %q topic_formats %q %s: %w", name, pattern, part, err)
			}
		}

		resolved[pattern] = format
	}

	return resolved, nil
}

func validatePart(part *PartFormat) error {
	part.Format = strings.ToLower(strings.TrimSpace(part.Format))

	switch part.Format {
	case FormatAvro:
		if part.SchemaFile == "" {
			return fmt.Errorf("format avro needs schema_file, the .avsc the data was written with")
		}

	case FormatProtobuf:
		if part.MessageType == "" {
			return fmt.Errorf("format protobuf needs message_type, the fully qualified message name")
		}

		if len(part.ProtoFiles) == 0 && part.DescriptorSet == "" {
			return fmt.Errorf("format protobuf needs proto_files or descriptor_set to find %s in", part.MessageType)
		}

		if len(part.ProtoFiles) > 0 && part.DescriptorSet != "" {
			return fmt.Errorf("format protobuf takes proto_files or descriptor_set, not both")
		}

	case FormatJSON, FormatMsgpack, FormatText, FormatBinary:

	default:
		return fmt.Errorf("unknown format %q: use one of %s, %s, %s, %s, %s or %s", part.Format,
			FormatAvro, FormatProtobuf, FormatJSON, FormatMsgpack, FormatText, FormatBinary)
	}

	return nil
}

// describeFormats renders topic_formats for reporting, sorted by pattern.
func describeFormats(formats map[string]*TopicFormat) []map[string]any {
	patterns := make([]string, 0, len(formats))
	for pattern := range formats {
		patterns = append(patterns, pattern)
	}

	sort.Strings(patterns)

	described := make([]map[string]any, 0, len(patterns))

	for _, pattern := range patterns {
		entry := map[string]any{"topic": pattern}

		if format := formats[pattern]; format.Key != nil {
			entry["key"] = describePart(format.Key)
		}

		if format := formats[pattern]; format.Value != nil {
			entry["value"] = describePart(format.Value)
		}

		described = append(described, entry)
	}

	return described
}

func describePart(part *PartFormat) map[string]any {
	described := map[string]any{"format": part.Format}

	if part.MessageType != "" {
		described["message_type"] = part.MessageType
	}

	return described
}
