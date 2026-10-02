// Package serde turns Kafka message bytes into something a caller can read,
// and a caller's JSON back into the bytes a consumer expects.
//
// It lives in internal/domain because every tool that reads or writes message
// content uses it, through records.Render and the produce and copy tools.
//
// A message is decoded by the first of these that applies:
//
//  1. The topic has a format fixed in topic_formats.
//  2. The bytes carry a Schema Registry header (a zero byte and a four-byte
//     schema id) and the cluster has a registry: Avro, Protobuf or JSON Schema.
//  3. Detection: JSON, then UTF-8 text, then base64 of the raw bytes.
//
// A decoding failure is never fatal and never silent. The bytes are returned as
// base64 and Decoded.Error says why, because a message nobody can read is still
// a message someone needs to see.
package serde

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
)

// Formats reported in Decoded.Format.
const (
	FormatNull       = "null"
	FormatJSON       = config.FormatJSON
	FormatText       = config.FormatText
	FormatBinary     = config.FormatBinary
	FormatAvro       = config.FormatAvro
	FormatProtobuf   = config.FormatProtobuf
	FormatMsgpack    = config.FormatMsgpack
	FormatJSONSchema = "json_schema"
)

// Part names which half of a record is being decoded. The two may differ: a
// string key over an Avro value is the usual case.
type Part string

const (
	Key   Part = "key"
	Value Part = "value"
)

// Decoded is one key or value rendered for a caller.
type Decoded struct {
	// Text is the rendering: JSON for every structured format, the string for
	// text, and base64 for binary.
	Text string

	Format string

	// SchemaID is the registry id the bytes named, or zero when they named none.
	SchemaID int

	// MessageType is the Avro record or Protobuf message the bytes hold.
	MessageType string

	// Document is the parsed JSON form of Text for structured formats, so a
	// caller that inspects fields does not parse the text a second time. Nil
	// for text, binary and null.
	Document any

	// Error says why bytes that looked decodable were not decoded.
	Error string
}

// Codec decodes and encodes messages for one cluster. It is safe for
// concurrent use; schemas are fetched once and cached.
type Codec struct {
	cluster  *config.Cluster
	registry *registry
	fixed    map[*config.PartFormat]fixedCodec
}

// New prepares a codec for a cluster, loading every schema topic_formats
// names, so a missing file or type stops the server at startup instead of
// failing on each message.
func New(cluster *config.Cluster) (*Codec, error) {
	codec := &Codec{cluster: cluster, fixed: map[*config.PartFormat]fixedCodec{}}

	if cluster != nil && cluster.SchemaRegistry != nil {
		registry, err := newRegistry(cluster.SchemaRegistry)
		if err != nil {
			return nil, fmt.Errorf("cluster %q schema registry: %w", cluster.Name, err)
		}

		codec.registry = registry
	}

	if cluster == nil {
		return codec, nil
	}

	for pattern, format := range cluster.TopicFormats {
		for part, settings := range map[Part]*config.PartFormat{Key: format.Key, Value: format.Value} {
			if settings == nil {
				continue
			}

			fixed, err := newFixed(settings)
			if err != nil {
				return nil, fmt.Errorf("cluster %q topic_formats %q %s: %w", cluster.Name, pattern, part, err)
			}

			codec.fixed[settings] = fixed
		}
	}

	return codec, nil
}

// HasRegistry reports whether the cluster has a schema registry configured.
func (c *Codec) HasRegistry() bool {
	return c != nil && c.registry != nil
}

// Decode renders one key or value of a record on topic.
func (c *Codec) Decode(ctx context.Context, topic string, part Part, data []byte) Decoded {
	if data == nil {
		return Decoded{Format: FormatNull}
	}

	if fixed := c.fixedFor(topic, part); fixed != nil {
		decoded, err := fixed.decode(data)
		if err != nil {
			return failed(data, fmt.Sprintf("does not decode as the configured %s format: %v", fixed.format(), err))
		}

		return decoded
	}

	if id, ok := framed(data); ok {
		if c == nil || c.registry == nil {
			return failed(data, fmt.Sprintf(
				"the bytes carry Schema Registry schema id %d, but this cluster has no schema_registry configured to decode them", id))
		}

		decoded, err := c.registry.decode(ctx, data)
		if err != nil {
			return failed(data, err.Error())
		}

		return decoded
	}

	return detect(data)
}

// fixedFor returns the configured codec for one part of a topic, or nil.
func (c *Codec) fixedFor(topic string, part Part) fixedCodec {
	if c == nil {
		return nil
	}

	format := c.cluster.FormatFor(topic)
	if format == nil {
		return nil
	}

	settings := format.Value
	if part == Key {
		settings = format.Key
	}

	if settings == nil {
		return nil
	}

	return c.fixed[settings]
}

// framed reports whether data starts with a Schema Registry header.
//
// A zero first byte followed by at least a four-byte id is the whole test,
// so text and JSON, which never start with a zero byte, are never mistaken for
// it.
func framed(data []byte) (int, bool) {
	if len(data) < 5 || data[0] != 0 {
		return 0, false
	}

	id := int(data[1])<<24 | int(data[2])<<16 | int(data[3])<<8 | int(data[4])

	return id, true
}

// detect renders bytes that carry no schema information.
func detect(data []byte) Decoded {
	trimmed := bytes.TrimSpace(data)

	if len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[') {
		var document any
		if err := json.Unmarshal(data, &document); err == nil {
			return Decoded{Text: string(data), Format: FormatJSON, Document: document}
		}
	}

	if utf8.Valid(data) {
		return Decoded{Text: string(data), Format: FormatText}
	}

	return Decoded{Text: base64.StdEncoding.EncodeToString(data), Format: FormatBinary}
}

// failed renders bytes that could not be decoded, carrying the reason.
func failed(data []byte, reason string) Decoded {
	decoded := Decoded{Text: base64.StdEncoding.EncodeToString(data), Format: FormatBinary, Error: reason}

	if id, ok := framed(data); ok {
		decoded.SchemaID = id
	}

	return decoded
}

// structured builds a Decoded from a decoded Go value, rendering it as JSON
// with sorted keys so two reads of one message are byte-identical.
func structured(format string, value any) (Decoded, error) {
	text, document, err := canonicalJSON(value)
	if err != nil {
		return Decoded{}, err
	}

	return Decoded{Text: text, Format: format, Document: document}, nil
}

// canonicalJSON renders JSON with sorted keys and numbers kept exact.
//
// Going through json.Number rather than float64 is what keeps a long beyond
// 2^53 intact: a float64 would round it, and an order id that reads back as a
// different number is worse than one that does not read at all.
func canonicalJSON(value any) (string, any, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", nil, err
	}

	return normalizeJSON(raw)
}

func normalizeJSON(raw []byte) (string, any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var document any
	if err := decoder.Decode(&document); err != nil {
		return "", nil, err
	}

	// encoding/json sorts map keys when marshalling, which is what makes the
	// text stable.
	text, err := json.Marshal(document)
	if err != nil {
		return "", nil, err
	}

	var plain any
	if err := json.Unmarshal(text, &plain); err != nil {
		return "", nil, err
	}

	return string(text), plain, nil
}
