// Package getschema implements the get_schema MCP tool.
package getschema

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
	"github.com/denizgursoy/kafka-mcp/internal/domain/serde"
)

// Item is one schema to look up, by subject or by id.
type Item struct {
	Subject string `json:"subject,omitempty" jsonschema:"Subject to look up, e.g. orders-value for the values of topic orders. Matched exactly and case-sensitively. Give subject or id, not both."`
	Version int    `json:"version,omitempty" jsonschema:"Optional version of subject. Defaults to the latest. Ignored with id."`
	ID      int    `json:"id,omitempty" jsonschema:"Schema id to look up, e.g. the schema_id get_message reported for a message. Give subject or id, not both."`

	CheckSchema *Candidate `json:"check_schema,omitempty" jsonschema:"Optional candidate schema to test against the subject's latest version under the subject's compatibility level, without registering it. Needs subject. Returns check.compatible and the registry's reasons in check.messages."`
}

// Candidate is a schema to test for compatibility.
type Candidate struct {
	Schema string `json:"schema" jsonschema:"The candidate schema text."`
	Type   string `json:"type,omitempty" jsonschema:"Optional: avro (default), protobuf or json_schema."`
}

// Check is the result of a compatibility test.
type Check struct {
	Compatible bool     `json:"compatible"`
	Messages   []string `json:"messages"`
}

// Input is the argument set accepted by the get_schema tool.
type Input struct {
	Items []Item `json:"items" jsonschema:"The schemas to look up, 1 to 100 of them. Looking up one schema is an array of length one. Results follow this order and a subject or id that does not exist is reported against its own item."`
}

// Reference is another schema this one depends on.
type Reference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// SubjectVersion is one subject version that uses a schema.
type SubjectVersion struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// Output is one schema.
type Output struct {
	SchemaID     int              `json:"schema_id"`
	Subject      string           `json:"subject,omitempty"`
	Version      int              `json:"version,omitempty"`
	Versions     []int            `json:"versions,omitempty"`
	Type         string           `json:"type"`
	Schema       string           `json:"schema"`
	References   []Reference      `json:"references"`
	MessageTypes []string         `json:"message_types,omitempty"`
	UsedBy       []SubjectVersion `json:"used_by,omitempty"`

	// Compatibility is the level in force for the subject, inherited from the
	// registry's global level when the subject sets none. Reported for a
	// subject lookup only.
	Compatibility string `json:"compatibility,omitempty"`
	Check         *Check `json:"check,omitempty"`
}

type BatchOutput = batch.Output[Output]

const description = `
Read 1 to 100 schemas from this cluster's Schema Registry in one call through
items, each by subject (latest version unless version is given) or by the
schema id a message carries. Returns the schema text, its type (avro, protobuf
or json_schema), id, version, every version of the subject, the schemas it
references, and for an id the subject versions that use it. A subject lookup
also reports the compatibility level in force. check_schema tests a candidate
schema against the subject's latest version without registering it, returning
whether it is compatible and the registry's reasons if not.

Read the schema before producing to a schema-encoded topic: it names every
field and enum a value needs, which a sampled message may not show. For
Protobuf, message_types lists the names produce_message accepts as
message_type. The subject for a topic's values is usually <topic>-value.

Results follow items order, each carrying index with result or error. Fails as
a whole when the cluster has no schema_registry configured.
`

// Register adds the get_schema tool to the MCP server.
func Register(server *mcp.Server, codec *serde.Codec) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "get_schema",
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, BatchOutput, error) {

			out, err := Run(ctx, codec, input)
			if err != nil {
				return nil, BatchOutput{}, fmt.Errorf("get schema: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run looks up every requested schema with bounded concurrency.
func Run(ctx context.Context, codec *serde.Codec, input Input) (BatchOutput, error) {
	// Every item would fail the same way, so the missing registry is the
	// call's error rather than repeated on each item.
	if !codec.HasRegistry() {
		return BatchOutput{}, fmt.Errorf(
			"this cluster has no schema_registry configured, so there are no schemas to read")
	}

	return batch.Run(ctx, input.Items, batch.MaxItems, func(ctx context.Context, item Item) (Output, error) {
		return get(ctx, codec, item)
	})
}

func get(ctx context.Context, codec *serde.Codec, item Item) (Output, error) {
	switch {
	case item.Subject == "" && item.ID == 0:
		return Output{}, fmt.Errorf("give a subject or an id to look up")
	case item.Subject != "" && item.ID != 0:
		return Output{}, fmt.Errorf("give either subject or id, not both: they may name different schemas")
	case item.ID < 0:
		return Output{}, fmt.Errorf("id must be positive, got %d", item.ID)
	case item.CheckSchema != nil && item.Subject == "":
		return Output{}, fmt.Errorf(
			"check_schema needs a subject: compatibility is judged against one subject's history, and an id may be used by several")
	case item.CheckSchema != nil && item.CheckSchema.Schema == "":
		return Output{}, fmt.Errorf("check_schema.schema is empty")
	case item.Version < 0:
		return Output{}, fmt.Errorf("version must be positive, or omitted for the latest, got %d", item.Version)
	}

	var (
		found serde.RegistrySchema
		err   error
	)

	if item.ID != 0 {
		found, err = codec.LookupID(ctx, item.ID)
	} else {
		found, err = codec.LookupSubject(ctx, item.Subject, item.Version)
	}

	if err != nil {
		return Output{}, err
	}

	out := Output{
		SchemaID:     found.ID,
		Subject:      found.Subject,
		Version:      found.Version,
		Versions:     found.Versions,
		Type:         found.Type,
		Schema:       found.Schema,
		References:   []Reference{},
		MessageTypes: found.MessageTypes,
	}

	for _, reference := range found.References {
		out.References = append(out.References, Reference{
			Name:    reference.Name,
			Subject: reference.Subject,
			Version: reference.Version,
		})
	}

	for _, use := range found.UsedBy {
		out.UsedBy = append(out.UsedBy, SubjectVersion{Subject: use.Subject, Version: use.Version})
	}

	if item.Subject != "" {
		level, err := codec.CompatibilityLevel(ctx, item.Subject)
		if err != nil {
			return Output{}, err
		}
		out.Compatibility = level
	}

	if item.CheckSchema != nil {
		kind := item.CheckSchema.Type
		if kind == "" {
			kind = found.Type
		}

		compatible, messages, err := codec.CheckCompatibility(ctx, item.Subject, item.CheckSchema.Schema, kind)
		if err != nil {
			return Output{}, err
		}
		out.Check = &Check{Compatible: compatible, Messages: messages}
	}

	return out, nil
}
