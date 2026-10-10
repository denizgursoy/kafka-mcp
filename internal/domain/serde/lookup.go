package serde

import (
	"context"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// RegistrySchema is a schema as the registry holds it, with what a caller
// needs to write a message against it.
type RegistrySchema struct {
	ID           int
	Subject      string
	Version      int
	Type         string
	Schema       string
	References   []sr.SchemaReference
	Versions     []int
	UsedBy       []sr.SubjectVersion
	MessageTypes []string
}

// LookupSubject returns a subject's schema at version, or the latest when
// version is zero, together with every version the subject has.
func (c *Codec) LookupSubject(ctx context.Context, subject string, version int) (RegistrySchema, error) {
	if !c.HasRegistry() {
		return RegistrySchema{}, noRegistry()
	}

	ctx, cancel := context.WithTimeout(ctx, registryTimeout)
	defer cancel()

	wanted := version
	if wanted == 0 {
		wanted = -1
	}

	found, err := c.registry.client.SchemaByVersion(ctx, subject, wanted)
	if err != nil {
		return RegistrySchema{}, fmt.Errorf("subject %q version %s: %w", subject, versionName(version), registryError(err))
	}

	versions, err := c.registry.client.SubjectVersions(ctx, subject)
	if err != nil {
		return RegistrySchema{}, fmt.Errorf("list versions of subject %q: %w", subject, registryError(err))
	}

	sort.Ints(versions)

	described := RegistrySchema{
		ID:         found.ID,
		Subject:    found.Subject,
		Version:    found.Version,
		Type:       kindName(found.Type),
		Schema:     found.Schema.Schema,
		References: found.References,
		Versions:   versions,
	}

	described.MessageTypes = c.messageTypes(ctx, found.ID)

	return described, nil
}

// LookupID returns the schema an id names and every subject version that
// uses it.
func (c *Codec) LookupID(ctx context.Context, id int) (RegistrySchema, error) {
	if !c.HasRegistry() {
		return RegistrySchema{}, noRegistry()
	}

	ctx, cancel := context.WithTimeout(ctx, registryTimeout)
	defer cancel()

	found, err := c.registry.client.SchemaByID(ctx, id)
	if err != nil {
		return RegistrySchema{}, fmt.Errorf("schema id %d: %w", id, registryError(err))
	}

	usedBy, err := c.registry.client.SchemaVersionsByID(ctx, id)
	if err != nil {
		return RegistrySchema{}, fmt.Errorf("list subjects using schema id %d: %w", id, registryError(err))
	}

	sort.Slice(usedBy, func(i, j int) bool {
		if usedBy[i].Subject != usedBy[j].Subject {
			return usedBy[i].Subject < usedBy[j].Subject
		}

		return usedBy[i].Version < usedBy[j].Version
	})

	return RegistrySchema{
		ID:           id,
		Type:         kindName(found.Type),
		Schema:       found.Schema,
		References:   found.References,
		UsedBy:       usedBy,
		MessageTypes: c.messageTypes(ctx, id),
	}, nil
}

// messageTypes lists every Protobuf message a schema defines, nested ones
// included, which are the names produce_message accepts as message_type. It is
// nil for other schema types, and for a schema that does not compile, which is
// not a reason to withhold its text.
func (c *Codec) messageTypes(ctx context.Context, id int) []string {
	compiled, err := c.registry.schema(ctx, id)
	if err != nil || compiled.proto == nil {
		return nil
	}

	var names []string

	var walk func(messages protoreflect.MessageDescriptors)
	walk = func(messages protoreflect.MessageDescriptors) {
		for i := 0; i < messages.Len(); i++ {
			names = append(names, string(messages.Get(i).FullName()))
			walk(messages.Get(i).Messages())
		}
	}

	walk(compiled.proto.Messages())

	return names
}

func noRegistry() error {
	return fmt.Errorf("this cluster has no schema_registry configured, so there are no schemas to look up")
}

// CompatibilityLevel returns the level in force for a subject, falling back to
// the registry's global level when the subject sets none.
func (c *Codec) CompatibilityLevel(ctx context.Context, subject string) (string, error) {
	if !c.HasRegistry() {
		return "", noRegistry()
	}

	ctx, cancel := context.WithTimeout(ctx, registryTimeout)
	defer cancel()

	results := c.registry.client.Compatibility(sr.WithParams(ctx, sr.DefaultToGlobal), subject)
	if len(results) == 0 {
		return "", fmt.Errorf("compatibility of subject %q: the registry returned nothing", subject)
	}

	if results[0].Err != nil {
		return "", fmt.Errorf("compatibility of subject %q: %w", subject, registryError(results[0].Err))
	}

	return results[0].Level.String(), nil
}

// CheckCompatibility asks the registry whether candidate could be registered
// under subject, without registering it. kind is avro, protobuf or
// json_schema; empty means the subject's current type.
func (c *Codec) CheckCompatibility(ctx context.Context, subject string, candidate string, kind string) (bool, []string, error) {
	if !c.HasRegistry() {
		return false, nil, noRegistry()
	}

	schemaType, err := schemaKind(kind)
	if err != nil {
		return false, nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, registryTimeout)
	defer cancel()

	result, err := c.registry.client.CheckCompatibility(sr.WithParams(ctx, sr.Verbose), subject, -1,
		sr.Schema{Schema: candidate, Type: schemaType})
	if err != nil {
		return false, nil, fmt.Errorf("check compatibility against subject %q: %w", subject, registryError(err))
	}

	messages := result.Messages
	if messages == nil {
		messages = []string{}
	}

	return result.Is, messages, nil
}

func schemaKind(kind string) (sr.SchemaType, error) {
	switch kind {
	case "", FormatAvro:
		return sr.TypeAvro, nil
	case FormatProtobuf:
		return sr.TypeProtobuf, nil
	case FormatJSONSchema:
		return sr.TypeJSON, nil
	}

	return 0, fmt.Errorf("type must be avro, protobuf or json_schema, got %q", kind)
}
