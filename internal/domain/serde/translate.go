package serde

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/sr"
)

// SchemaID returns the registry id a framed message names, and false when the
// bytes carry no Schema Registry header.
func SchemaID(data []byte) (int, bool) {
	return framed(data)
}

// Translate rewrites the schema id in a framed message so it is valid in the
// destination's registry: the schema behind the id is read from the source
// registry, registered in the destination under subject with every schema it
// references, and the id the destination assigns replaces the original. The
// payload is not touched, so the message reads the same on both sides.
//
// Registering is a write to the destination's registry; the caller must have
// checked the destination may be changed.
func Translate(ctx context.Context, source, destination *Codec, subject string, data []byte) ([]byte, Encoded, error) {
	id, ok := framed(data)
	if !ok {
		return nil, Encoded{}, fmt.Errorf("the bytes carry no schema registry header")
	}

	if !source.HasRegistry() {
		return nil, Encoded{}, fmt.Errorf(
			"the source cluster has no schema_registry configured, so schema id %d cannot be looked up", id)
	}

	if !destination.HasRegistry() {
		return nil, Encoded{}, fmt.Errorf(
			"the destination cluster has no schema_registry configured to register schema id %d in", id)
	}

	ctx, cancel := context.WithTimeout(ctx, registryTimeout)
	defer cancel()

	schema, err := source.registry.client.SchemaByID(ctx, id)
	if err != nil {
		return nil, Encoded{}, fmt.Errorf("fetch schema id %d from the source registry: %w", id, registryError(err))
	}

	references, err := copyReferences(ctx, source.registry.client, destination.registry.client, schema.References)
	if err != nil {
		return nil, Encoded{}, err
	}

	schema.References = references

	registered, err := destination.registry.client.CreateSchema(ctx, subject, schema)
	if err != nil {
		return nil, Encoded{}, fmt.Errorf("register schema id %d under %q in the destination registry: %w",
			id, subject, registryError(err))
	}

	translated := append([]byte(nil), data...)

	var header sr.ConfluentHeader
	if err := header.UpdateID(translated, uint32(registered.ID)); err != nil {
		return nil, Encoded{}, err
	}

	return translated, Encoded{
		Format:   kindName(schema.Type),
		SchemaID: registered.ID,
		Subject:  subject,
		Version:  registered.Version,
	}, nil
}

// copyReferences registers every referenced schema in the destination under
// its own subject, depth first, and returns the references rewritten to the
// versions the destination assigned.
func copyReferences(ctx context.Context, source, destination *sr.Client, references []sr.SchemaReference) ([]sr.SchemaReference, error) {
	copied := make([]sr.SchemaReference, 0, len(references))

	for _, reference := range references {
		referenced, err := source.SchemaByVersion(ctx, reference.Subject, reference.Version)
		if err != nil {
			return nil, fmt.Errorf("fetch referenced schema %q version %d from the source registry: %w",
				reference.Subject, reference.Version, registryError(err))
		}

		nested, err := copyReferences(ctx, source, destination, referenced.References)
		if err != nil {
			return nil, err
		}

		referenced.Schema.References = nested

		registered, err := destination.CreateSchema(ctx, reference.Subject, referenced.Schema)
		if err != nil {
			return nil, fmt.Errorf("register referenced schema %q in the destination registry: %w",
				reference.Subject, registryError(err))
		}

		copied = append(copied, sr.SchemaReference{
			Name:    reference.Name,
			Subject: reference.Subject,
			Version: registered.Version,
		})
	}

	return copied, nil
}

// SameRegistry reports whether two codecs talk to the same registry, in which
// case a schema id means the same thing on both sides.
func SameRegistry(a, b *Codec) bool {
	if !a.HasRegistry() || !b.HasRegistry() {
		return false
	}

	if a.registry == b.registry {
		return true
	}

	left, right := a.cluster.SchemaRegistry.URLs, b.cluster.SchemaRegistry.URLs
	if len(left) != len(right) {
		return false
	}

	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}

	return true
}
