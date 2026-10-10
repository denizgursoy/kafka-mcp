package serde

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/twmb/avro"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/tlscfg"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
)

// registryTimeout bounds one registry request. A registry that hangs must not
// hold a message read for the length of the whole tool call.
const registryTimeout = 10 * time.Second

// maxIndexDepth bounds the protobuf message index read from a header. Real
// schemas nest a handful of levels; a huge count means the bytes are not a
// header at all.
const maxIndexDepth = 64

// registry decodes and encodes through a Schema Registry, caching every schema
// it fetches by id. Ids are immutable in a registry, so the cache never needs
// invalidating.
type registry struct {
	client *sr.Client

	mu      sync.Mutex
	schemas map[int]*compiled
}

// compiled is one registry schema, parsed once and reused for every message
// that names it.
type compiled struct {
	id     int
	kind   sr.SchemaType
	avro   *avro.Schema
	proto  protoreflect.FileDescriptor
	json   *jsonschema.Resolved
	record string
}

func newRegistry(settings *config.SchemaRegistry) (*registry, error) {
	options := []sr.ClientOpt{sr.URLs(settings.URLs...)}

	httpClient := &http.Client{Timeout: registryTimeout}

	if settings.TLS != nil && settings.TLS.Enabled {
		tlsConfig, err := registryTLS(settings.TLS)
		if err != nil {
			return nil, err
		}

		httpClient.Transport = &http.Transport{TLSClientConfig: tlsConfig}
	}

	options = append(options, sr.HTTPClient(httpClient))

	switch {
	case settings.User != "":
		options = append(options, sr.BasicAuth(settings.User, settings.Password))
	case settings.BearerToken != "":
		options = append(options, sr.BearerToken(settings.BearerToken))
	}

	client, err := sr.NewClient(options...)
	if err != nil {
		return nil, err
	}

	return &registry{client: client, schemas: map[int]*compiled{}}, nil
}

func registryTLS(settings *config.TLS) (*tls.Config, error) {
	cfg, err := tlscfg.New(
		tlscfg.MaybeWithDiskKeyPair(settings.CertFile, settings.KeyFile),
		tlscfg.MaybeWithDiskCA(settings.CAFile, tlscfg.ForClient),
		tlscfg.WithSystemCertPool(),
	)
	if err != nil {
		return nil, fmt.Errorf("configure tls: %w", err)
	}
	cfg.InsecureSkipVerify = settings.InsecureSkipVerify

	return cfg, nil
}

// decode reads a framed message: header, optional protobuf index, payload.
func (r *registry) decode(ctx context.Context, data []byte) (Decoded, error) {
	var header sr.ConfluentHeader

	id, payload, err := header.DecodeID(data)
	if err != nil {
		return Decoded{}, fmt.Errorf("read the schema registry header: %w", err)
	}

	schema, err := r.schema(ctx, id)
	if err != nil {
		return Decoded{}, err
	}

	var decoded Decoded

	switch schema.kind {
	case sr.TypeAvro:
		decoded, err = decodeAvro(schema.avro, payload)
		decoded.MessageType = schema.record

	case sr.TypeProtobuf:
		var index []int

		index, payload, err = header.DecodeIndex(payload, maxIndexDepth)
		if err != nil {
			return Decoded{}, fmt.Errorf("schema id %d: read the protobuf message index: %w", id, err)
		}

		var message protoreflect.MessageDescriptor

		message, err = messageAt(schema.proto, index)
		if err != nil {
			return Decoded{}, fmt.Errorf("schema id %d: %w", id, err)
		}

		decoded, err = decodeProto(message, payload)
		decoded.MessageType = string(message.FullName())

	case sr.TypeJSON:
		decoded, err = decodeJSONPayload(payload)
		decoded.Format = FormatJSONSchema
	}

	if err != nil {
		return Decoded{}, fmt.Errorf("schema id %d (%s): %w", id, kindName(schema.kind), err)
	}

	decoded.SchemaID = id

	return decoded, nil
}

// Target names the registry schema to encode against. Every field is
// optional: ID wins when set; otherwise Subject (defaulting to
// <topic>-<part>) at Version (defaulting to the latest). MessageType picks
// the protobuf message and defaults to the first in the file.
type Target struct {
	Subject     string
	Version     int
	ID          int
	MessageType string
}

// Encoded reports what an encode used, so a caller can show it before writing.
type Encoded struct {
	Format      string `json:"format"`
	SchemaID    int    `json:"schema_id,omitempty"`
	Subject     string `json:"subject,omitempty"`
	Version     int    `json:"version,omitempty"`
	MessageType string `json:"message_type,omitempty"`
}

// encode turns a JSON document into a framed message for the target schema.
func (r *registry) encode(ctx context.Context, topic string, part Part, document string, target Target) ([]byte, Encoded, error) {
	encoded := Encoded{SchemaID: target.ID, Subject: target.Subject, Version: target.Version}

	if encoded.SchemaID == 0 {
		if encoded.Subject == "" {
			encoded.Subject = topic + "-" + string(part)
		}

		version := target.Version
		if version == 0 {
			version = -1
		}

		lookupCtx, cancel := context.WithTimeout(ctx, registryTimeout)
		defer cancel()

		subject, err := r.client.SchemaByVersion(lookupCtx, encoded.Subject, version)
		if err != nil {
			return nil, Encoded{}, fmt.Errorf("look up subject %q version %s: %w",
				encoded.Subject, versionName(target.Version), registryError(err))
		}

		encoded.SchemaID = subject.ID
		encoded.Version = subject.Version
	}

	schema, err := r.schema(ctx, encoded.SchemaID)
	if err != nil {
		return nil, Encoded{}, err
	}

	var (
		header  sr.ConfluentHeader
		index   []int
		payload []byte
	)

	switch schema.kind {
	case sr.TypeAvro:
		encoded.Format = FormatAvro
		encoded.MessageType = schema.record

		payload, err = encodeAvro(schema.avro, document)

	case sr.TypeProtobuf:
		encoded.Format = FormatProtobuf

		var message protoreflect.MessageDescriptor

		message, index, err = messageNamed(schema.proto, target.MessageType)
		if err != nil {
			return nil, Encoded{}, fmt.Errorf("schema id %d: %w", encoded.SchemaID, err)
		}

		encoded.MessageType = string(message.FullName())

		payload, err = encodeProto(message, document)

	case sr.TypeJSON:
		encoded.Format = FormatJSONSchema

		payload, err = encodeJSONSchema(schema.json, document)
	}

	if err != nil {
		return nil, Encoded{}, fmt.Errorf("value does not fit schema id %d (%s): %w", encoded.SchemaID, kindName(schema.kind), err)
	}

	framed, err := header.AppendEncode(nil, encoded.SchemaID, index)
	if err != nil {
		return nil, Encoded{}, err
	}

	return append(framed, payload...), encoded, nil
}

// schema returns the compiled schema for an id, fetching it and every schema
// it references on first use.
func (r *registry) schema(ctx context.Context, id int) (*compiled, error) {
	r.mu.Lock()
	cached, ok := r.schemas[id]
	r.mu.Unlock()

	if ok {
		return cached, nil
	}

	ctx, cancel := context.WithTimeout(ctx, registryTimeout)
	defer cancel()

	schema, err := r.client.SchemaByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("fetch schema id %d from the schema registry: %w", id, registryError(err))
	}

	built, err := r.compile(ctx, id, schema)
	if err != nil {
		return nil, fmt.Errorf("schema id %d: %w", id, err)
	}

	r.mu.Lock()
	r.schemas[id] = built
	r.mu.Unlock()

	return built, nil
}

func (r *registry) compile(ctx context.Context, id int, schema sr.Schema) (*compiled, error) {
	built := &compiled{id: id, kind: schema.Type}

	switch schema.Type {
	case sr.TypeAvro:
		var cache avro.SchemaCache

		// References must be parsed before the schema that uses them, so
		// named types they define are known when the main schema is parsed.
		if err := r.parseAvroReferences(ctx, &cache, schema.References, map[string]bool{}); err != nil {
			return nil, err
		}

		parsed, err := cache.Parse(schema.Schema)
		if err != nil {
			return nil, fmt.Errorf("parse avro schema: %w", err)
		}

		built.avro = parsed
		built.record = avroName(parsed)

	case sr.TypeProtobuf:
		sources := map[string]string{}
		if err := r.collectProtoReferences(ctx, sources, schema.References); err != nil {
			return nil, err
		}

		const main = "__registry_schema__.proto"
		sources[main] = schema.Schema

		file, err := compileProtoSources(sources, main)
		if err != nil {
			return nil, err
		}

		built.proto = file

	case sr.TypeJSON:
		resolved, err := compileJSONSchema(schema.Schema)
		if err != nil {
			return nil, err
		}

		built.json = resolved

	default:
		return nil, fmt.Errorf("unsupported schema type %v", schema.Type)
	}

	return built, nil
}

func (r *registry) parseAvroReferences(ctx context.Context, cache *avro.SchemaCache, references []sr.SchemaReference, seen map[string]bool) error {
	for _, reference := range references {
		key := fmt.Sprintf("%s/%d", reference.Subject, reference.Version)
		if seen[key] {
			continue
		}

		seen[key] = true

		referenced, err := r.client.SchemaByVersion(ctx, reference.Subject, reference.Version)
		if err != nil {
			return fmt.Errorf("fetch referenced schema %q version %d: %w", reference.Subject, reference.Version, registryError(err))
		}

		if err := r.parseAvroReferences(ctx, cache, referenced.References, seen); err != nil {
			return err
		}

		if _, err := cache.Parse(referenced.Schema.Schema); err != nil {
			return fmt.Errorf("parse referenced avro schema %q: %w", reference.Name, err)
		}
	}

	return nil
}

// collectProtoReferences gathers every imported file's source, keyed by the
// import path the referencing file uses.
func (r *registry) collectProtoReferences(ctx context.Context, sources map[string]string, references []sr.SchemaReference) error {
	for _, reference := range references {
		if _, done := sources[reference.Name]; done {
			continue
		}

		referenced, err := r.client.SchemaByVersion(ctx, reference.Subject, reference.Version)
		if err != nil {
			return fmt.Errorf("fetch referenced schema %q version %d: %w", reference.Subject, reference.Version, registryError(err))
		}

		sources[reference.Name] = referenced.Schema.Schema

		if err := r.collectProtoReferences(ctx, sources, referenced.References); err != nil {
			return err
		}
	}

	return nil
}

// registryError makes a registry failure readable: the registry's own message
// rather than an HTTP dump.
func registryError(err error) error {
	var response *sr.ResponseError
	if errors.As(err, &response) && response.Message != "" {
		return fmt.Errorf("%s (http %d)", response.Message, response.StatusCode)
	}

	return err
}

func kindName(kind sr.SchemaType) string {
	switch kind {
	case sr.TypeAvro:
		return FormatAvro
	case sr.TypeProtobuf:
		return FormatProtobuf
	case sr.TypeJSON:
		return FormatJSONSchema
	}

	return kind.String()
}

func versionName(version int) string {
	if version == 0 {
		return "latest"
	}

	return fmt.Sprint(version)
}

// decodeJSONPayload renders the JSON a JSON Schema message carries, kept as
// stored because it was never re-encoded.
func decodeJSONPayload(payload []byte) (Decoded, error) {
	var document any
	if err := json.Unmarshal(payload, &document); err != nil {
		return Decoded{}, fmt.Errorf("payload is not JSON: %w", err)
	}

	return Decoded{Text: string(payload), Document: document}, nil
}
