package serde

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/bufbuild/protocompile"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/twmb/avro"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
)

// fixedCodec handles one part of a topic whose format is set in
// topic_formats. Its bytes carry no header: the configuration is the schema.
type fixedCodec interface {
	format() string
	decode(data []byte) (Decoded, error)
	encode(document string) ([]byte, Encoded, error)
}

func newFixed(settings *config.PartFormat) (fixedCodec, error) {
	switch settings.Format {
	case config.FormatAvro:
		source, err := os.ReadFile(settings.SchemaFile)
		if err != nil {
			return nil, fmt.Errorf("read avro schema_file: %w", err)
		}

		schema, err := avro.Parse(string(source))
		if err != nil {
			return nil, fmt.Errorf("parse avro schema_file %s: %w", settings.SchemaFile, err)
		}

		return fixedAvro{schema: schema}, nil

	case config.FormatProtobuf:
		message, err := loadProtoMessage(settings)
		if err != nil {
			return nil, err
		}

		return fixedProto{message: message}, nil

	case config.FormatJSON:
		return fixedJSON{}, nil

	case config.FormatMsgpack:
		return fixedMsgpack{}, nil

	case config.FormatText:
		return fixedText{}, nil

	case config.FormatBinary:
		return fixedBinary{}, nil
	}

	return nil, fmt.Errorf("unknown format %q", settings.Format)
}

// EncodeConfigured encodes a JSON document for a topic whose part has a format
// set in topic_formats. ok is false when it has none, and the caller should
// write the value as given.
func (c *Codec) EncodeConfigured(topic string, part Part, document string) ([]byte, Encoded, bool, error) {
	fixed := c.fixedFor(topic, part)
	if fixed == nil {
		return nil, Encoded{}, false, nil
	}

	data, encoded, err := fixed.encode(document)
	if err != nil {
		return nil, Encoded{}, true, fmt.Errorf("%s does not fit the %s format configured for topic %q: %w",
			part, fixed.format(), topic, err)
	}

	return data, encoded, true, nil
}

// Encode encodes a JSON document against a registry schema, framed with the
// header a registry-aware consumer expects.
func (c *Codec) Encode(ctx context.Context, topic string, part Part, document string, target Target) ([]byte, Encoded, error) {
	if c == nil || c.registry == nil {
		return nil, Encoded{}, fmt.Errorf(
			"this cluster has no schema_registry configured, so there is no schema to encode the %s against", part)
	}

	return c.registry.encode(ctx, topic, part, document, target)
}

type fixedAvro struct{ schema *avro.Schema }

func (f fixedAvro) format() string { return FormatAvro }

func (f fixedAvro) decode(data []byte) (Decoded, error) {
	decoded, err := decodeAvro(f.schema, data)
	decoded.MessageType = avroName(f.schema)

	return decoded, err
}

func (f fixedAvro) encode(document string) ([]byte, Encoded, error) {
	data, err := encodeAvro(f.schema, document)

	return data, Encoded{Format: FormatAvro, MessageType: avroName(f.schema)}, err
}

type fixedProto struct {
	message protoreflect.MessageDescriptor
}

func (f fixedProto) format() string { return FormatProtobuf }

func (f fixedProto) decode(data []byte) (Decoded, error) {
	decoded, err := decodeProto(f.message, data)
	decoded.MessageType = string(f.message.FullName())

	return decoded, err
}

func (f fixedProto) encode(document string) ([]byte, Encoded, error) {
	data, err := encodeProto(f.message, document)

	return data, Encoded{Format: FormatProtobuf, MessageType: string(f.message.FullName())}, err
}

type fixedJSON struct{}

func (fixedJSON) format() string { return FormatJSON }

func (fixedJSON) decode(data []byte) (Decoded, error) {
	decoded, err := decodeJSONPayload(data)
	decoded.Format = FormatJSON

	return decoded, err
}

func (fixedJSON) encode(document string) ([]byte, Encoded, error) {
	if !json.Valid([]byte(document)) {
		return nil, Encoded{}, fmt.Errorf("the value is not valid JSON")
	}

	return []byte(document), Encoded{Format: FormatJSON}, nil
}

type fixedText struct{}

func (fixedText) format() string { return FormatText }

func (fixedText) decode(data []byte) (Decoded, error) {
	if !utf8.Valid(data) {
		return Decoded{}, fmt.Errorf("the bytes are not valid UTF-8")
	}

	return Decoded{Text: string(data), Format: FormatText}, nil
}

func (fixedText) encode(document string) ([]byte, Encoded, error) {
	return []byte(document), Encoded{Format: FormatText}, nil
}

type fixedBinary struct{}

func (fixedBinary) format() string { return FormatBinary }

func (fixedBinary) decode(data []byte) (Decoded, error) {
	return Decoded{Text: base64.StdEncoding.EncodeToString(data), Format: FormatBinary}, nil
}

func (fixedBinary) encode(document string) ([]byte, Encoded, error) {
	data, err := base64.StdEncoding.DecodeString(document)
	if err != nil {
		return nil, Encoded{}, fmt.Errorf("a binary value must be given as base64: %w", err)
	}

	return data, Encoded{Format: FormatBinary}, nil
}

type fixedMsgpack struct{}

func (fixedMsgpack) format() string { return FormatMsgpack }

func (fixedMsgpack) decode(data []byte) (Decoded, error) {
	reader := bytes.NewReader(data)

	// bytes.Reader is an io.ByteScanner, which stops the decoder buffering
	// ahead, so reader.Len is exactly what the value did not consume.
	decoder := msgpack.NewDecoder(reader)
	decoder.SetMapDecoder(func(d *msgpack.Decoder) (any, error) { return d.DecodeUntypedMap() })

	value, err := decoder.DecodeInterface()
	if err != nil {
		return Decoded{}, err
	}

	if reader.Len() > 0 {
		return Decoded{}, fmt.Errorf("%d bytes follow the first msgpack value", reader.Len())
	}

	return structured(FormatMsgpack, jsonable(value))
}

func (fixedMsgpack) encode(document string) ([]byte, Encoded, error) {
	value, err := parseJSON(document)
	if err != nil {
		return nil, Encoded{}, err
	}

	var buffer bytes.Buffer

	encoder := msgpack.NewEncoder(&buffer)
	encoder.SetSortMapKeys(true)

	if err := encoder.Encode(fromJSONNumbers(value)); err != nil {
		return nil, Encoded{}, err
	}

	return buffer.Bytes(), Encoded{Format: FormatMsgpack}, nil
}

// jsonable converts what msgpack decodes into values JSON can carry: map keys
// become strings, and bytes become text when they are text and base64 when
// they are not.
func jsonable(value any) any {
	switch typed := value.(type) {
	case map[any]any:
		converted := make(map[string]any, len(typed))
		for key, child := range typed {
			converted[fmt.Sprint(key)] = jsonable(child)
		}

		return converted

	case map[string]any:
		converted := make(map[string]any, len(typed))
		for key, child := range typed {
			converted[key] = jsonable(child)
		}

		return converted

	case []any:
		converted := make([]any, len(typed))
		for i, child := range typed {
			converted[i] = jsonable(child)
		}

		return converted

	case []byte:
		if utf8.Valid(typed) {
			return string(typed)
		}

		return base64.StdEncoding.EncodeToString(typed)
	}

	return value
}

// parseJSON parses a caller's document keeping numbers exact.
func parseJSON(document string) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(document))
	decoder.UseNumber()

	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("the value is not valid JSON: %w", err)
	}

	if decoder.More() {
		return nil, fmt.Errorf("the value holds more than one JSON document")
	}

	return value, nil
}

// fromJSONNumbers turns json.Number into int64 where the number is whole and
// float64 otherwise, so a count stays an integer for a typed consumer.
func fromJSONNumbers(value any) any {
	switch typed := value.(type) {
	case json.Number:
		if integer, err := typed.Int64(); err == nil {
			return integer
		}

		float, _ := typed.Float64()

		return float

	case map[string]any:
		for key, child := range typed {
			typed[key] = fromJSONNumbers(child)
		}

	case []any:
		for i, child := range typed {
			typed[i] = fromJSONNumbers(child)
		}
	}

	return value
}

func decodeAvro(schema *avro.Schema, payload []byte) (Decoded, error) {
	var value any

	rest, err := schema.Decode(payload, &value)
	if err != nil {
		return Decoded{}, err
	}

	if len(rest) > 0 {
		return Decoded{}, fmt.Errorf("%d bytes are left after the record, so the bytes were not written with this schema", len(rest))
	}

	raw, err := schema.EncodeJSON(value)
	if err != nil {
		return Decoded{}, err
	}

	text, document, err := normalizeJSON(raw)
	if err != nil {
		return Decoded{}, err
	}

	return Decoded{Text: text, Format: FormatAvro, Document: document}, nil
}

func encodeAvro(schema *avro.Schema, document string) ([]byte, error) {
	parsed, err := parseJSON(document)
	if err != nil {
		return nil, err
	}

	// Avro's JSON decoding drops fields the schema does not have. On a write
	// that turns a misspelt field into a silently written default, so unknown
	// fields are refused first.
	if err := checkAvroFields(schema.Root(), parsed, "", map[string]*avro.SchemaNode{}); err != nil {
		return nil, err
	}

	var value any
	if err := schema.DecodeJSON([]byte(document), &value); err != nil {
		return nil, err
	}

	return schema.Encode(value)
}

// checkAvroFields reports the first object key the schema has no field for.
func checkAvroFields(node *avro.SchemaNode, value any, path string, named map[string]*avro.SchemaNode) error {
	if node.Name != "" {
		named[node.Name] = node
		if node.Namespace != "" {
			named[node.Namespace+"."+node.Name] = node
		}
	}

	if definition, ok := named[node.Type]; ok && node.Name == "" {
		node = definition
	}

	switch node.Type {
	case "record", "error":
		object, ok := value.(map[string]any)
		if !ok {
			return nil
		}

		known := make(map[string]avro.SchemaField, len(node.Fields))

		for _, field := range node.Fields {
			known[field.Name] = field
			for _, alias := range field.Aliases {
				known[alias] = field
			}
		}

		keys := make([]string, 0, len(object))
		for key := range object {
			keys = append(keys, key)
		}

		sort.Strings(keys)

		for _, key := range keys {
			field, ok := known[key]
			if !ok {
				return fmt.Errorf("field %q is not in record %s", fieldPath(path, key), node.Name)
			}

			if err := checkAvroFields(&field.Type, object[key], fieldPath(path, key), named); err != nil {
				return err
			}
		}

	case "array":
		if items, ok := value.([]any); ok && node.Items != nil {
			for i, item := range items {
				if err := checkAvroFields(node.Items, item, fmt.Sprintf("%s[%d]", path, i), named); err != nil {
					return err
				}
			}
		}

	case "map":
		if object, ok := value.(map[string]any); ok && node.Values != nil {
			for key, child := range object {
				if err := checkAvroFields(node.Values, child, fieldPath(path, key), named); err != nil {
					return err
				}
			}
		}

	case "union":
		return checkAvroUnion(node, value, path, named)
	}

	return nil
}

// checkAvroUnion accepts a tagged branch ({"type": value}) or a bare value
// that at least one record branch has every field for.
func checkAvroUnion(node *avro.SchemaNode, value any, path string, named map[string]*avro.SchemaNode) error {
	object, ok := value.(map[string]any)
	if !ok {
		return nil
	}

	if len(object) == 1 {
		for tag, inner := range object {
			for i := range node.Branches {
				branch := &node.Branches[i]
				if branch.Type == tag || branch.Name == tag || branch.Namespace+"."+branch.Name == tag {
					return checkAvroFields(branch, inner, path, named)
				}
			}
		}
	}

	var first error

	for i := range node.Branches {
		branch := &node.Branches[i]

		if branch.Type == "null" {
			continue
		}

		err := checkAvroFields(branch, value, path, named)
		if err == nil {
			return nil
		}

		if first == nil {
			first = err
		}
	}

	return first
}

func fieldPath(path, key string) string {
	if path == "" {
		return key
	}

	return path + "." + key
}

func avroName(schema *avro.Schema) string {
	root := schema.Root()
	if root.Name == "" {
		return ""
	}

	if root.Namespace == "" {
		return root.Name
	}

	return root.Namespace + "." + root.Name
}

func decodeProto(message protoreflect.MessageDescriptor, payload []byte) (Decoded, error) {
	value := dynamicpb.NewMessage(message)

	if err := proto.Unmarshal(payload, value); err != nil {
		return Decoded{}, err
	}

	// Unset proto3 fields are emitted with their defaults, because proto3
	// cannot tell unset from default and a search for status NEW must see a
	// message whose status was never set. Original field names are used so
	// the JSON reads like the .proto file.
	raw, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(value)
	if err != nil {
		return Decoded{}, err
	}

	text, document, err := normalizeJSON(raw)
	if err != nil {
		return Decoded{}, err
	}

	return Decoded{Text: text, Format: FormatProtobuf, Document: document}, nil
}

func encodeProto(message protoreflect.MessageDescriptor, document string) ([]byte, error) {
	value := dynamicpb.NewMessage(message)

	if err := protojson.Unmarshal([]byte(document), value); err != nil {
		return nil, err
	}

	return proto.MarshalOptions{Deterministic: true}.Marshal(value)
}

// messageAt follows a Schema Registry message index: the first number picks a
// top-level message, each later one a message nested in the previous.
func messageAt(file protoreflect.FileDescriptor, index []int) (protoreflect.MessageDescriptor, error) {
	if len(index) == 0 {
		index = []int{0}
	}

	messages := file.Messages()

	var message protoreflect.MessageDescriptor

	for depth, position := range index {
		if position < 0 || position >= messages.Len() {
			return nil, fmt.Errorf("message index %v points past the %d message(s) at depth %d of the schema", index, messages.Len(), depth)
		}

		message = messages.Get(position)
		messages = message.Messages()
	}

	return message, nil
}

// messageNamed finds a message by full or short name and returns the index a
// header needs to point at it. An empty name means the first message.
func messageNamed(file protoreflect.FileDescriptor, name string) (protoreflect.MessageDescriptor, []int, error) {
	if file.Messages().Len() == 0 {
		return nil, nil, fmt.Errorf("the schema defines no messages")
	}

	if name == "" {
		return file.Messages().Get(0), []int{0}, nil
	}

	var (
		found []protoreflect.MessageDescriptor
		paths [][]int
		names []string
	)

	var walk func(messages protoreflect.MessageDescriptors, prefix []int)
	walk = func(messages protoreflect.MessageDescriptors, prefix []int) {
		for i := 0; i < messages.Len(); i++ {
			message := messages.Get(i)
			path := append(append([]int{}, prefix...), i)

			names = append(names, string(message.FullName()))

			if string(message.FullName()) == name || string(message.Name()) == name {
				found = append(found, message)
				paths = append(paths, path)
			}

			walk(message.Messages(), path)
		}
	}

	walk(file.Messages(), nil)

	switch len(found) {
	case 1:
		return found[0], paths[0], nil
	case 0:
		return nil, nil, fmt.Errorf("message_type %q is not in the schema, which defines %s", name, strings.Join(names, ", "))
	}

	return nil, nil, fmt.Errorf("message_type %q is ambiguous; use the fully qualified name", name)
}

// compileProtoSources compiles .proto sources held in memory, with the
// standard google/protobuf imports available.
func compileProtoSources(sources map[string]string, main string) (protoreflect.FileDescriptor, error) {
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			Accessor: func(path string) (io.ReadCloser, error) {
				source, ok := sources[path]
				if !ok {
					return nil, fmt.Errorf("%s: %w", path, os.ErrNotExist)
				}

				return io.NopCloser(strings.NewReader(source)), nil
			},
		}),
	}

	files, err := compiler.Compile(context.Background(), main)
	if err != nil {
		return nil, fmt.Errorf("compile protobuf schema: %w", err)
	}

	return files[0], nil
}

// loadProtoMessage finds the configured message in .proto files or a
// compiled descriptor set.
func loadProtoMessage(settings *config.PartFormat) (protoreflect.MessageDescriptor, error) {
	name := protoreflect.FullName(settings.MessageType)

	if settings.DescriptorSet != "" {
		raw, err := os.ReadFile(settings.DescriptorSet)
		if err != nil {
			return nil, fmt.Errorf("read descriptor_set: %w", err)
		}

		var set descriptorpb.FileDescriptorSet
		if err := proto.Unmarshal(raw, &set); err != nil {
			return nil, fmt.Errorf("descriptor_set %s is not a FileDescriptorSet: %w", settings.DescriptorSet, err)
		}

		files, err := protodesc.NewFiles(&set)
		if err != nil {
			return nil, fmt.Errorf("descriptor_set %s: %w (build it with its imports: buf build -o, or protoc --include_imports)",
				settings.DescriptorSet, err)
		}

		descriptor, err := files.FindDescriptorByName(name)
		if err != nil {
			return nil, fmt.Errorf("message_type %s is not in descriptor_set %s", name, settings.DescriptorSet)
		}

		message, ok := descriptor.(protoreflect.MessageDescriptor)
		if !ok {
			return nil, fmt.Errorf("message_type %s is not a message", name)
		}

		return message, nil
	}

	resolver := &protocompile.SourceResolver{ImportPaths: settings.ImportPaths}

	compiler := protocompile.Compiler{Resolver: protocompile.WithStandardImports(resolver)}

	files, err := compiler.Compile(context.Background(), settings.ProtoFiles...)
	if err != nil {
		return nil, fmt.Errorf("compile proto_files %v (import_paths %v): %w",
			settings.ProtoFiles, absolute(settings.ImportPaths), err)
	}

	descriptor, err := files.AsResolver().FindDescriptorByName(name)
	if err != nil {
		return nil, fmt.Errorf("message_type %s is not defined in proto_files %v", name, settings.ProtoFiles)
	}

	message, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("message_type %s is not a message", name)
	}

	return message, nil
}

func absolute(paths []string) []string {
	resolved := make([]string, 0, len(paths))

	for _, path := range paths {
		if abs, err := filepath.Abs(path); err == nil {
			resolved = append(resolved, abs)
		} else {
			resolved = append(resolved, path)
		}
	}

	return resolved
}

// compileJSONSchema keeps a JSON Schema for validating writes. Resolution is
// deferred to the first write, because reading a message needs no schema and a
// schema this library cannot resolve must not make its messages unreadable.
func compileJSONSchema(text string) (*jsonschema.Resolved, error) {
	var schema jsonschema.Schema
	if err := json.Unmarshal([]byte(text), &schema); err != nil {
		return nil, fmt.Errorf("parse json schema: %w", err)
	}

	resolved, err := schema.Resolve(nil)
	if err != nil {
		// Draft-04 and draft-06 schemas are common in registries and this
		// validator supports draft-07 and 2020-12. Their keywords mostly
		// coincide, so the schema is retried as the default draft rather than
		// refusing every write against it.
		schema.Schema = ""

		retried, retryErr := schema.Resolve(nil)
		if retryErr != nil {
			return nil, fmt.Errorf("resolve json schema: %w", err)
		}

		resolved = retried
	}

	return resolved, nil
}

func encodeJSONSchema(resolved *jsonschema.Resolved, document string) ([]byte, error) {
	var value any
	if err := json.Unmarshal([]byte(document), &value); err != nil {
		return nil, fmt.Errorf("the value is not valid JSON: %w", err)
	}

	if resolved == nil {
		return nil, fmt.Errorf("the schema could not be resolved, so the value cannot be validated")
	}

	if err := resolved.Validate(value); err != nil {
		return nil, err
	}

	return []byte(document), nil
}
