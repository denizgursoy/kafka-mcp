// Structured filters over JSON message payloads, used by the filter argument
// of search_messages.
//
// A filter is a tree of nodes. A node is either a boolean combinator:
//
//	{"and": [ ... ]}
//	{"or":  [ ... ]}
//	{"not": { ... }}
//
// or a leaf naming a field, an operator and, for binary operators, a value:
//
//	{"field": "payload.amount", "op": "gte", "value": 500}
//
// This lives in the tool that uses it. Should a second tool ever need to
// filter JSON, move it to a package of its own under internal.
package searchmessages

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Operators usable in a leaf node.
const (
	OpEq         = "eq"
	OpNe         = "ne"
	OpGt         = "gt"
	OpGte        = "gte"
	OpLt         = "lt"
	OpLte        = "lte"
	OpContains   = "contains"
	OpStartsWith = "starts_with"
	OpEndsWith   = "ends_with"
	OpRegex      = "regex"
	OpIn         = "in"
	OpExists     = "exists"
	OpIsNull     = "is_null"
	OpIsNotNull  = "is_not_null"
	OpIsTrue     = "is_true"
	OpIsFalse    = "is_false"
)

// filterOperators returns every supported operator, for documentation and
// errors.
func filterOperators() []string {
	return []string{
		OpEq, OpNe, OpGt, OpGte, OpLt, OpLte,
		OpContains, OpStartsWith, OpEndsWith, OpRegex, OpIn,
		OpExists, OpIsNull, OpIsNotNull, OpIsTrue, OpIsFalse,
	}
}

// unary operators take no value, because the state they test is the presence
// or the exact JSON literal of the field itself.
var unary = map[string]bool{
	OpExists:    true,
	OpIsNull:    true,
	OpIsNotNull: true,
	OpIsTrue:    true,
	OpIsFalse:   true,
}

// Filter is a compiled filter, ready to match many messages.
type compiledFilter struct {
	root node
}

type node interface {
	match(document any) bool
}

// Compile parses and validates a filter.
//
// Everything that can be checked without data is checked here: unknown
// operators, malformed trees, regexes that do not parse. A filter that
// compiles will never fail at match time, so one odd message cannot abort a
// scan of a whole topic.
func compileFilter(raw []byte) (*compiledFilter, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, fmt.Errorf("filter is empty")
	}

	root, err := compileNode(raw)
	if err != nil {
		return nil, err
	}

	return &compiledFilter{root: root}, nil
}

// Match reports whether a JSON document satisfies the filter.
//
// A payload that is not JSON never matches: a field condition cannot be true
// of a document that has no fields.
func (f *compiledFilter) Match(document []byte) bool {
	var parsed any

	if err := json.Unmarshal(document, &parsed); err != nil {
		return false
	}

	return f.root.match(parsed)
}

// rawNode is the shape of any node before its kind is known. Every field is
// deferred so that "value": null can be told apart from an absent "value".
type rawNode struct {
	And   []json.RawMessage `json:"and"`
	Or    []json.RawMessage `json:"or"`
	Not   json.RawMessage   `json:"not"`
	Field *string           `json:"field"`
	Op    *string           `json:"op"`
	Value json.RawMessage   `json:"value"`
}

func compileNode(raw []byte) (node, error) {
	var parsed rawNode

	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("filter node is not valid JSON: %w", err)
	}

	kinds := 0

	if parsed.And != nil {
		kinds++
	}

	if parsed.Or != nil {
		kinds++
	}

	if parsed.Not != nil {
		kinds++
	}

	if parsed.Field != nil || parsed.Op != nil {
		kinds++
	}

	if kinds == 0 {
		return nil, fmt.Errorf(
			`filter node must be one of {"and":[...]}, {"or":[...]}, {"not":{...}} or {"field":...,"op":...}`)
	}

	if kinds > 1 {
		return nil, fmt.Errorf(
			"filter node must have exactly one of and, or, not or field/op, but several were given")
	}

	switch {
	case parsed.And != nil:
		children, err := compileChildren(parsed.And, "and")
		if err != nil {
			return nil, err
		}

		return andNode(children), nil

	case parsed.Or != nil:
		children, err := compileChildren(parsed.Or, "or")
		if err != nil {
			return nil, err
		}

		return orNode(children), nil

	case parsed.Not != nil:
		child, err := compileNode(parsed.Not)
		if err != nil {
			return nil, err
		}

		return notNode{child}, nil
	}

	return compileLeaf(parsed)
}

func compileChildren(raws []json.RawMessage, kind string) ([]node, error) {
	if len(raws) == 0 {
		return nil, fmt.Errorf("%q must contain at least one filter", kind)
	}

	children := make([]node, 0, len(raws))

	for i, raw := range raws {
		child, err := compileNode(raw)
		if err != nil {
			return nil, fmt.Errorf("%s[%d]: %w", kind, i, err)
		}

		children = append(children, child)
	}

	return children, nil
}

func compileLeaf(parsed rawNode) (node, error) {
	if parsed.Field == nil {
		return nil, fmt.Errorf(`filter leaf is missing "field"`)
	}

	if parsed.Op == nil {
		return nil, fmt.Errorf(`filter leaf for %q is missing "op"`, *parsed.Field)
	}

	op := *parsed.Op

	path, err := compilePath(*parsed.Field)
	if err != nil {
		return nil, err
	}

	leaf := &leafNode{path: path, op: op}

	if unary[op] {
		if parsed.Value != nil {
			return nil, fmt.Errorf(
				"operator %q takes no value, but one was given for field %q", op, *parsed.Field)
		}

		return leaf, nil
	}

	if parsed.Value == nil {
		return nil, fmt.Errorf(
			`operator %q on field %q needs a "value"`, op, *parsed.Field)
	}

	var value any

	if err := json.Unmarshal(parsed.Value, &value); err != nil {
		return nil, fmt.Errorf("value for field %q is not valid JSON: %w", *parsed.Field, err)
	}

	leaf.value = value

	switch op {
	case OpEq, OpNe, OpGt, OpGte, OpLt, OpLte,
		OpContains, OpStartsWith, OpEndsWith:

	case OpRegex:
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf(
				"operator %q on field %q needs a string pattern", op, *parsed.Field)
		}

		pattern, err := regexp.Compile(text)
		if err != nil {
			return nil, fmt.Errorf(
				"pattern for field %q does not compile: %w", *parsed.Field, err)
		}

		leaf.pattern = pattern

	case OpIn:
		list, ok := value.([]any)
		if !ok {
			return nil, fmt.Errorf(
				"operator %q on field %q needs a list of values", op, *parsed.Field)
		}

		leaf.list = list

	default:
		return nil, fmt.Errorf(
			"unknown operator %q on field %q, must be one of %s",
			op, *parsed.Field, strings.Join(filterOperators(), ", "))
	}

	return leaf, nil
}

type andNode []node

func (n andNode) match(document any) bool {
	for _, child := range n {
		if !child.match(document) {
			return false
		}
	}

	return true
}

type orNode []node

func (n orNode) match(document any) bool {
	for _, child := range n {
		if child.match(document) {
			return true
		}
	}

	return false
}

type notNode struct {
	child node
}

func (n notNode) match(document any) bool {
	return !n.child.match(document)
}

type leafNode struct {
	path    []segment
	op      string
	value   any
	list    []any
	pattern *regexp.Regexp
}

func (n *leafNode) match(document any) bool {
	// A path may address several values, through a [*] wildcard. The leaf
	// matches when any addressed value satisfies it.
	for _, found := range resolve(document, n.path) {
		if n.test(found) {
			return true
		}
	}

	return false
}

// test applies the operator to one resolved field.
func (n *leafNode) test(found value) bool {
	switch n.op {
	case OpExists:
		return true
	case OpIsNull:
		return found.data == nil
	case OpIsNotNull:
		return found.data != nil
	case OpIsTrue:
		// Strict: the JSON literal true, never the string "true" or 1.
		boolean, ok := found.data.(bool)

		return ok && boolean
	case OpIsFalse:
		boolean, ok := found.data.(bool)

		return ok && !boolean
	}

	// Every remaining operator compares a value, and null has nothing to
	// compare, so it can only be a non-match.
	if found.data == nil {
		return false
	}

	switch n.op {
	case OpEq:
		return equal(found.data, n.value)
	case OpNe:
		return !equal(found.data, n.value)
	case OpGt, OpGte, OpLt, OpLte:
		return n.compare(found.data)
	case OpContains, OpStartsWith, OpEndsWith:
		return n.compareText(found.data)
	case OpRegex:
		text, ok := asText(found.data)

		return ok && n.pattern.MatchString(text)
	case OpIn:
		for _, candidate := range n.list {
			if equal(found.data, candidate) {
				return true
			}
		}

		return false
	}

	return false
}

func (n *leafNode) compare(data any) bool {
	left, ok := asNumber(data)
	if !ok {
		return false
	}

	right, ok := asNumber(n.value)
	if !ok {
		return false
	}

	switch n.op {
	case OpGt:
		return left > right
	case OpGte:
		return left >= right
	case OpLt:
		return left < right
	case OpLte:
		return left <= right
	}

	return false
}

func (n *leafNode) compareText(data any) bool {
	left, ok := asText(data)
	if !ok {
		return false
	}

	right, ok := asText(n.value)
	if !ok {
		return false
	}

	switch n.op {
	case OpContains:
		return strings.Contains(left, right)
	case OpStartsWith:
		return strings.HasPrefix(left, right)
	case OpEndsWith:
		return strings.HasSuffix(left, right)
	}

	return false
}

// equal compares two JSON values. Numbers are compared numerically so that 500
// and 500.0 are the same value, as they are in JSON.
func equal(left, right any) bool {
	leftNumber, leftOK := asNumber(left)
	rightNumber, rightOK := asNumber(right)

	if leftOK && rightOK {
		return leftNumber == rightNumber
	}

	switch typed := left.(type) {
	case string:
		other, ok := right.(string)

		return ok && typed == other
	case bool:
		other, ok := right.(bool)

		return ok && typed == other
	case nil:
		return right == nil
	}

	return false
}

func asNumber(data any) (float64, bool) {
	switch typed := data.(type) {
	case float64:
		return typed, true
	case json.Number:
		parsed, err := typed.Float64()

		return parsed, err == nil
	}

	return 0, false
}

func asText(data any) (string, bool) {
	text, ok := data.(string)

	return text, ok
}

// value is one field addressed by a path. The wrapper exists so that a field
// holding JSON null can be told apart from a field that is not there at all:
// only fields that were found are returned.
type value struct {
	data any
}

// segment is one step of a field path: an object key, an array index, or a
// wildcard over every element of an array.
type segment struct {
	key      string
	index    int
	isIndex  bool
	wildcard bool
}

// compilePath parses a dotted path with optional array subscripts, such as
// payload.items[0].sku or items[*].qty.
func compilePath(path string) ([]segment, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("field path is empty")
	}

	segments := make([]segment, 0, strings.Count(path, ".")+1)

	for _, part := range strings.Split(path, ".") {
		if part == "" {
			return nil, fmt.Errorf("field path %q has an empty segment", path)
		}

		name := part

		if open := strings.IndexByte(part, '['); open >= 0 {
			name = part[:open]
			part = part[open:]
		} else {
			part = ""
		}

		if name != "" {
			segments = append(segments, segment{key: name})
		}

		for part != "" {
			if !strings.HasPrefix(part, "[") {
				return nil, fmt.Errorf("field path %q is malformed near %q", path, part)
			}

			close := strings.IndexByte(part, ']')
			if close < 0 {
				return nil, fmt.Errorf("field path %q has an unclosed subscript", path)
			}

			subscript := part[1:close]
			part = part[close+1:]

			if subscript == "*" {
				segments = append(segments, segment{wildcard: true})

				continue
			}

			index, err := strconv.Atoi(subscript)
			if err != nil || index < 0 {
				return nil, fmt.Errorf(
					"field path %q has an invalid array index %q", path, subscript)
			}

			segments = append(segments, segment{index: index, isIndex: true})
		}
	}

	if len(segments) == 0 {
		return nil, fmt.Errorf("field path %q addresses nothing", path)
	}

	return segments, nil
}

// resolve walks a path and returns every value it addresses. A path that
// addresses nothing returns no values, which is how a missing field is told
// apart from a field holding null.
func resolve(document any, path []segment) []value {
	current := []any{document}

	for _, step := range path {
		next := make([]any, 0, len(current))

		for _, item := range current {
			switch {
			case step.wildcard:
				list, ok := item.([]any)
				if !ok {
					continue
				}

				next = append(next, list...)

			case step.isIndex:
				list, ok := item.([]any)
				if !ok || step.index >= len(list) {
					continue
				}

				next = append(next, list[step.index])

			default:
				object, ok := item.(map[string]any)
				if !ok {
					continue
				}

				found, ok := object[step.key]
				if !ok {
					continue
				}

				next = append(next, found)
			}
		}

		current = next

		if len(current) == 0 {
			return nil
		}
	}

	values := make([]value, 0, len(current))

	for _, item := range current {
		values = append(values, value{data: item})
	}

	return values
}
