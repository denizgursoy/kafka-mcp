package searchmessages

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type JSONFilterSuite struct {
	suite.Suite
}

func TestJSONFilterSuite(t *testing.T) {
	suite.Run(t, new(JSONFilterSuite))
}

// document is the fixture every case matches against. It deliberately carries
// the five states a field can be in: absent, null, true, false, and a value.
const document = `{
	"eventType": "NEW",
	"payload": {
		"amount": 500,
		"currency": "EUR",
		"verified": true,
		"refunded": false,
		"cancelledAt": null,
		"note": ""
	},
	"items": [
		{"sku": "a-1", "qty": 2, "deletedAt": null},
		{"sku": "b-2", "qty": 7}
	]
}`

// match compiles a filter and applies it to the fixture document.
func (s *JSONFilterSuite) match(filter string) bool {
	s.T().Helper()

	compiled, err := compileFilter([]byte(filter))
	s.Require().NoError(err,
		"the filter must compile, otherwise the case is testing the parser rather than matching")

	return compiled.Match([]byte(document))
}

// rejects asserts that a filter is refused at compile time.
func (s *JSONFilterSuite) rejects(filter string) {
	s.T().Helper()

	_, err := compileFilter([]byte(filter))

	s.Require().Error(err,
		"an invalid filter must be rejected before any scanning, so the caller fixes it instead of trusting an empty result")
}

func (s *JSONFilterSuite) TestComparisonOperators() {
	s.Run("eq matches an identical string", func() {
		s.Require().True(s.match(`{"field":"eventType","op":"eq","value":"NEW"}`),
			"eq must match a field holding exactly the given string, which is the most common filter of all")
	})

	s.Run("eq is case sensitive", func() {
		s.Require().False(s.match(`{"field":"eventType","op":"eq","value":"new"}`),
			"eq compares values literally, because an event type that differs in case is a different value")
	})

	s.Run("ne matches a different string", func() {
		s.Require().True(s.match(`{"field":"eventType","op":"ne","value":"OLD"}`),
			"ne must match when the field holds something other than the given value")
	})

	s.Run("eq matches a number", func() {
		s.Require().True(s.match(`{"field":"payload.amount","op":"eq","value":500}`),
			"numbers must compare numerically, so a JSON amount can be matched exactly")
	})

	s.Run("gte includes the boundary", func() {
		s.Require().True(s.match(`{"field":"payload.amount","op":"gte","value":500}`),
			"gte must include the boundary value, since a user asking for 500 and above means 500 too")
	})

	s.Run("gt excludes the boundary", func() {
		s.Require().False(s.match(`{"field":"payload.amount","op":"gt","value":500}`),
			"gt must exclude the boundary, or it would be indistinguishable from gte")
	})

	s.Run("gt matches a value above the bound", func() {
		s.Require().True(s.match(`{"field":"payload.amount","op":"gt","value":499}`),
			"gt must match a field strictly greater than the bound")
	})

	s.Run("lte includes the boundary", func() {
		s.Require().True(s.match(`{"field":"payload.amount","op":"lte","value":500}`),
			"lte must include the boundary value for the same reason gte does")
	})

	s.Run("lt excludes the boundary", func() {
		s.Require().False(s.match(`{"field":"payload.amount","op":"lt","value":500}`),
			"lt must exclude the boundary, or it would be indistinguishable from lte")
	})

	s.Run("contains matches a substring", func() {
		s.Require().True(s.match(`{"field":"payload.currency","op":"contains","value":"UR"}`),
			"contains must match any substring, which is how a caller searches inside a longer field")
	})

	s.Run("starts_with matches a prefix", func() {
		s.Require().True(s.match(`{"field":"payload.currency","op":"starts_with","value":"EU"}`),
			"starts_with must match a prefix, which is how prefixed identifiers are selected")
	})

	s.Run("ends_with does not match a wrong suffix", func() {
		s.Require().False(s.match(`{"field":"payload.currency","op":"ends_with","value":"XX"}`),
			"ends_with must reject a suffix the field does not end with")
	})

	s.Run("regex matches a pattern", func() {
		s.Require().True(s.match(`{"field":"items[0].sku","op":"regex","value":"^[a-z]-\\d$"}`),
			"regex must apply the pattern to the field, for callers describing a shape rather than a value")
	})

	s.Run("in matches a listed value", func() {
		s.Require().True(s.match(`{"field":"eventType","op":"in","value":["NEW","PAID"]}`),
			"in must match when the field equals any listed value, which avoids a chain of or nodes")
	})

	s.Run("in does not match an unlisted value", func() {
		s.Require().False(s.match(`{"field":"eventType","op":"in","value":["PAID"]}`),
			"in must reject a value absent from the list")
	})
}

func (s *JSONFilterSuite) TestExists() {
	s.Run("exists matches a field holding a value", func() {
		s.Require().True(s.match(`{"field":"payload.amount","op":"exists"}`),
			"exists must match any field that is present, whatever it holds")
	})

	s.Run("exists matches a field set to null", func() {
		s.Require().True(s.match(`{"field":"payload.cancelledAt","op":"exists"}`),
			"a field explicitly set to null is present, so exists must match it: that is what separates exists from is_not_null")
	})

	s.Run("exists does not match a missing field", func() {
		s.Require().False(s.match(`{"field":"payload.missing","op":"exists"}`),
			"a field the message does not carry must not be reported as existing")
	})
}

func (s *JSONFilterSuite) TestIsNull() {
	s.Run("is_null matches an explicit null", func() {
		s.Require().True(s.match(`{"field":"payload.cancelledAt","op":"is_null"}`),
			"a field present and set to null must match, because that is the state is_null names")
	})

	s.Run("is_null does not match a missing field", func() {
		s.Require().False(s.match(`{"field":"payload.missing","op":"is_null"}`),
			"a field that is absent is a different state from one set to null, and conflating them hides schema drift")
	})

	s.Run("is_null does not match a field holding a value", func() {
		s.Require().False(s.match(`{"field":"payload.amount","op":"is_null"}`),
			"a field holding a number is not null")
	})

	s.Run("is_null does not match false", func() {
		s.Require().False(s.match(`{"field":"payload.refunded","op":"is_null"}`),
			"false is a value, not the absence of one, and treating it as null would select the wrong messages")
	})
}

func (s *JSONFilterSuite) TestIsNotNull() {
	s.Run("is_not_null matches a field holding a value", func() {
		s.Require().True(s.match(`{"field":"payload.amount","op":"is_not_null"}`),
			"a field present with a value is the case is_not_null exists to select")
	})

	s.Run("is_not_null matches false", func() {
		s.Require().True(s.match(`{"field":"payload.refunded","op":"is_not_null"}`),
			"false is a real value, so a field set to false is not null")
	})

	s.Run("is_not_null matches an empty string", func() {
		s.Require().True(s.match(`{"field":"payload.note","op":"is_not_null"}`),
			"an empty string is a value, and treating it as null would silently drop messages that carry it")
	})

	s.Run("is_not_null does not match an explicit null", func() {
		s.Require().False(s.match(`{"field":"payload.cancelledAt","op":"is_not_null"}`),
			"a field set to null is exactly what is_not_null must exclude")
	})

	s.Run("is_not_null does not match a missing field", func() {
		s.Require().False(s.match(`{"field":"payload.missing","op":"is_not_null"}`),
			"a missing field holds no usable value, so reporting it as non-null would mislead the caller")
	})
}

func (s *JSONFilterSuite) TestIsTrueAndIsFalse() {
	s.Run("is_true matches the boolean true", func() {
		s.Require().True(s.match(`{"field":"payload.verified","op":"is_true"}`),
			"is_true must match a field holding the JSON literal true")
	})

	s.Run("is_true does not match false", func() {
		s.Require().False(s.match(`{"field":"payload.refunded","op":"is_true"}`),
			"is_true must not match the opposite boolean")
	})

	s.Run("is_true does not match a number", func() {
		s.Require().False(s.match(`{"field":"payload.amount","op":"is_true"}`),
			"is_true is strict: a non-zero number is not the boolean true, and coercing it would produce false positives")
	})

	s.Run("is_true does not match a string", func() {
		s.Require().False(s.match(`{"field":"payload.currency","op":"is_true"}`),
			"a string is never the boolean true, so a topic storing \"true\" as text must be matched with eq instead")
	})

	s.Run("is_true does not match a missing field", func() {
		s.Require().False(s.match(`{"field":"payload.missing","op":"is_true"}`),
			"a field that is absent cannot be true")
	})

	s.Run("is_false matches the boolean false", func() {
		s.Require().True(s.match(`{"field":"payload.refunded","op":"is_false"}`),
			"is_false must match a field holding the JSON literal false")
	})

	s.Run("is_false does not match true", func() {
		s.Require().False(s.match(`{"field":"payload.verified","op":"is_false"}`),
			"is_false must not match the opposite boolean")
	})

	s.Run("is_false does not match an explicit null", func() {
		s.Require().False(s.match(`{"field":"payload.cancelledAt","op":"is_false"}`),
			"null is not false, and conflating them would select messages whose flag was never set")
	})

	s.Run("is_false does not match a missing field", func() {
		s.Require().False(s.match(`{"field":"payload.missing","op":"is_false"}`),
			"a field that is absent cannot be false")
	})
}

func (s *JSONFilterSuite) TestNotIsNotTheSameAsIsNotNull() {
	s.Run("is_not_null requires the field to be present", func() {
		s.Require().False(s.match(`{"field":"payload.missing","op":"is_not_null"}`),
			"is_not_null means present and non-null, so a missing field must not match")
	})

	s.Run("not is_null also matches a missing field", func() {
		s.Require().True(s.match(`{"not":{"field":"payload.missing","op":"is_null"}}`),
			"negating is_null includes fields that are absent, which is why the two forms are not interchangeable")
	})
}

func (s *JSONFilterSuite) TestBooleanComposition() {
	s.Run("and matches when every child matches", func() {
		s.Require().True(s.match(
			`{"and":[{"field":"eventType","op":"eq","value":"NEW"},{"field":"payload.amount","op":"gte","value":500}]}`),
			"and must require all of its children, which is how a caller combines conditions")
	})

	s.Run("and fails when one child fails", func() {
		s.Require().False(s.match(
			`{"and":[{"field":"eventType","op":"eq","value":"NEW"},{"field":"payload.amount","op":"gt","value":500}]}`),
			"a single failing child must fail the whole and, or the filter would be looser than the caller asked")
	})

	s.Run("or matches when one child matches", func() {
		s.Require().True(s.match(
			`{"or":[{"field":"eventType","op":"eq","value":"OLD"},{"field":"payload.amount","op":"eq","value":500}]}`),
			"or must match on any child, which is how alternatives are expressed")
	})

	s.Run("or fails when no child matches", func() {
		s.Require().False(s.match(
			`{"or":[{"field":"eventType","op":"eq","value":"OLD"},{"field":"payload.amount","op":"eq","value":1}]}`),
			"or must fail when none of its alternatives hold")
	})

	s.Run("not inverts its child", func() {
		s.Require().True(s.match(`{"not":{"field":"eventType","op":"eq","value":"OLD"}}`),
			"not must invert the verdict of the node it wraps")
	})

	s.Run("nesting and inside or", func() {
		s.Require().True(s.match(
			`{"or":[{"and":[{"field":"eventType","op":"eq","value":"OLD"},{"field":"payload.amount","op":"eq","value":500}]},{"field":"payload.verified","op":"is_true"}]}`),
			"nested combinators must compose, since a real question rarely fits a single flat condition")
	})
}

func (s *JSONFilterSuite) TestArrayPaths() {
	s.Run("an indexed element", func() {
		s.Require().True(s.match(`{"field":"items[0].qty","op":"eq","value":2}`),
			"an index must address that element, so a caller can filter on the first item of a list")
	})

	s.Run("a later indexed element", func() {
		s.Require().True(s.match(`{"field":"items[1].sku","op":"eq","value":"b-2"}`),
			"indexing must not be limited to the first element")
	})

	s.Run("an index beyond the end", func() {
		s.Require().False(s.match(`{"field":"items[5].sku","op":"exists"}`),
			"an index past the end of the array addresses nothing and must not match")
	})

	s.Run("a wildcard matching one element", func() {
		s.Require().True(s.match(`{"field":"items[*].qty","op":"gt","value":5}`),
			"a wildcard must match when any element satisfies the condition, which is how lists are searched")
	})

	s.Run("a wildcard matching no element", func() {
		s.Require().False(s.match(`{"field":"items[*].qty","op":"gt","value":99}`),
			"a wildcard must fail when no element satisfies the condition")
	})

	s.Run("a wildcard with is_null", func() {
		s.Require().True(s.match(`{"field":"items[*].deletedAt","op":"is_null"}`),
			"the null operators must work through a wildcard too, since soft-deleted list entries are a common question")
	})
}

func (s *JSONFilterSuite) TestTypeMismatchIsNotAMatchAndNotAnError() {
	compiled, err := compileFilter(
		[]byte(`{"field":"payload.currency","op":"gt","value":10}`))

	s.Require().NoError(err,
		"comparing a string field numerically is only detectable against real data, so it must compile")
	s.Require().False(compiled.Match([]byte(document)),
		"a type mismatch must be a non-match rather than an error, so one odd message cannot abort a whole scan")
}

func (s *JSONFilterSuite) TestNonJSONValueNeverMatches() {
	compiled, err := compileFilter([]byte(`{"field":"eventType","op":"exists"}`))
	s.Require().NoError(err, "the filter itself is valid")

	s.Run("a plain text value", func() {
		s.Require().False(compiled.Match([]byte("this is not json")),
			"a message that is not JSON cannot satisfy a field filter and must not be reported as a match")
	})

	s.Run("an empty value", func() {
		s.Require().False(compiled.Match(nil),
			"an empty payload has no fields and must not match")
	})
}

func (s *JSONFilterSuite) TestCompileRejectsInvalidFilters() {
	s.Run("an unknown operator", func() {
		s.rejects(`{"field":"a","op":"approximately","value":1}`)
	})

	s.Run("a leaf without a field", func() {
		s.rejects(`{"op":"eq","value":1}`)
	})

	s.Run("a leaf without an operator", func() {
		s.rejects(`{"field":"a","value":1}`)
	})

	s.Run("a value given to a unary operator", func() {
		s.rejects(`{"field":"a","op":"is_null","value":1}`)
	})

	s.Run("a regex that does not parse", func() {
		s.rejects(`{"field":"a","op":"regex","value":"ORD-["}`)
	})

	s.Run("a regex pattern that is not a string", func() {
		s.rejects(`{"field":"a","op":"regex","value":5}`)
	})

	s.Run("in without a list", func() {
		s.rejects(`{"field":"a","op":"in","value":"NEW"}`)
	})

	s.Run("an empty and", func() {
		s.rejects(`{"and":[]}`)
	})

	s.Run("an empty object", func() {
		s.rejects(`{}`)
	})

	s.Run("malformed json", func() {
		s.rejects(`{"and":`)
	})

	s.Run("more than one node kind", func() {
		s.rejects(`{"and":[{"field":"a","op":"exists"}],"or":[{"field":"b","op":"exists"}]}`)
	})
}

func (s *JSONFilterSuite) TestEmptyFilterIsRejected() {
	_, err := compileFilter(nil)

	s.Require().Error(err,
		"an absent filter must be handled by the caller, because a filter matching everything would dump the topic")
}
