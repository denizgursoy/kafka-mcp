package searchmessages

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/kgo"
)

type ScriptSuite struct {
	suite.Suite
}

func TestScriptSuite(t *testing.T) {
	suite.Run(t, new(ScriptSuite))
}

// record is the message every case is evaluated against.
func (s *ScriptSuite) record() *kgo.Record {
	s.T().Helper()

	return &kgo.Record{
		Topic:     "orders",
		Partition: 2,
		Offset:    17,
		Timestamp: time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC),
		Key:       []byte("order-123"),
		Value: []byte(`{
			"eventType": "NEW",
			"payload": {
				"amount": 500,
				"currency": "EUR",
				"verified": true,
				"cancelledAt": null,
				"items": [{"sku": "a-1", "qty": 2}, {"sku": "b-2", "qty": 7}]
			}
		}`),
		Headers: []kgo.RecordHeader{
			{Key: "correlation-id", Value: []byte("corr-999")},
		},
	}
}

// matches compiles a script and runs it against the fixture record.
func (s *ScriptSuite) matches(source string) bool {
	s.T().Helper()

	script, err := compileScript(source)
	s.Require().NoError(err,
		"the script must compile, otherwise the case is testing the compiler rather than matching")

	defer script.close()

	matched, err := script.match(s.record())
	s.Require().NoError(err,
		"a well-formed script must not fail against a well-formed message")

	return matched
}

func (s *ScriptSuite) TestReadsTheMessageValue() {
	s.Run("a top level field", func() {
		s.Require().True(s.matches(`return value.eventType === 'NEW'`),
			"reading a field of the parsed value is the most common filter there is")
	})

	s.Run("a nested field", func() {
		s.Require().True(s.matches(`return value.payload.amount >= 500`),
			"nested access must work, because real messages are not flat")
	})

	s.Run("an array element", func() {
		s.Require().True(s.matches(`return value.payload.items[1].qty === 7`),
			"indexing an array must work, since lists inside messages are ordinary")
	})

	s.Run("an array method", func() {
		s.Require().True(s.matches(
			`return value.payload.items.some(function (i) { return i.qty > 5 })`),
			"array methods are the reason for using JavaScript instead of a fixed operator set")
	})

	s.Run("a computed comparison", func() {
		s.Require().True(s.matches(`return value.payload.amount * 2 === 1000`),
			"arithmetic across fields cannot be expressed by a filter grammar, and is why scripts are more capable")
	})
}

func (s *ScriptSuite) TestDistinguishesNullFromMissing() {
	s.Run("a field set to null", func() {
		s.Require().True(s.matches(`return value.payload.cancelledAt === null`),
			"a field present and set to null must be detectable, because that is a real state in a message")
	})

	s.Run("a missing field is undefined, not null", func() {
		s.Require().True(s.matches(`return value.payload.refundedAt === undefined`),
			"an absent field is a different state from one set to null, and conflating them hides schema drift")
	})

	s.Run("a missing field does not equal null", func() {
		s.Require().False(s.matches(`return value.payload.refundedAt === null`),
			"strict equality must keep absent and null apart, which is what JavaScript gives us for free")
	})
}

func (s *ScriptSuite) TestReadsTheMessageMetadata() {
	s.Run("the key", func() {
		s.Require().True(s.matches(`return key === 'order-123'`),
			"an exact key comparison is the most precise lookup there is, and must be expressible")
	})

	s.Run("a header", func() {
		s.Require().True(s.matches(`return headers['correlation-id'] === 'corr-999'`),
			"correlation ids usually live in headers, so headers must be reachable")
	})

	s.Run("the partition", func() {
		s.Require().True(s.matches(`return partition === 2`),
			"the partition must be readable so a script can narrow to one without a separate parameter")
	})

	s.Run("the offset", func() {
		s.Require().True(s.matches(`return offset === 17`),
			"the offset must be readable, since it identifies the message")
	})

	s.Run("the timestamp as a date", func() {
		s.Require().True(s.matches(`return timestamp.getUTCFullYear() === 2026`),
			"the timestamp must be a Date so a script can compare times without parsing strings")
	})
}

func (s *ScriptSuite) TestKeyIsNullWhenAbsent() {
	script, err := compileScript(`return key === null`)
	s.Require().NoError(err, "the script must compile")

	defer script.close()

	matched, err := script.match(&kgo.Record{Value: []byte(`{}`)})

	s.Require().NoError(err, "a message without a key must not fail the script")
	s.Require().True(matched,
		"an absent key must be null rather than an empty string, or a script cannot tell a missing key from an empty one")
}

func (s *ScriptSuite) TestNonJSONValueIsAString() {
	script, err := compileScript(`return value.indexOf('ERROR') >= 0`)
	s.Require().NoError(err, "the script must compile")

	defer script.close()

	matched, err := script.match(&kgo.Record{Value: []byte("level=ERROR msg=failed")})

	s.Require().NoError(err,
		"a plain text message must not fail a script: not every topic holds JSON")
	s.Require().True(matched,
		"a value that is not JSON must arrive as a string so text topics remain searchable")
}

func (s *ScriptSuite) TestStringMethodsOnValue() {
	s.Require().True(s.matches(`return value.payload.currency.toLowerCase() === 'eur'`),
		"string methods must be available, since case-insensitive matching used to need a dedicated option")
}

func (s *ScriptSuite) TestRegularExpressions() {
	s.Require().True(s.matches(`return /^order-\d+$/.test(key)`),
		"regular expressions must work, because they replace the regex match mode that scripts supersede")
}

func (s *ScriptSuite) TestTruthinessDecidesTheMatch() {
	s.Run("a non-empty string matches", func() {
		s.Require().True(s.matches(`return value.eventType`),
			"a truthy value must count as a match, so a script need not spell out a boolean")
	})

	s.Run("an empty result does not match", func() {
		s.Require().False(s.matches(`return value.payload.refundedAt`),
			"undefined is falsy and must not match, which is the ordinary JavaScript reading")
	})

	s.Run("returning nothing does not match", func() {
		s.Require().False(s.matches(`var x = 1`),
			"a script that returns nothing must match nothing rather than everything, so a mistake is not silently permissive")
	})
}

func (s *ScriptSuite) TestRejectsASyntaxErrorBeforeScanning() {
	_, err := compileScript(`return value.eventType ===`)

	s.Require().Error(err,
		"a malformed script must be refused before any message is read, so the caller fixes it instead of waiting for an empty result")
}

func (s *ScriptSuite) TestReportsAScriptErrorPerMessage() {
	script, err := compileScript(`return value.payload.amount > 0`)
	s.Require().NoError(err, "the script must compile")

	defer script.close()

	// A plain string value has no payload, so reading through it throws.
	_, err = script.match(&kgo.Record{Value: []byte("not json at all")})

	s.Require().Error(err,
		"a script that throws on a message must report an error for that message, so a broken script is distinguishable from a genuine absence of matches")
}

func (s *ScriptSuite) TestStopsAnInfiniteLoop() {
	script, err := compileScript(`while (true) {} return true`)
	s.Require().NoError(err, "an infinite loop is valid JavaScript and must compile")

	defer script.close()

	done := make(chan error, 1)

	go func() {
		_, err := script.match(s.record())
		done <- err
	}()

	// The interrupt is what makes user-supplied scripts safe to run at all.
	time.Sleep(50 * time.Millisecond)
	script.interrupt()

	select {
	case err := <-done:
		s.Require().Error(err,
			"an interrupted script must return an error rather than a verdict, because it never reached one")
	case <-time.After(5 * time.Second):
		s.Require().Fail(
			"an infinite loop was not interrupted: without this a single script would hang the server forever")
	}
}

func (s *ScriptSuite) TestStopsRunawayRecursion() {
	script, err := compileScript(`function f() { return f() } return f()`)
	s.Require().NoError(err, "unbounded recursion is valid JavaScript and must compile")

	defer script.close()

	_, err = script.match(s.record())

	s.Require().Error(err,
		"runaway recursion must raise an error rather than exhaust the host stack and take the server down with it")
}

func (s *ScriptSuite) TestIsDeterministic() {
	s.Run("the clock is fixed", func() {
		first := s.matches(`return Date.now() === Date.now()`)

		s.Require().True(first,
			"time must not advance during a scan, or the same search could return different messages on consecutive runs")
	})

	s.Run("randomness is fixed", func() {
		script, err := compileScript(`return Math.random()`)
		s.Require().NoError(err, "the script must compile")

		defer script.close()

		// Two evaluations of the same runtime must agree, so a scan cannot
		// produce a different answer for identical messages.
		one, err := script.match(s.record())
		s.Require().NoError(err, "the script must run")

		two, err := script.match(s.record())
		s.Require().NoError(err, "the script must run again")

		s.Require().Equal(one, two,
			"a random source must be seeded so results are reproducible, which the repository requires of every tool")
	})
}

func (s *ScriptSuite) TestHasNoAccessToTheHost() {
	for _, global := range []string{"require", "process", "console", "fetch", "setTimeout"} {
		s.Run("there is no "+global, func() {
			s.Require().True(s.matches(`return typeof `+global+` === 'undefined'`),
				"the script sandbox must expose no host capability: a filter has no business reaching the filesystem, the network or the clock")
		})
	}
}
