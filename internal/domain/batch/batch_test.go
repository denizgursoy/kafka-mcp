package batch_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
)

type BatchSuite struct{ suite.Suite }

func TestBatchSuite(t *testing.T) { suite.Run(t, new(BatchSuite)) }

func (s *BatchSuite) TestPreservesOrderAndIsolatesItemErrors() {
	out, err := batch.Run(s.T().Context(), []int{3, 2, 1}, 3,
		func(_ context.Context, value int) (int, error) {
			if value == 2 {
				return 0, errors.New("two failed")
			}
			return value * 10, nil
		})

	s.Require().NoError(err, "a worker error belongs to its item, not to the whole structurally valid batch")
	s.Require().Equal(30, *out.Results[0].Result, "the first result must remain aligned with the first input")
	s.Require().Equal("two failed", out.Results[1].Error, "the failed item must keep its actionable error")
	s.Require().Equal(10, *out.Results[2].Result, "concurrent completion must not reorder the final item")
	s.Require().Equal(2, out.Succeeded, "only successful items must be counted")
	s.Require().Equal(1, out.Failed, "only failed items must be counted")
}

func (s *BatchSuite) TestRejectsAnEmptyBatch() {
	_, err := batch.Run(s.T().Context(), []int{}, 3,
		func(_ context.Context, value int) (int, error) { return value, nil })

	s.Require().Error(err, "an empty items array is neither a single operation nor a useful batch and must be refused")
}

func (s *BatchSuite) TestRejectsAnOversizedBatch() {
	_, err := batch.Run(s.T().Context(), []int{1, 2, 3, 4}, 3,
		func(_ context.Context, value int) (int, error) { return value, nil })

	s.Require().Error(err, "the maximum must be enforced before workers start, so one call cannot create unbounded cluster load")
}

// conflicting names a field the envelope also has: every write tool's output
// carries "applied", and so does Output.
type conflicting struct {
	Name    string `json:"name"`
	Applied bool   `json:"applied"`
}

func (s *BatchSuite) TestOutputMarshalsAgainstItsInferredSchema() {
	// The tools return Output directly and let the MCP SDK infer the schema, so
	// what is inferred has to accept what is actually sent.
	schema, err := jsonschema.For[batch.Output[conflicting]](nil)
	s.Require().NoError(err, "the envelope's schema must be inferable, or no tool can register it")

	resolved, err := schema.Resolve(nil)
	s.Require().NoError(err, "the inferred schema must resolve")

	encoded, err := json.Marshal(batch.Output[conflicting]{
		Results: []batch.Result[conflicting]{
			{Index: 0, Result: &conflicting{Name: "one", Applied: true}},
			{Index: 1, Error: "second failed"},
		},
		Succeeded: 1,
		Failed:    1,
		Applied:   1,
	})
	s.Require().NoError(err, "marshalling the envelope must succeed")

	var decoded any
	s.Require().NoError(json.Unmarshal(encoded, &decoded), "the encoded envelope must be valid JSON")

	s.Run("the envelope validates", func() {
		s.Require().NoError(resolved.Validate(decoded),
			"a tool's own output schema must accept what that tool sends, or every call fails at the protocol boundary")
	})

	s.Run("a result field named like the envelope's survives", func() {
		s.Require().Contains(string(encoded), `"applied":true`,
			"the item's own applied flag must reach the caller; nesting it under result is what keeps it from colliding with the envelope's count")
	})
}
