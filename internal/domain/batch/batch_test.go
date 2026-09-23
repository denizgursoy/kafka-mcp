package batch_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
)

type BatchSuite struct{ suite.Suite }

type sampleOutput struct {
	Name string `json:"name"`
}

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

func (s *BatchSuite) TestOutputSchemaAcceptsSingleAndBatchShapes() {
	schema, err := batch.OutputSchema[sampleOutput]().Resolve(nil)
	s.Require().NoError(err, "the combined MCP output schema must resolve before a tool can register it")

	s.Run("single output", func() {
		s.Require().NoError(schema.Validate(map[string]any{"name": "one"}),
			"the original single-operation shape must remain valid for backward compatibility")
	})

	s.Run("batch output", func() {
		s.Require().NoError(schema.Validate(map[string]any{
			"results":   []any{map[string]any{"index": 0, "result": map[string]any{"name": "one"}}},
			"succeeded": 1,
			"failed":    0,
			"applied":   0,
			"atomic":    false,
		}), "the batch envelope must validate without also requiring single-operation fields")
	})
}
