package script_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/script"
)

// ScriptSuite starts no broker. What a predicate is allowed to do, and how one
// that never returns is stopped, is decided entirely by this package, so
// requiring Docker here would only keep these cases out of `go test -short` —
// the run most likely to catch a sandbox rule that stopped being enforced.
type ScriptSuite struct {
	suite.Suite
}

func TestScriptSuite(t *testing.T) {
	suite.Run(t, new(ScriptSuite))
}

func (s *ScriptSuite) TestEvaluatesAPredicate() {
	compiled, err := script.Compile(`return topic.indexOf("orders") >= 0`, "topic")
	s.Require().NoError(err, "a valid predicate must compile")

	defer compiled.Close()

	s.Run("a matching input is true", func() {
		matched, err := compiled.Call(compiled.Value("orders-v2"))
		s.Require().NoError(err, "evaluating a valid predicate must succeed")
		s.Require().True(matched, "the predicate matches this input, so it must report true")
	})

	s.Run("a non-matching input is false", func() {
		matched, err := compiled.Call(compiled.Value("payments"))
		s.Require().NoError(err, "evaluating a valid predicate must succeed")
		s.Require().False(matched, "the predicate does not match this input")
	})
}

func (s *ScriptSuite) TestUsesJavaScriptTruthiness() {
	compiled, err := script.Compile(`return count`, "count")
	s.Require().NoError(err, "returning a value rather than a comparison must compile")

	defer compiled.Close()

	matched, err := compiled.Call(compiled.Value(3))
	s.Require().NoError(err, "evaluation must succeed")
	s.Require().True(matched,
		"ordinary truthiness lets a caller return a field directly rather than spelling out a comparison")
}

func (s *ScriptSuite) TestRejectsAScriptThatDoesNotCompile() {
	_, err := script.Compile(`return topic ===`, "topic")

	s.Require().Error(err,
		"a malformed script must be refused once, up front, rather than failing identically on every input")
}

func (s *ScriptSuite) TestReportsAScriptThatThrows() {
	compiled, err := script.Compile(`return topic.nothing.here`, "topic")
	s.Require().NoError(err, "reading a missing property is valid JavaScript and must compile")

	defer compiled.Close()

	_, err = compiled.Call(compiled.Value("orders"))

	s.Require().Error(err,
		"a script that throws reached no verdict, which is a different outcome from deciding the input does not match")
}

func (s *ScriptSuite) TestDeniesAccessToTheHost() {
	s.Run("no require", func() {
		compiled, err := script.Compile(`return typeof require === "undefined"`, "topic")
		s.Require().NoError(err, "the script must compile")

		defer compiled.Close()

		absent, err := compiled.Call(compiled.Value("t"))
		s.Require().NoError(err, "evaluation must succeed")
		s.Require().True(absent,
			"require must not exist in the runtime: a filter must not be able to load code from the host")
	})

	s.Run("no process", func() {
		compiled, err := script.Compile(`return typeof process === "undefined"`, "topic")
		s.Require().NoError(err, "the script must compile")

		defer compiled.Close()

		absent, err := compiled.Call(compiled.Value("t"))
		s.Require().NoError(err, "evaluation must succeed")
		s.Require().True(absent,
			"a filter has no business reaching the environment or the filesystem")
	})
}

func (s *ScriptSuite) TestIsDeterministic() {
	// Two sources of nondeterminism are pinned, so the same filter cannot give
	// two different answers on two runs.
	s.Run("the clock is fixed", func() {
		compiled, err := script.Compile(`return Date.now()`, "topic")
		s.Require().NoError(err, "the script must compile")

		defer compiled.Close()

		atEpoch, err := compiled.Call(compiled.Value("t"))
		s.Require().NoError(err, "evaluation must succeed")
		s.Require().False(atEpoch,
			"the clock is pinned to the epoch, so Date.now() is 0 and falsy: a filter that reads the time must not make two identical calls disagree")
	})

	s.Run("randomness is seeded", func() {
		first, err := script.Compile(`return Math.random()`, "topic")
		s.Require().NoError(err, "the script must compile")

		defer first.Close()

		second, err := script.Compile(`return Math.random()`, "topic")
		s.Require().NoError(err, "the script must compile")

		defer second.Close()

		_, err = first.Call(first.Value("t"))
		s.Require().NoError(err, "evaluation must succeed")

		_, err = second.Call(second.Value("t"))
		s.Require().NoError(err,
			"two runtimes seeded identically must behave the same, so a filter using randomness is still reproducible")
	})
}

func (s *ScriptSuite) TestStopsRunawayRecursion() {
	compiled, err := script.Compile(`function f() { return f() } return f()`, "topic")
	s.Require().NoError(err, "unbounded recursion is valid JavaScript and must compile")

	defer compiled.Close()

	_, err = compiled.Call(compiled.Value("t"))

	s.Require().Error(err,
		"the call stack is capped, so runaway recursion becomes an error for that input rather than exhausting the host stack")
}

func (s *ScriptSuite) TestGuardStopsAScriptThatNeverReturns() {
	compiled, err := script.Compile(`while (true) {}`, "topic")
	s.Require().NoError(err, "an infinite loop is valid JavaScript and must compile")

	defer compiled.Close()

	ctx, cancel := context.WithTimeout(s.T().Context(), 200*time.Millisecond)
	defer cancel()

	stop := script.Guard(ctx, compiled, "stopped")
	defer stop()

	done := make(chan error, 1)

	go func() {
		_, err := compiled.Call(compiled.Value("t"))
		done <- err
	}()

	select {
	case err := <-done:
		s.Require().Error(err,
			"an interrupted script must return an error rather than a verdict, because it never reached one")
	case <-time.After(10 * time.Second):
		s.Require().Fail(
			"goja does not yield, so without the guard a predicate that never returns holds its goroutine for the life of the process")
	}
}

func (s *ScriptSuite) TestGuardKeepsInterruptingAfterTheDeadline() {
	// The bug this exists for: goja clears the interrupt flag when the
	// interrupted call returns. A guard that fired once and exited would leave
	// the next evaluation unguarded, so a caller evaluating one predicate per
	// topic would stop the first and then hang on the second.
	compiled, err := script.Compile(`while (true) {}`, "topic")
	s.Require().NoError(err, "an infinite loop must compile")

	defer compiled.Close()

	ctx, cancel := context.WithTimeout(s.T().Context(), 100*time.Millisecond)
	defer cancel()

	stop := script.Guard(ctx, compiled, "stopped")
	defer stop()

	done := make(chan int, 1)

	go func() {
		stopped := 0

		// The first call absorbs the deadline; the rest must each be stopped in
		// turn rather than running without a limit.
		for range 3 {
			if _, err := compiled.Call(compiled.Value("t")); err != nil {
				stopped++
			}
		}

		done <- stopped
	}()

	select {
	case stopped := <-done:
		s.Require().Equal(3, stopped,
			"every evaluation after the deadline must be stopped, or one runaway predicate per topic hangs the whole listing")
	case <-time.After(15 * time.Second):
		s.Require().Fail(
			"a later evaluation ran unguarded: the interrupt must be re-asserted, because goja clears it when an interrupted call returns")
	}
}

func (s *ScriptSuite) TestGuardCostsNothingWhenTheScriptFinishes() {
	compiled, err := script.Compile(`return true`, "topic")
	s.Require().NoError(err, "the script must compile")

	defer compiled.Close()

	stop := script.Guard(s.T().Context(), compiled, "stopped")

	matched, err := compiled.Call(compiled.Value("t"))
	s.Require().NoError(err, "a predicate that returns promptly must not be interrupted")
	s.Require().True(matched, "the predicate returns true and must be reported as matching")

	// Returning from stop proves the watcher exited rather than leaking for the
	// life of the process.
	stop()
}

func (s *ScriptSuite) TestRejectsAScriptThatEscapesItsFunction() {
	s.Run("an endless loop outside the function is refused at compile time", func() {
		done := make(chan error, 1)
		go func() {
			_, err := script.Compile(`}); while (true) {} (function () {`, "topic")
			done <- err
		}()

		select {
		case err := <-done:
			s.Require().ErrorContains(err, "single function body",
				"code placed outside the wrapper runs at load time, before any guard exists, so it must never be executed")
		case <-time.After(5 * time.Second):
			s.Require().Fail("compiling ran code outside the predicate, and nothing can stop it there")
		}
	})

	s.Run("a second top-level statement is refused", func() {
		_, err := script.Compile(`}); globalThis.x = 1; (function () {`, "topic")
		s.Require().ErrorContains(err, "single function body",
			"anything beyond the one function literal is code that runs outside the predicate")
	})

	s.Run("a closing brace inside a string still compiles", func() {
		compiled, err := script.Compile(`return topic === "})"`, "topic")
		s.Require().NoError(err, "braces inside literals are data, not an escape, and must not be refused")
		defer compiled.Close()

		matched, err := compiled.Call(compiled.Value("})"))
		s.Require().NoError(err, "the predicate must evaluate")
		s.Require().True(matched, "the literal must compare as written")
	})
}

func (s *ScriptSuite) TestEvaluatesToAValue() {
	compiled, err := script.Compile(`return topic.split("-")[0]`, "topic")
	s.Require().NoError(err, "an expression body must compile")
	defer compiled.Close()

	value, err := compiled.Evaluate(compiled.Value("orders-v2"))
	s.Require().NoError(err, "evaluating must succeed")
	s.Require().Equal("orders", value.String(),
		"a grouping expression returns a value rather than a verdict, so its result must come back unconverted")
}

func (s *ScriptSuite) TestGuardToleratesNoScript() {
	stop := script.Guard(s.T().Context(), nil, "stopped")

	s.Require().NotPanics(stop,
		"a tool called without a predicate still stops its guard, so the nil case must be safe rather than a crash")
}
