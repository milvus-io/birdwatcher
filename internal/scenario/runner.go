package scenario

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"

	"github.com/milvus-io/birdwatcher/internal/ops"
)

// Runner runs a scenario against a RunContext. The context is owned by the
// caller; the runner only mutates its StepResults/Rand/Client fields.
type Runner struct {
	Out io.Writer
}

// Run executes the scenario top-to-bottom.
//
// Client ownership: if `rc.Client` is nil on entry, the scenario is
// expected to have a `connect` step that creates one; the runner marks it
// `OwnsClient` and closes it at exit. If `rc.Client` is set on entry
// (e.g. CLI is already connected), the runner does not close it.
func (r *Runner) Run(ctx context.Context, s *Scenario, rc *ops.RunContext) error {
	if rc.StepResults == nil {
		rc.StepResults = map[string]any{}
	}
	if rc.Vars == nil {
		rc.Vars = map[string]any{}
	}
	for k, v := range s.Vars {
		if _, exists := rc.Vars[k]; !exists {
			rc.Vars[k] = v
		}
	}
	if rc.Rand == nil {
		seed := time.Now().UnixNano()
		if s.Seed != nil {
			seed = *s.Seed
		}
		rc.Rand = rand.New(rand.NewSource(seed))
	}
	if rc.Stdout == nil {
		rc.Stdout = r.writerOr(os.Stdout)
	}

	initiallyOwned := rc.OwnsClient
	defer func() {
		// Only close if we created the client inside this scenario.
		if rc.OwnsClient && !initiallyOwned && rc.Client != nil {
			_ = rc.Client.Close(context.Background())
			rc.Client = nil
			rc.OwnsClient = false
		}
	}()

	stopOnError := s.ShouldStopOnError()

	for i, step := range s.Steps {
		resolver := DefaultResolver(rc.Vars, rc.StepResults)
		fmt.Fprintf(rc.Stdout, "\n=== step %d: %s (%s) ===\n", i, step.Name, step.Op)

		op, ok := ops.New(step.Op)
		if !ok {
			err := fmt.Errorf("unknown op %q", step.Op)
			if stopOnError && step.OnError != "ignore" && step.OnError != "continue" {
				return err
			}
			rc.StepResults[step.Name] = map[string]any{"error": err.Error()}
			fmt.Fprintf(rc.Stdout, "step %s: %v (skipped due to on_error)\n", step.Name, err)
			continue
		}

		// Deep-copy the params node so interpolation doesn't mutate the
		// original scenario across re-runs.
		paramsNode := copyNode(&step.Params)
		if err := resolver.Interpolate(paramsNode); err != nil {
			if !skipOnErr(stopOnError, step.OnError) {
				return fmt.Errorf("step %s: interpolate: %w", step.Name, err)
			}
			rc.StepResults[step.Name] = map[string]any{"error": err.Error()}
			fmt.Fprintf(rc.Stdout, "step %s: %v (skipped)\n", step.Name, err)
			continue
		}
		if !isEmptyNode(paramsNode) {
			if err := paramsNode.Decode(op); err != nil {
				if !skipOnErr(stopOnError, step.OnError) {
					return fmt.Errorf("step %s: decode params: %w", step.Name, err)
				}
				rc.StepResults[step.Name] = map[string]any{"error": err.Error()}
				fmt.Fprintf(rc.Stdout, "step %s: %v (skipped)\n", step.Name, err)
				continue
			}
		}

		res, err := op.Execute(ctx, rc)
		if err != nil {
			if errors.Is(err, ops.ErrNotImplemented) && step.OnError == "ignore" {
				fmt.Fprintf(rc.Stdout, "step %s: %v (ignored)\n", step.Name, err)
				rc.StepResults[step.Name] = map[string]any{"error": err.Error()}
				continue
			}
			if !skipOnErr(stopOnError, step.OnError) {
				return fmt.Errorf("step %s (%s): %w", step.Name, step.Op, err)
			}
			rc.StepResults[step.Name] = map[string]any{"error": err.Error()}
			fmt.Fprintf(rc.Stdout, "step %s: %v (ignored)\n", step.Name, err)
			continue
		}
		rc.StepResults[step.Name] = res
	}
	return nil
}

// skipOnErr returns true when a step-level on_error setting allows the
// runner to continue past a failure, overriding the scenario-level
// stop_on_error.
func skipOnErr(stopOnError bool, onErr string) bool {
	switch onErr {
	case "ignore", "continue":
		return true
	case "fail":
		return false
	}
	return !stopOnError
}

func (r *Runner) writerOr(def io.Writer) io.Writer {
	if r.Out != nil {
		return r.Out
	}
	return def
}
