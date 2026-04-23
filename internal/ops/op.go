package ops

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

// ErrNotImplemented is returned by ops that are reserved for v2.
var ErrNotImplemented = errors.New("op not implemented in v1")

// RunContext is passed to every op. It carries the client, scenario-scoped
// state (vars, previous step results, rng) and an output writer used by ops
// to print human-readable results (both CLI and scenario go through this).
type RunContext struct {
	Client      *milvusclient.Client
	OwnsClient  bool
	Vars        map[string]any
	StepResults map[string]any
	Rand        *rand.Rand
	Stdout      io.Writer
}

// Op is the shared execution unit. Each Milvus operation implements it,
// and both the CLI commands (states/instance.go) and the YAML scenario
// runner (internal/scenario) call it the same way.
type Op interface {
	Execute(ctx context.Context, rc *RunContext) (any, error)
}

// Factory constructs a fresh Op. The scenario runner uses factories because
// each step decodes its params into a new Op instance.
type Factory func() Op

var registry = map[string]Factory{}

// Register binds an op name (the `op:` key in YAML) to a factory. Called
// from init() of each op file.
func Register(name string, f Factory) {
	if _, ok := registry[name]; ok {
		panic(fmt.Sprintf("ops: duplicate registration %q", name))
	}
	registry[name] = f
}

// New constructs an op by name. Used by the scenario runner.
func New(name string) (Op, bool) {
	f, ok := registry[name]
	if !ok {
		return nil, false
	}
	return f(), true
}

// RegisteredNames lists all known op names, useful for diagnostics.
func RegisteredNames() []string {
	out := make([]string, 0, len(registry))
	for n := range registry {
		out = append(out, n)
	}
	return out
}

// Out returns the RunContext's stdout or falls back to io.Discard.
func (rc *RunContext) Out() io.Writer {
	if rc == nil || rc.Stdout == nil {
		return io.Discard
	}
	return rc.Stdout
}
