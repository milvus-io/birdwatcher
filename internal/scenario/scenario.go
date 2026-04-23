// Package scenario implements a YAML-driven executor for milvusctl ops.
// A scenario is a named list of steps; each step names an op (from
// internal/ops) and a parameter block. Variables, environment, and prior
// step results can be interpolated into any scalar with ${...} syntax.
package scenario

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Scenario is the top-level YAML document.
type Scenario struct {
	Name        string         `yaml:"name,omitempty"`
	Description string         `yaml:"description,omitempty"`
	Seed        *int64         `yaml:"seed,omitempty"`
	Vars        map[string]any `yaml:"vars,omitempty"`
	StopOnError *bool          `yaml:"stop_on_error,omitempty"`
	Steps       []Step         `yaml:"steps"`
}

// Step is a single operation. `Params` is kept as a raw yaml.Node so we can
// interpolate `${...}` references against per-run state before decoding
// into the op's typed struct.
type Step struct {
	Name    string    `yaml:"name,omitempty"`
	Op      string    `yaml:"op"`
	OnError string    `yaml:"on_error,omitempty"`
	Params  yaml.Node `yaml:"params,omitempty"`
}

// Load parses a scenario file. Variable interpolation happens per-step
// during Run, not here.
func Load(path string) (*Scenario, error) {
	bs, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read scenario: %w", err)
	}
	return Parse(bs)
}

// Parse parses scenario YAML bytes.
func Parse(data []byte) (*Scenario, error) {
	var s Scenario
	if err := yaml.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("parse scenario: %w", err)
	}
	if len(s.Steps) == 0 {
		return nil, fmt.Errorf("scenario has no steps")
	}
	for i, st := range s.Steps {
		if st.Op == "" {
			return nil, fmt.Errorf("step %d missing `op`", i)
		}
		if st.Name == "" {
			// Default step names so ${steps.<name>.*} always has something.
			s.Steps[i].Name = fmt.Sprintf("step_%d_%s", i, st.Op)
		}
	}
	return &s, nil
}

// ShouldStopOnError returns the effective stop-on-error flag.
func (s *Scenario) ShouldStopOnError() bool {
	if s.StopOnError != nil {
		return *s.StopOnError
	}
	return true
}
