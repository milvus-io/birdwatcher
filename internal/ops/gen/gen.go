// Package gen provides deterministic data generators used by YAML scenarios
// and by CLI flag-based `--gen field=spec` inputs. Every generator receives
// the scenario's *rand.Rand so runs can be reproduced with a top-level seed.
package gen

import (
	"fmt"
	"math/rand"

	"gopkg.in/yaml.v3"
)

// Spec describes one generator invocation. `Type` names the generator
// (matched against the registry); the remaining YAML keys land in `Args`.
type Spec struct {
	Type string
	Seed *int64
	Args map[string]any
}

// UnmarshalYAML lifts the `type`/`seed` keys out and stashes the rest in
// Args, so generators don't need custom Go structs for their parameters.
func (s *Spec) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("gen spec must be a mapping, got kind %d", node.Kind)
	}
	s.Args = map[string]any{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		k := node.Content[i]
		v := node.Content[i+1]
		switch k.Value {
		case "type":
			if err := v.Decode(&s.Type); err != nil {
				return err
			}
		case "seed":
			var sd int64
			if err := v.Decode(&sd); err != nil {
				return err
			}
			s.Seed = &sd
		default:
			var val any
			if err := v.Decode(&val); err != nil {
				return err
			}
			s.Args[k.Value] = val
		}
	}
	if s.Type == "" {
		return fmt.Errorf("gen spec missing type")
	}
	return nil
}

// Generator produces a typed slice (e.g. []int64, [][]float32) from a Spec.
type Generator interface {
	Generate(spec *Spec, r *rand.Rand) (any, error)
}

var registry = map[string]Generator{}

func Register(name string, g Generator) {
	if _, ok := registry[name]; ok {
		panic("gen: duplicate " + name)
	}
	registry[name] = g
}

// Resolve looks up the generator and runs it. If the spec sets its own
// seed, a fresh *rand.Rand is used for determinism independent of the
// scenario-wide rng.
func Resolve(spec *Spec, r *rand.Rand) (any, error) {
	if spec == nil {
		return nil, fmt.Errorf("nil gen spec")
	}
	g, ok := registry[spec.Type]
	if !ok {
		return nil, fmt.Errorf("unknown generator %q", spec.Type)
	}
	if spec.Seed != nil {
		r = rand.New(rand.NewSource(*spec.Seed))
	}
	if r == nil {
		r = rand.New(rand.NewSource(1))
	}
	return g.Generate(spec, r)
}

// argInt fetches an int-like arg with a default; supports YAML's int/float
// unification.
func argInt(args map[string]any, key string, def int) int {
	v, ok := args[key]
	if !ok {
		return def
	}
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case string:
		var out int
		fmt.Sscanf(n, "%d", &out)
		return out
	}
	return def
}

func argInt64(args map[string]any, key string, def int64) int64 {
	v, ok := args[key]
	if !ok {
		return def
	}
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case string:
		var out int64
		fmt.Sscanf(n, "%d", &out)
		return out
	}
	return def
}

func argFloat64(args map[string]any, key string, def float64) float64 {
	v, ok := args[key]
	if !ok {
		return def
	}
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	return def
}

func argString(args map[string]any, key, def string) string {
	if v, ok := args[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return def
}

func requireCount(spec *Spec) (int, error) {
	n := argInt(spec.Args, "count", 0)
	if n <= 0 {
		return 0, fmt.Errorf("%s: `count` must be > 0", spec.Type)
	}
	return n, nil
}
