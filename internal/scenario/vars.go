package scenario

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// varExpr matches ${...}. Inner captures are kept liberal — splitting is
// done later against dotted segments.
var varExpr = regexp.MustCompile(`\$\{([^}]+)\}`)

// Resolver supplies values for ${vars.*}, ${env.*}, ${steps.*.*}.
type Resolver struct {
	Vars        map[string]any
	Env         func(string) string
	StepResults map[string]any
}

// DefaultResolver uses os.Getenv.
func DefaultResolver(vars, steps map[string]any) *Resolver {
	return &Resolver{Vars: vars, StepResults: steps, Env: os.Getenv}
}

// Interpolate walks a yaml.Node tree and substitutes ${...} references on
// string scalars in place. Scalars that were originally typed (int, bool,
// float) parse back to those types after substitution because we leave
// the node tag alone — yaml's implicit re-typing on Decode handles it.
func (r *Resolver) Interpolate(node *yaml.Node) error {
	if node == nil {
		return nil
	}
	switch node.Kind {
	case yaml.DocumentNode, yaml.SequenceNode:
		for _, c := range node.Content {
			if err := r.Interpolate(c); err != nil {
				return err
			}
		}
	case yaml.MappingNode:
		for _, c := range node.Content {
			if err := r.Interpolate(c); err != nil {
				return err
			}
		}
	case yaml.ScalarNode:
		if !strings.Contains(node.Value, "${") {
			return nil
		}
		expanded, typed, err := r.expand(node.Value)
		if err != nil {
			return err
		}
		node.Value = expanded
		if typed {
			// Force re-detection of scalar type (e.g. int, bool).
			node.Tag = ""
			node.Style = 0
		}
	}
	return nil
}

// expand replaces all ${...} occurrences. Returns (newString, fullyTyped).
// `fullyTyped` is true when the entire input was a single ${...} and the
// resolved value wasn't already a string — in that case we clear the
// node tag so YAML decodes it with its natural type.
func (r *Resolver) expand(s string) (string, bool, error) {
	// Fast path: single-expression input like "${vars.dim}".
	if m := varExpr.FindStringSubmatchIndex(s); m != nil && m[0] == 0 && m[1] == len(s) {
		ref := s[m[2]:m[3]]
		v, err := r.lookup(ref)
		if err != nil {
			return "", false, err
		}
		return scalarString(v), !isString(v), nil
	}
	var firstErr error
	out := varExpr.ReplaceAllStringFunc(s, func(match string) string {
		inner := match[2 : len(match)-1]
		v, err := r.lookup(inner)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			return match
		}
		return scalarString(v)
	})
	return out, false, firstErr
}

// lookup resolves dotted references. Supports:
//
//	vars.<key>
//	env.<KEY>
//	steps.<name>.<field>     (field may be a simple path inside the step
//	                          result's map; complex traversal is v2)
func (r *Resolver) lookup(ref string) (any, error) {
	ref = strings.TrimSpace(ref)
	parts := strings.SplitN(ref, ".", 2)
	if len(parts) < 2 {
		return nil, fmt.Errorf("bad reference %q", ref)
	}
	switch parts[0] {
	case "vars":
		v, ok := r.Vars[parts[1]]
		if !ok {
			return nil, fmt.Errorf("undefined var %q", parts[1])
		}
		return v, nil
	case "env":
		if r.Env == nil {
			return "", nil
		}
		return r.Env(parts[1]), nil
	case "steps":
		stepParts := strings.SplitN(parts[1], ".", 2)
		if len(stepParts) != 2 {
			return nil, fmt.Errorf("step reference must be steps.<name>.<field>")
		}
		res, ok := r.StepResults[stepParts[0]]
		if !ok {
			return nil, fmt.Errorf("unknown step %q", stepParts[0])
		}
		return lookupField(res, stepParts[1])
	}
	return nil, fmt.Errorf("unknown namespace %q in %q", parts[0], ref)
}

// lookupField picks a field out of a step result — supports map access
// and falls back to reflection-ish behavior for the small number of
// typed result structs in internal/ops.
func lookupField(val any, field string) (any, error) {
	if m, ok := val.(map[string]any); ok {
		v, ok := m[field]
		if !ok {
			return nil, fmt.Errorf("field %q not in step result", field)
		}
		return v, nil
	}
	// Marshal→unmarshal trick: lets us index any yaml-tagged struct by name
	// without importing its type here.
	bs, err := yaml.Marshal(val)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := yaml.Unmarshal(bs, &m); err != nil {
		return nil, fmt.Errorf("step result not addressable: %w", err)
	}
	v, ok := m[field]
	if !ok {
		return nil, fmt.Errorf("field %q not in step result", field)
	}
	return v, nil
}

func scalarString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", t)
	}
}

func isString(v any) bool {
	_, ok := v.(string)
	return ok
}
