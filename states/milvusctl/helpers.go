package milvusctl

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/milvus-io/birdwatcher/internal/ops"
	"github.com/milvus-io/birdwatcher/internal/ops/gen"
)

// runCtx builds the shared RunContext used by every CLI command so ops
// produced by CLI or YAML behave identically (same stdout, same client).
func (s *MilvusctlState) runCtx() *ops.RunContext {
	return &ops.RunContext{Client: s.client, Stdout: os.Stdout}
}

// executeOp is the one-line adapter used by every `*Command` method
// so there's zero divergence between CLI and scenario execution paths.
func (s *MilvusctlState) executeOp(ctx context.Context, op ops.Op) error {
	_, err := op.Execute(ctx, s.runCtx())
	return err
}

// endpoint formats the connection address with optional database name.
func (s *MilvusctlState) endpoint() string {
	if s.clientcfg.DBName != "" {
		return s.clientcfg.Address + "/" + s.clientcfg.DBName
	}
	return s.clientcfg.Address
}

// parseKV turns "a=1,b=2.5" into {a: 1, b: 2.5} using heuristic typing.
func parseKV(s string) (map[string]any, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	out := map[string]any{}
	for _, kv := range strings.Split(s, ",") {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		eq := strings.IndexByte(kv, '=')
		if eq <= 0 {
			return nil, fmt.Errorf("bad kv %q, want key=value", kv)
		}
		k := strings.TrimSpace(kv[:eq])
		v := strings.TrimSpace(kv[eq+1:])
		out[k] = coerceScalar(v)
	}
	return out, nil
}

// parseKVString is like parseKV but every value stays a string (needed
// for Search WithSearchParam which takes string/string).
func parseKVString(s string) (map[string]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	out := map[string]string{}
	for _, kv := range strings.Split(s, ",") {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		eq := strings.IndexByte(kv, '=')
		if eq <= 0 {
			return nil, fmt.Errorf("bad kv %q, want key=value", kv)
		}
		out[strings.TrimSpace(kv[:eq])] = strings.TrimSpace(kv[eq+1:])
	}
	return out, nil
}

// coerceScalar best-effort parses "123", "1.5", "true" into int/float/bool.
func coerceScalar(s string) any {
	if s == "true" {
		return true
	}
	if s == "false" {
		return false
	}
	var i int64
	if _, err := fmt.Sscanf(s, "%d", &i); err == nil && fmt.Sprintf("%d", i) == s {
		return i
	}
	var f float64
	if _, err := fmt.Sscanf(s, "%f", &f); err == nil {
		return f
	}
	return s
}

// parseGenSpecs parses multiple CLI --gen strings into ops.ColumnSpec.
// Format: `<field>=<gen_type>[:k=v[:k=v]...]`.
func parseGenSpecs(raws []string) ([]ops.ColumnSpec, error) {
	out := make([]ops.ColumnSpec, 0, len(raws))
	for _, r := range raws {
		eq := strings.IndexByte(r, '=')
		if eq <= 0 {
			return nil, fmt.Errorf("bad --gen %q, want field=gen_type:key=value", r)
		}
		field := r[:eq]
		spec, err := parseGenSpec(r[eq+1:])
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", field, err)
		}
		out = append(out, ops.ColumnSpec{Field: field, Gen: spec})
	}
	return out, nil
}

// parseGenSpec parses "gen_type:k=v:k=v" into a gen.Spec.
func parseGenSpec(s string) (*gen.Spec, error) {
	parts := strings.Split(s, ":")
	if len(parts) == 0 || parts[0] == "" {
		return nil, fmt.Errorf("empty gen spec")
	}
	out := &gen.Spec{Type: parts[0], Args: map[string]any{}}
	for _, kv := range parts[1:] {
		if kv == "" {
			continue
		}
		eq := strings.IndexByte(kv, '=')
		if eq <= 0 {
			return nil, fmt.Errorf("bad kv %q, want key=value", kv)
		}
		out.Args[strings.TrimSpace(kv[:eq])] = coerceScalar(strings.TrimSpace(kv[eq+1:]))
	}
	return out, nil
}
