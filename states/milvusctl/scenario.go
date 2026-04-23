package milvusctl

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/internal/ops"
	"github.com/milvus-io/birdwatcher/internal/scenario"
)

type RunScenarioParam struct {
	framework.ParamBase `use:"run scenario" desc:"run a YAML scenario of ops"`
	File                string   `name:"file" default:"" desc:"scenario YAML path"`
	Vars                []string `name:"var" desc:"override var, k=v (repeatable)"`
	ContinueOnError     bool     `name:"continue-on-error" default:"false" desc:"override scenario stop_on_error"`
}

func (s *MilvusctlState) RunScenarioCommand(ctx context.Context, p *RunScenarioParam) error {
	if p.File == "" {
		return fmt.Errorf("--file is required")
	}
	sc, err := scenario.Load(p.File)
	if err != nil {
		return err
	}
	overrides, err := parseVarOverrides(p.Vars)
	if err != nil {
		return err
	}
	rc := &ops.RunContext{
		Client: s.client,
		Vars:   overrides,
		Stdout: os.Stdout,
		Rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	if p.ContinueOnError {
		f := false
		sc.StopOnError = &f
	}
	runner := &scenario.Runner{Out: os.Stdout}
	return runner.Run(ctx, sc, rc)
}

func parseVarOverrides(raws []string) (map[string]any, error) {
	out := map[string]any{}
	for _, kv := range raws {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		eq := strings.IndexByte(kv, '=')
		if eq <= 0 {
			return nil, fmt.Errorf("bad --var %q, want key=value", kv)
		}
		out[strings.TrimSpace(kv[:eq])] = coerceScalar(strings.TrimSpace(kv[eq+1:]))
	}
	return out, nil
}
