package ops

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

type FlushParams struct {
	Name  string `yaml:"name"`
	Async bool   `yaml:"async,omitempty"`
}

func (p *FlushParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	if rc.Client == nil {
		return nil, errNoClient("flush")
	}
	if p.Name == "" {
		return nil, fmt.Errorf("flush: `name` required")
	}
	task, err := rc.Client.Flush(ctx, milvusclient.NewFlushOption(p.Name))
	if err != nil {
		return nil, err
	}
	if !p.Async {
		if err := task.Await(ctx); err != nil {
			return nil, err
		}
	}
	fmt.Fprintf(rc.Out(), "collection %q flushed\n", p.Name)
	return map[string]any{"name": p.Name}, nil
}

func init() {
	Register("flush", func() Op { return &FlushParams{} })
}
