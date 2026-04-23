package ops

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

type LoadCollectionParams struct {
	Name    string `yaml:"name"`
	Replica int    `yaml:"replica,omitempty"`
	Async   bool   `yaml:"async,omitempty"`
	Refresh bool   `yaml:"refresh,omitempty"`
}

func (p *LoadCollectionParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	if rc.Client == nil {
		return nil, errNoClient("load_collection")
	}
	if p.Name == "" {
		return nil, fmt.Errorf("load_collection: `name` required")
	}
	opt := milvusclient.NewLoadCollectionOption(p.Name)
	if p.Replica > 0 {
		opt = opt.WithReplica(p.Replica)
	}
	if p.Refresh {
		opt = opt.WithRefresh(true)
	}
	task, err := rc.Client.LoadCollection(ctx, opt)
	if err != nil {
		return nil, err
	}
	if !p.Async {
		if err := task.Await(ctx); err != nil {
			return nil, err
		}
	}
	fmt.Fprintf(rc.Out(), "collection %q loaded\n", p.Name)
	return map[string]any{"name": p.Name}, nil
}

func init() {
	Register("load_collection", func() Op { return &LoadCollectionParams{} })
}
