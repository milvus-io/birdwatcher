package ops

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

type HasCollectionParams struct {
	Name string `yaml:"name"`
}

type HasCollectionResult struct {
	Name string `yaml:"name"`
	Has  bool   `yaml:"has"`
}

func (p *HasCollectionParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	if rc.Client == nil {
		return nil, errNoClient("has_collection")
	}
	if p.Name == "" {
		return nil, fmt.Errorf("has_collection: `name` required")
	}
	has, err := rc.Client.HasCollection(ctx, milvusclient.NewHasCollectionOption(p.Name))
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(rc.Out(), "collection %q exists=%v\n", p.Name, has)
	return HasCollectionResult{Name: p.Name, Has: has}, nil
}

func init() {
	Register("has_collection", func() Op { return &HasCollectionParams{} })
}
