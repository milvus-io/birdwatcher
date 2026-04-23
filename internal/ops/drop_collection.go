package ops

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

type DropCollectionParams struct {
	Name string `yaml:"name"`
}

func (p *DropCollectionParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	if rc.Client == nil {
		return nil, errNoClient("drop_collection")
	}
	if p.Name == "" {
		return nil, fmt.Errorf("drop_collection: `name` required")
	}
	if err := rc.Client.DropCollection(ctx, milvusclient.NewDropCollectionOption(p.Name)); err != nil {
		return nil, err
	}
	fmt.Fprintf(rc.Out(), "collection %q dropped\n", p.Name)
	return map[string]any{"name": p.Name}, nil
}

func init() {
	Register("drop_collection", func() Op { return &DropCollectionParams{} })
}
