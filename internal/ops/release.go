package ops

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

type ReleaseCollectionParams struct {
	Name string `yaml:"name"`
}

func (p *ReleaseCollectionParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	if rc.Client == nil {
		return nil, errNoClient("release_collection")
	}
	if p.Name == "" {
		return nil, fmt.Errorf("release_collection: `name` required")
	}
	if err := rc.Client.ReleaseCollection(ctx, milvusclient.NewReleaseCollectionOption(p.Name)); err != nil {
		return nil, err
	}
	fmt.Fprintf(rc.Out(), "collection %q released\n", p.Name)
	return map[string]any{"name": p.Name}, nil
}

func init() {
	Register("release_collection", func() Op { return &ReleaseCollectionParams{} })
}
