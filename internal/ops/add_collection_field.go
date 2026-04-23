package ops

import (
	"context"
	"fmt"

	schemapkg "github.com/milvus-io/birdwatcher/internal/schema"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

// AddFieldParams adds one field to an existing collection. Milvus currently
// requires newly added fields to be nullable, which we default on so
// scenario writers don't have to remember.
type AddFieldParams struct {
	Collection string              `yaml:"collection"`
	Field      schemapkg.FieldSpec `yaml:"field"`
}

func (p *AddFieldParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	if rc.Client == nil {
		return nil, errNoClient("add_collection_field")
	}
	if p.Collection == "" {
		return nil, fmt.Errorf("add_collection_field: `collection` required")
	}
	if !p.Field.Nullable {
		p.Field.Nullable = true
	}
	f, err := schemapkg.BuildField(p.Field)
	if err != nil {
		return nil, err
	}
	if err := rc.Client.AddCollectionField(ctx, milvusclient.NewAddCollectionFieldOption(p.Collection, f)); err != nil {
		return nil, err
	}
	fmt.Fprintf(rc.Out(), "field %q added to collection %q\n", p.Field.Name, p.Collection)
	return map[string]any{"collection": p.Collection, "field": p.Field.Name}, nil
}

func init() {
	Register("add_collection_field", func() Op { return &AddFieldParams{} })
}
