package ops

import (
	"context"
	"fmt"

	schemapkg "github.com/milvus-io/birdwatcher/internal/schema"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

// CreateCollectionParams creates a collection from either an inline schema,
// a schema file, or the quick-setup builder fields. Name on the params
// overrides the schema's own name when both are provided.
type CreateCollectionParams struct {
	Name         string                `yaml:"name"`
	SchemaFile   string                `yaml:"schema_file,omitempty"`
	Schema       *schemapkg.SchemaSpec `yaml:"schema,omitempty"`
	ShardNum     int32                 `yaml:"shard_num,omitempty"`
	Consistency  string                `yaml:"consistency,omitempty"`
	DynamicField *bool                 `yaml:"enable_dynamic_field,omitempty"`
}

func (p *CreateCollectionParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	if rc.Client == nil {
		return nil, errNoClient("create_collection")
	}
	sch, err := p.resolveSchema()
	if err != nil {
		return nil, err
	}
	if p.Name != "" {
		sch.WithName(p.Name)
	}
	if p.DynamicField != nil {
		sch.WithDynamicFieldEnabled(*p.DynamicField)
	}
	opt := milvusclient.NewCreateCollectionOption(sch.CollectionName, sch)
	if p.ShardNum > 0 {
		opt = opt.WithShardNum(p.ShardNum)
	}
	if cl, ok := parseConsistency(p.Consistency); ok {
		opt = opt.WithConsistencyLevel(cl)
	}
	if err := rc.Client.CreateCollection(ctx, opt); err != nil {
		return nil, err
	}
	fmt.Fprintf(rc.Out(), "collection %q created\n", sch.CollectionName)
	return map[string]any{"name": sch.CollectionName}, nil
}

func (p *CreateCollectionParams) resolveSchema() (*entity.Schema, error) {
	switch {
	case p.Schema != nil:
		if p.Name != "" && p.Schema.Name == "" {
			p.Schema.Name = p.Name
		}
		return schemapkg.BuildSchema(*p.Schema)
	case p.SchemaFile != "":
		return schemapkg.LoadFile(p.SchemaFile)
	default:
		return nil, fmt.Errorf("create_collection: `schema` or `schema_file` required")
	}
}

func init() {
	Register("create_collection", func() Op { return &CreateCollectionParams{} })
}
