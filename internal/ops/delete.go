package ops

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

type DeleteParams struct {
	Collection string `yaml:"collection"`
	Partition  string `yaml:"partition,omitempty"`
	Filter     string `yaml:"filter,omitempty"`
	// PKField only needed when deleting by IDs; defaults to primary key.
	PKField   string   `yaml:"pk_field,omitempty"`
	Int64IDs  []int64  `yaml:"int64_ids,omitempty"`
	StringIDs []string `yaml:"string_ids,omitempty"`
}

type DeleteResult struct {
	Count int64 `yaml:"count"`
}

func (p *DeleteParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	if rc.Client == nil {
		return nil, errNoClient("delete")
	}
	if p.Collection == "" {
		return nil, fmt.Errorf("delete: `collection` required")
	}
	if p.Filter == "" && len(p.Int64IDs) == 0 && len(p.StringIDs) == 0 {
		return nil, fmt.Errorf("delete: one of `filter`, `int64_ids`, `string_ids` required")
	}
	opt := milvusclient.NewDeleteOption(p.Collection)
	if p.Partition != "" {
		opt = opt.WithPartition(p.Partition)
	}
	if p.Filter != "" {
		opt = opt.WithExpr(p.Filter)
	}
	if len(p.Int64IDs) > 0 {
		opt = opt.WithInt64IDs(p.PKField, p.Int64IDs)
	}
	if len(p.StringIDs) > 0 {
		opt = opt.WithStringIDs(p.PKField, p.StringIDs)
	}
	res, err := rc.Client.Delete(ctx, opt)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(rc.Out(), "deleted %d rows from %q\n", res.DeleteCount, p.Collection)
	return DeleteResult{Count: res.DeleteCount}, nil
}

func init() {
	Register("delete", func() Op { return &DeleteParams{} })
}
