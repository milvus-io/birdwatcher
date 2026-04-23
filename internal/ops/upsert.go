package ops

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

type UpsertParams struct {
	Collection    string       `yaml:"collection"`
	Partition     string       `yaml:"partition,omitempty"`
	Columns       []ColumnSpec `yaml:"columns"`
	PartialUpdate bool         `yaml:"partial_update,omitempty"`
}

type UpsertResult struct {
	Count int64 `yaml:"count"`
}

func (p *UpsertParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	if rc.Client == nil {
		return nil, errNoClient("upsert")
	}
	if p.Collection == "" {
		return nil, fmt.Errorf("upsert: `collection` required")
	}
	if len(p.Columns) == 0 {
		return nil, fmt.Errorf("upsert: at least one column required")
	}
	cols, err := buildColumns(p.Columns, rc)
	if err != nil {
		return nil, err
	}
	opt := milvusclient.NewColumnBasedInsertOption(p.Collection, cols...)
	if p.Partition != "" {
		opt = opt.WithPartition(p.Partition)
	}
	if p.PartialUpdate {
		opt = opt.WithPartialUpdate(true)
	}
	res, err := rc.Client.Upsert(ctx, opt)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(rc.Out(), "upserted %d rows into %q\n", res.UpsertCount, p.Collection)
	return UpsertResult{Count: res.UpsertCount}, nil
}

func init() {
	Register("upsert", func() Op { return &UpsertParams{} })
}
