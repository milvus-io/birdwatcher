package ops

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

type QueryParams struct {
	Collection       string   `yaml:"collection"`
	Filter           string   `yaml:"filter,omitempty"`
	OutputFields     []string `yaml:"output_fields"`
	Partitions       []string `yaml:"partitions,omitempty"`
	Limit            int      `yaml:"limit,omitempty"`
	Offset           int      `yaml:"offset,omitempty"`
	ConsistencyLevel string   `yaml:"consistency,omitempty"`
}

type QueryResult struct {
	RowCount int `yaml:"row_count"`
}

func (p *QueryParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	if rc.Client == nil {
		return nil, errNoClient("query")
	}
	if p.Collection == "" {
		return nil, fmt.Errorf("query: `collection` required")
	}
	if len(p.OutputFields) == 0 {
		return nil, fmt.Errorf("query: at least one output field required")
	}
	opt := milvusclient.NewQueryOption(p.Collection).WithOutputFields(p.OutputFields...)
	if p.Filter != "" {
		opt = opt.WithFilter(p.Filter)
	}
	if p.Limit > 0 {
		opt = opt.WithLimit(p.Limit)
	}
	if p.Offset > 0 {
		opt = opt.WithOffset(p.Offset)
	}
	if len(p.Partitions) > 0 {
		opt = opt.WithPartitions(p.Partitions...)
	}
	if cl, ok := parseConsistency(p.ConsistencyLevel); ok {
		opt = opt.WithConsistencyLevel(cl)
	}
	rs, err := rc.Client.Query(ctx, opt)
	if err != nil {
		return nil, err
	}
	printQueryResult(rc.Out(), &rs, p.OutputFields)
	return QueryResult{RowCount: rs.ResultCount}, nil
}

func printQueryResult(out interface{ Write([]byte) (int, error) }, rs *milvusclient.ResultSet, fields []string) {
	n := rs.ResultCount
	fmt.Fprintf(out, "query returned %d rows\n", n)
	for i := 0; i < n; i++ {
		fmt.Fprintf(out, "  [%d]", i)
		for _, f := range fields {
			if col := rs.GetColumn(f); col != nil {
				v, _ := col.Get(i)
				fmt.Fprintf(out, " %s=%v", f, v)
			}
		}
		fmt.Fprintln(out)
	}
}

func init() {
	Register("query", func() Op { return &QueryParams{} })
}
