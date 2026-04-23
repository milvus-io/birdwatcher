package ops

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/internal/ops/gen"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

// VectorSpec supplies one (or many) query vectors. A single spec can emit
// multiple vectors via its generator's `count`, which is then flattened
// into the []entity.Vector passed to SDK Search.
type VectorSpec struct {
	Gen  *gen.Spec   `yaml:"gen,omitempty"`
	Data [][]float32 `yaml:"data,omitempty"`
}

type SearchParams struct {
	Collection       string            `yaml:"collection"`
	AnnsField        string            `yaml:"anns_field"`
	TopK             int               `yaml:"topk"`
	Metric           string            `yaml:"metric,omitempty"`
	Filter           string            `yaml:"filter,omitempty"`
	OutputFields     []string          `yaml:"output_fields,omitempty"`
	QueryVectors     []VectorSpec      `yaml:"query_vectors"`
	SearchParams     map[string]string `yaml:"params,omitempty"`
	Partitions       []string          `yaml:"partitions,omitempty"`
	Offset           int               `yaml:"offset,omitempty"`
	ConsistencyLevel string            `yaml:"consistency,omitempty"`
	GroupByField     string            `yaml:"group_by,omitempty"`
}

type SearchResult struct {
	GroupCount int `yaml:"group_count"`
	TotalHits  int `yaml:"total_hits"`
}

func (p *SearchParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	if rc.Client == nil {
		return nil, errNoClient("search")
	}
	if p.Collection == "" {
		return nil, fmt.Errorf("search: `collection` required")
	}
	if p.TopK <= 0 {
		p.TopK = 10
	}
	if len(p.QueryVectors) == 0 {
		return nil, fmt.Errorf("search: at least one query vector required")
	}
	vecs, err := resolveQueryVectors(p.QueryVectors, rc)
	if err != nil {
		return nil, err
	}
	opt := milvusclient.NewSearchOption(p.Collection, p.TopK, vecs)
	if p.AnnsField != "" {
		opt = opt.WithANNSField(p.AnnsField)
	}
	if p.Filter != "" {
		opt = opt.WithFilter(p.Filter)
	}
	if len(p.OutputFields) > 0 {
		opt = opt.WithOutputFields(p.OutputFields...)
	}
	if len(p.Partitions) > 0 {
		opt = opt.WithPartitions(p.Partitions...)
	}
	if p.Offset > 0 {
		opt = opt.WithOffset(p.Offset)
	}
	if p.GroupByField != "" {
		opt = opt.WithGroupByField(p.GroupByField)
	}
	if cl, ok := parseConsistency(p.ConsistencyLevel); ok {
		opt = opt.WithConsistencyLevel(cl)
	}
	for k, v := range p.SearchParams {
		opt = opt.WithSearchParam(k, v)
	}
	results, err := rc.Client.Search(ctx, opt)
	if err != nil {
		return nil, err
	}
	total := 0
	for i, rs := range results {
		total += rs.ResultCount
		fmt.Fprintf(rc.Out(), "query %d: %d hits\n", i, rs.ResultCount)
		printResultSet(rc.Out(), &rs, p.OutputFields)
	}
	return SearchResult{GroupCount: len(results), TotalHits: total}, nil
}

// resolveQueryVectors resolves VectorSpecs into entity.Vector. Each spec's
// generator (or literal data) produces one or more [][]float32 rows, which
// are appended as individual entity.FloatVector items.
func resolveQueryVectors(specs []VectorSpec, rc *RunContext) ([]entity.Vector, error) {
	var out []entity.Vector
	for i, s := range specs {
		rows, err := resolveVectorSpec(s, rc)
		if err != nil {
			return nil, fmt.Errorf("query_vectors[%d]: %w", i, err)
		}
		for _, r := range rows {
			out = append(out, entity.FloatVector(r))
		}
	}
	return out, nil
}

func resolveVectorSpec(s VectorSpec, rc *RunContext) ([][]float32, error) {
	if s.Gen != nil {
		raw, err := gen.Resolve(s.Gen, rc.Rand)
		if err != nil {
			return nil, err
		}
		rows, ok := raw.([][]float32)
		if !ok {
			return nil, fmt.Errorf("generator %q produced %T, want [][]float32", s.Gen.Type, raw)
		}
		return rows, nil
	}
	if len(s.Data) > 0 {
		return s.Data, nil
	}
	return nil, fmt.Errorf("vector spec missing `gen` or `data`")
}

func printResultSet(out interface{ Write([]byte) (int, error) }, rs *milvusclient.ResultSet, outputFields []string) {
	if rs.IDs != nil {
		for i := 0; i < rs.IDs.Len(); i++ {
			id, _ := rs.IDs.Get(i)
			score := float32(0)
			if i < len(rs.Scores) {
				score = rs.Scores[i]
			}
			fmt.Fprintf(out, "  [%d] id=%v score=%.4f", i, id, score)
			for _, f := range outputFields {
				if col := rs.GetColumn(f); col != nil {
					v, _ := col.Get(i)
					fmt.Fprintf(out, " %s=%v", f, v)
				}
			}
			fmt.Fprintln(out)
		}
	}
}

func init() {
	Register("search", func() Op { return &SearchParams{} })
}
