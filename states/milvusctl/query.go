package milvusctl

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/internal/ops"
)

// -----------------------------------------------------------------------------
// search

type SearchParam struct {
	framework.ParamBase `use:"search" desc:"ANN search against a collection"`
	Collection          string   `name:"collection" default:"" desc:"collection name"`
	Field               string   `name:"field" default:"" desc:"ANN search field"`
	TopK                int64    `name:"topk" default:"10" desc:"top-K"`
	Metric              string   `name:"metric" default:"L2" desc:"metric type (informational; server uses index metric)"`
	Filter              string   `name:"filter" default:"" desc:"boolean expression filter"`
	Output              []string `name:"output" desc:"output fields"`
	VectorGen           string   `name:"vector-gen" default:"" desc:"generator for query vector(s), e.g. random_float_vector:dim=8:count=1"`
	Params              string   `name:"params" default:"" desc:"search params k=v,k=v (e.g. ef=64)"`
}

func (s *MilvusctlState) SearchCommand(ctx context.Context, p *SearchParam) error {
	if p.VectorGen == "" {
		return fmt.Errorf("--vector-gen is required (e.g. random_float_vector:dim=8:count=1)")
	}
	spec, err := parseGenSpec(p.VectorGen)
	if err != nil {
		return err
	}
	kv, err := parseKVString(p.Params)
	if err != nil {
		return err
	}
	return s.executeOp(ctx, &ops.SearchParams{
		Collection:   p.Collection,
		AnnsField:    p.Field,
		TopK:         int(p.TopK),
		Metric:       p.Metric,
		Filter:       p.Filter,
		OutputFields: p.Output,
		QueryVectors: []ops.VectorSpec{{Gen: spec}},
		SearchParams: kv,
	})
}

// -----------------------------------------------------------------------------
// query

type QueryParam struct {
	framework.ParamBase `use:"query" desc:"filter-based query against a collection"`
	Collection          string   `name:"collection" default:"" desc:"collection name"`
	Filter              string   `name:"filter" default:"" desc:"boolean expression filter"`
	Output              []string `name:"output" desc:"output fields (required)"`
	Limit               int64    `name:"limit" default:"0" desc:"result cap (omit for count(*))"`
	Offset              int64    `name:"offset" default:"0" desc:"offset into results"`
}

func (s *MilvusctlState) QueryCommand(ctx context.Context, p *QueryParam) error {
	return s.executeOp(ctx, &ops.QueryParams{
		Collection:   p.Collection,
		Filter:       p.Filter,
		OutputFields: p.Output,
		Limit:        int(p.Limit),
		Offset:       int(p.Offset),
	})
}

// -----------------------------------------------------------------------------
// export primary keys to parquet

type ExportPKParam struct {
	framework.ParamBase `use:"export pk" desc:"export a collection's primary keys to a parquet file via QueryIterator"`
	Collection          string `name:"collection" default:"" desc:"collection name"`
	Output              string `name:"output" default:"" desc:"output parquet file path"`
	Filter              string `name:"filter" default:"" desc:"optional boolean expression filter"`
	BatchSize           int64  `name:"batch-size" default:"1000" desc:"iterator batch size"`
}

func (s *MilvusctlState) ExportPkCommand(ctx context.Context, p *ExportPKParam) error {
	if p.Collection == "" || p.Output == "" {
		return fmt.Errorf("--collection and --output are required")
	}
	return s.executeOp(ctx, &ops.ExportPKParams{
		Collection: p.Collection,
		Output:     p.Output,
		Filter:     p.Filter,
		BatchSize:  int(p.BatchSize),
	})
}
