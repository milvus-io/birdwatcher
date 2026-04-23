package milvusctl

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/internal/ops"
	schemapkg "github.com/milvus-io/birdwatcher/internal/schema"
)

// -----------------------------------------------------------------------------
// load collection

type LoadCollectionParam struct {
	framework.ParamBase `use:"load collection" desc:"load a collection into memory"`
	Name                string `name:"name" default:"" desc:"collection name"`
	Replica             int64  `name:"replica" default:"0" desc:"replica number (0 uses server default)"`
	Refresh             bool   `name:"refresh" default:"false" desc:"whether use load with refresh flag"`
}

func (s *MilvusctlState) LoadCollectionCommand(ctx context.Context, p *LoadCollectionParam) error {
	if p.Name == "" {
		return fmt.Errorf("--name is required")
	}
	return s.executeOp(ctx, &ops.LoadCollectionParams{Name: p.Name, Replica: int(p.Replica), Refresh: p.Refresh})
}

// -----------------------------------------------------------------------------
// release collection

type ReleaseCollectionParam struct {
	framework.ParamBase `use:"release collection" desc:"release a collection from memory"`
	Name                string `name:"name" default:"" desc:"collection name"`
}

func (s *MilvusctlState) ReleaseCollectionCommand(ctx context.Context, p *ReleaseCollectionParam) error {
	if p.Name == "" {
		return fmt.Errorf("--name is required")
	}
	return s.executeOp(ctx, &ops.ReleaseCollectionParams{Name: p.Name})
}

// -----------------------------------------------------------------------------
// flush collection

type FlushCollectionParam struct {
	framework.ParamBase `use:"flush collection" desc:"flush pending data of a collection"`
	Name                string `name:"name" default:"" desc:"collection name"`
}

func (s *MilvusctlState) FlushCollectionCommand(ctx context.Context, p *FlushCollectionParam) error {
	if p.Name == "" {
		return fmt.Errorf("--name is required")
	}
	return s.executeOp(ctx, &ops.FlushParams{Name: p.Name})
}

// -----------------------------------------------------------------------------
// create partition

type CreatePartitionParam struct {
	framework.ParamBase `use:"create partition" desc:"create a partition in a collection"`
	Collection          string `name:"collection" default:"" desc:"collection name"`
	Name                string `name:"name" default:"" desc:"partition name"`
}

func (s *MilvusctlState) CreatePartitionCommand(ctx context.Context, p *CreatePartitionParam) error {
	if p.Collection == "" || p.Name == "" {
		return fmt.Errorf("--collection and --name are required")
	}
	return s.executeOp(ctx, &ops.CreatePartitionParams{Collection: p.Collection, Name: p.Name})
}

// -----------------------------------------------------------------------------
// add collection field

type AddCollectionFieldParam struct {
	framework.ParamBase `use:"add collection field" desc:"add a field to an existing collection"`
	Collection          string `name:"collection" default:"" desc:"collection name"`
	Field               string `name:"field" default:"" desc:"new field name"`
	Type                string `name:"type" default:"int64" desc:"field type (int64/varchar/...)"`
	Dim                 int64  `name:"dim" default:"0" desc:"vector dim (vector types only)"`
	MaxLength           int64  `name:"max-length" default:"0" desc:"varchar max length"`
	Nullable            bool   `name:"nullable" default:"true" desc:"whether the field is nullable (required for add field)"`
}

func (s *MilvusctlState) AddCollectionFieldCommand(ctx context.Context, p *AddCollectionFieldParam) error {
	if p.Collection == "" || p.Field == "" {
		return fmt.Errorf("--collection and --field are required")
	}
	return s.executeOp(ctx, &ops.AddFieldParams{
		Collection: p.Collection,
		Field: schemapkg.FieldSpec{
			Name:      p.Field,
			Type:      p.Type,
			Dim:       p.Dim,
			MaxLength: p.MaxLength,
			Nullable:  p.Nullable,
		},
	})
}

// -----------------------------------------------------------------------------
// create index

type CreateIndexParam struct {
	framework.ParamBase `use:"create index" desc:"create an index on a field"`
	Collection          string `name:"collection" default:"" desc:"collection name"`
	Field               string `name:"field" default:"" desc:"field name"`
	Type                string `name:"type" default:"AUTOINDEX" desc:"index type: HNSW|IVF_FLAT|IVF_PQ|FLAT|AUTOINDEX|DISKANN"`
	Metric              string `name:"metric" default:"L2" desc:"metric type"`
	Params              string `name:"params" default:"" desc:"index params as k=v,k=v"`
	IndexName           string `name:"index-name" default:"" desc:"optional index name"`
}

func (s *MilvusctlState) CreateIndexCommand(ctx context.Context, p *CreateIndexParam) error {
	if p.Collection == "" || p.Field == "" {
		return fmt.Errorf("--collection and --field are required")
	}
	params, err := parseKV(p.Params)
	if err != nil {
		return err
	}
	return s.executeOp(ctx, &ops.CreateIndexParams{
		Collection: p.Collection,
		Field:      p.Field,
		Index: ops.IndexSpec{
			Type:   p.Type,
			Metric: p.Metric,
			Name:   p.IndexName,
			Params: params,
		},
	})
}
