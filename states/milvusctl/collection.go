package milvusctl

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/internal/ops"
	schemapkg "github.com/milvus-io/birdwatcher/internal/schema"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

// -----------------------------------------------------------------------------
// list collections

type ListCollectionsParam struct {
	framework.DataSetParam `use:"list collections" desc:"list all collections in current database"`
}

func (s *MilvusctlState) ListCollectionsCommand(ctx context.Context, p *ListCollectionsParam) error {
	names, err := s.client.ListCollections(ctx, milvusclient.NewListCollectionOption())
	if err != nil {
		return err
	}
	if len(names) == 0 {
		fmt.Println("(no collections)")
		return nil
	}
	for _, n := range names {
		fmt.Println(n)
	}
	// framework.NewListResult(names)
	return nil
}

// -----------------------------------------------------------------------------
// describe collection

type DescribeCollectionParam struct {
	framework.ParamBase `use:"describe collection" desc:"show collection schema and stats"`
	Name                string `name:"name" default:"" desc:"collection name"`
}

func (s *MilvusctlState) DescribeCollectionCommand(ctx context.Context, p *DescribeCollectionParam) error {
	if p.Name == "" {
		return fmt.Errorf("--name is required")
	}
	return s.executeOp(ctx, &ops.DescribeCollectionParams{Name: p.Name})
}

// -----------------------------------------------------------------------------
// create collection

type CreateCollectionParam struct {
	framework.ParamBase `use:"create collection" desc:"create a new collection"`
	Name                string   `name:"name" default:"" desc:"collection name"`
	Dim                 int64    `name:"dim" default:"0" desc:"vector dimension (flag mode)"`
	PKType              string   `name:"pk" default:"int64" desc:"primary key type: int64 or varchar"`
	PKMaxLength         int64    `name:"pk-max-length" default:"256" desc:"varchar pk max length"`
	AutoID              bool     `name:"auto-id" default:"false" desc:"primary key auto id"`
	VectorField         string   `name:"vector-field" default:"vector" desc:"vector field name"`
	VectorType          string   `name:"vector-type" default:"float_vector" desc:"vector type: float_vector/binary_vector/float16_vector/bfloat16_vector/int8_vector"`
	ShardNum            int64    `name:"shard" default:"1" desc:"shard number"`
	DynamicField        bool     `name:"dynamic-field" default:"true" desc:"enable dynamic field"`
	SchemaFile          string   `name:"schema-file" default:"" desc:"YAML schema file (overrides flag-based schema)"`
	ScalarFields        []string `name:"scalar-field" desc:"extra scalar field, repeatable, format name:type[:maxLen]"`
}

func (s *MilvusctlState) CreateCollectionCommand(ctx context.Context, p *CreateCollectionParam) error {
	if p.SchemaFile != "" {
		return s.executeOp(ctx, &ops.CreateCollectionParams{
			Name:       p.Name,
			SchemaFile: p.SchemaFile,
			ShardNum:   int32(p.ShardNum),
		})
	}
	sch, err := schemapkg.Build(schemapkg.BuilderOptions{
		Name:         p.Name,
		PKType:       p.PKType,
		AutoID:       p.AutoID,
		PKMaxLength:  p.PKMaxLength,
		VectorField:  p.VectorField,
		VectorType:   p.VectorType,
		Dim:          p.Dim,
		DynamicField: p.DynamicField,
		ScalarFields: p.ScalarFields,
	})
	if err != nil {
		return err
	}
	opt := milvusclient.NewCreateCollectionOption(sch.CollectionName, sch).WithShardNum(int32(p.ShardNum))
	if err := s.client.CreateCollection(ctx, opt); err != nil {
		return err
	}
	fmt.Printf("collection %q created\n", sch.CollectionName)
	return nil
}

// -----------------------------------------------------------------------------
// drop collection

type DropCollectionParam struct {
	framework.ParamBase `use:"drop collection" desc:"drop a collection"`
	Name                string `name:"name" default:"" desc:"collection name"`
	Yes                 bool   `name:"yes" default:"false" desc:"skip confirmation"`
}

func (s *MilvusctlState) DropCollectionCommand(ctx context.Context, p *DropCollectionParam) error {
	if p.Name == "" {
		return fmt.Errorf("--name is required")
	}
	if !p.Yes {
		fmt.Printf("about to drop collection %q; pass --yes to confirm\n", p.Name)
		return nil
	}
	return s.executeOp(ctx, &ops.DropCollectionParams{Name: p.Name})
}

// -----------------------------------------------------------------------------
// has collection

type HasCollectionParam struct {
	framework.ParamBase `use:"has collection" desc:"check collection existence"`
	Name                string `name:"name" default:"" desc:"collection name"`
}

func (s *MilvusctlState) HasCollectionCommand(ctx context.Context, p *HasCollectionParam) error {
	if p.Name == "" {
		return fmt.Errorf("--name is required")
	}
	return s.executeOp(ctx, &ops.HasCollectionParams{Name: p.Name})
}
