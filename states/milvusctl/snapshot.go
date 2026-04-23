package milvusctl

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

// -----------------------------------------------------------------------------
// create snapshot

type CreateSnapshotParam struct {
	framework.ParamBase         `use:"create snapshot" desc:"create a snapshot for a collection"`
	Name                        string `name:"name" default:"" desc:"snapshot name"`
	Collection                  string `name:"collection" default:"" desc:"collection name"`
	DB                          string `name:"db" default:"" desc:"database name (optional)"`
	Description                 string `name:"desc" default:"" desc:"snapshot description (optional)"`
	CompactionProtectionSeconds int64  `name:"compaction-protection-seconds" default:"0" desc:"seconds to protect referenced segments from compaction (0 = no protection, max 7 days)"`
}

// createSnapshotOptionWrapper wraps milvusclient.CreateSnapshotOption to inject
// the compaction_protection_seconds field which is not yet exposed by the SDK builder.
type createSnapshotOptionWrapper struct {
	base                        milvusclient.CreateSnapshotOption
	compactionProtectionSeconds int64
}

func (w *createSnapshotOptionWrapper) Request() *milvuspb.CreateSnapshotRequest {
	req := w.base.Request()
	req.CompactionProtectionSeconds = w.compactionProtectionSeconds
	return req
}

func (s *MilvusctlState) CreateSnapshotCommand(ctx context.Context, p *CreateSnapshotParam) error {
	if p.Name == "" {
		return fmt.Errorf("--name is required")
	}
	if p.Collection == "" {
		return fmt.Errorf("--collection is required")
	}
	if p.CompactionProtectionSeconds < 0 {
		return fmt.Errorf("--compaction-protection-seconds must be >= 0")
	}
	baseOpt := milvusclient.NewCreateSnapshotOption(p.Name, p.Collection)
	if p.DB != "" {
		baseOpt = baseOpt.WithDbName(p.DB)
	}
	if p.Description != "" {
		baseOpt = baseOpt.WithDescription(p.Description)
	}
	var opt milvusclient.CreateSnapshotOption = baseOpt
	if p.CompactionProtectionSeconds > 0 {
		opt = &createSnapshotOptionWrapper{
			base:                        baseOpt,
			compactionProtectionSeconds: p.CompactionProtectionSeconds,
		}
	}
	if err := s.client.CreateSnapshot(ctx, opt); err != nil {
		return err
	}
	fmt.Printf("snapshot %q created for collection %q\n", p.Name, p.Collection)
	return nil
}

// -----------------------------------------------------------------------------
// list snapshots

type ListSnapshotsParam struct {
	framework.ParamBase `use:"list snapshots" desc:"list snapshots"`
	Collection          string `name:"collection" default:"" desc:"collection name"`
	DB                  string `name:"db" default:"" desc:"database name (optional)"`
}

func (s *MilvusctlState) ListSnapshotsCommand(ctx context.Context, p *ListSnapshotsParam) error {
	if p.Collection == "" {
		return fmt.Errorf("--collection is required")
	}
	opt := milvusclient.NewListSnapshotsOption(p.Collection)
	if p.DB != "" {
		opt = opt.WithDbName(p.DB)
	}
	names, err := s.client.ListSnapshots(ctx, opt)
	if err != nil {
		return err
	}
	if len(names) == 0 {
		fmt.Println("(no snapshots)")
		return nil
	}
	for i, n := range names {
		fmt.Printf("  [%d] %s\n", i, n)
	}
	return nil
}

// -----------------------------------------------------------------------------
// describe snapshot

type DescribeSnapshotParam struct {
	framework.ParamBase `use:"describe snapshot" desc:"show snapshot detail"`
	Name                string `name:"name" default:"" desc:"snapshot name"`
	Collection          string `name:"collection" default:"" desc:"collection name"`
}

func (s *MilvusctlState) DescribeSnapshotCommand(ctx context.Context, p *DescribeSnapshotParam) error {
	if p.Name == "" {
		return fmt.Errorf("--name is required")
	}
	if p.Collection == "" {
		return fmt.Errorf("--collection is required")
	}
	resp, err := s.client.DescribeSnapshot(ctx, milvusclient.NewDescribeSnapshotOption(p.Name, p.Collection))
	if err != nil {
		return err
	}
	fmt.Println("Snapshot Detail:")
	fmt.Printf("  Name:           %s\n", resp.GetName())
	fmt.Printf("  Description:    %s\n", resp.GetDescription())
	fmt.Printf("  Collection:     %s\n", resp.GetCollectionName())
	fmt.Printf("  CreateTs:       %d\n", resp.GetCreateTs())
	fmt.Printf("  S3Location:     %s\n", resp.GetS3Location())
	fmt.Printf("  PartitionNames: %v\n", resp.GetPartitionNames())
	return nil
}

// -----------------------------------------------------------------------------
// drop snapshot

type DropSnapshotParam struct {
	framework.ParamBase `use:"drop snapshot" desc:"drop a snapshot by name"`
	Name                string `name:"name" default:"" desc:"snapshot name"`
	Collection          string `name:"collection" default:"" desc:"collection name"`
	Yes                 bool   `name:"yes" default:"false" desc:"skip confirmation"`
}

func (s *MilvusctlState) DropSnapshotCommand(ctx context.Context, p *DropSnapshotParam) error {
	if p.Name == "" {
		return fmt.Errorf("--name is required")
	}
	if p.Collection == "" {
		return fmt.Errorf("--collection is required")
	}
	if !p.Yes {
		fmt.Printf("about to drop snapshot %q; pass --yes to confirm\n", p.Name)
		return nil
	}
	if err := s.client.DropSnapshot(ctx, milvusclient.NewDropSnapshotOption(p.Name, p.Collection)); err != nil {
		return err
	}
	fmt.Printf("snapshot %q dropped\n", p.Name)
	return nil
}
