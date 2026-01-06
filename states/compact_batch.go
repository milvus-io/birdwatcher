package states

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type CompactBatchParam struct {
	framework.ParamBase `use:"compact-batch" desc:"dispatch compact job by batch"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to filter with"`
	StorageVersion      int64 `name:"storageVersion" default:"-1"`
	Run                 bool  `name:"run" default:"false"`
}

func (s *InstanceState) CompactBatchCommand(ctx context.Context, p *CompactBatchParam) error {
	if p.CollectionID <= 0 {
		return fmt.Errorf("invalid collection id: %d", p.CollectionID)
	}
	// list segment info to get row count information
	segments, err := common.ListSegments(ctx, s.client, s.basePath, func(s *models.Segment) bool {
		return (p.CollectionID == 0 || p.CollectionID == s.CollectionID) &&
			(p.StorageVersion == -1 || p.StorageVersion == s.StorageVersion) &&
			s.GetState() == commonpb.SegmentState_Flushed
	})
	if err != nil {
		return err
	}

	segmentIDs := lo.Map(segments, func(s *models.Segment, _ int) int64 {
		return s.ID
	})

	fmt.Println("segment ids: ", segmentIDs)

	if !p.Run {
		return nil
	}
	sessions, err := common.ListSessions(ctx, s.client, s.basePath)
	if err != nil {
		return err
	}

	session := lo.FindOrElse(sessions, nil, func(session *models.Session) bool {
		return session.ServerName == "datacoord" || session.ServerName == "mixcoord"
	})

	if session == nil {
		return fmt.Errorf("datacoord not found")
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}

	conn, err := grpc.DialContext(context.Background(), session.Address, opts...)
	if err != nil {
		fmt.Printf("failed to connect to datacoord(%d) addr: %s, err: %s\n", session.ServerID, session.Address, err.Error())
		return err
	}

	client := datapb.NewDataCoordClient(conn)
	resp, err := client.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{
		CollectionID: p.CollectionID,
		SegmentIds:   segmentIDs,
	})
	if err != nil {
		return errors.Wrapf(err, "manual compact fail with collectionID:%d", p.CollectionID)
	}
	fmt.Printf("manual compact done, collectionID:%d, compactionID:%d, rpc status:%v\n",
		p.CollectionID, resp.GetCompactionID(), resp.GetStatus())

	return nil
}
