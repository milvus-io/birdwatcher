package repair

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type ManualCompactionParam struct {
	framework.ParamBase `use:"repair manual-compaction" desc:"do manual compaction"`
	Collection          int64 `name:"collection" default:"0" desc:"collection id"`
}

func (c *ComponentRepair) ManualCompactionCommand(ctx context.Context, p *ManualCompactionParam) error {
	if p.Collection == 0 {
		return errors.New("collection id should not be zero")
	}
	return doManualCompaction(ctx, c.client, c.basePath, p.Collection)
}

func doManualCompaction(ctx context.Context, cli kv.MetaKV, basePath string, collID int64) error {
	sessions, err := common.ListSessions(ctx, cli, basePath)
	if err != nil {
		return errors.Wrap(err, "failed to list session")
	}
	for _, session := range sessions {
		if session.ServerName == "datacoord" {
			opts := []grpc.DialOption{
				grpc.WithInsecure(),
				grpc.WithBlock(),
				grpc.WithTimeout(2 * time.Second),
			}

			conn, err := grpc.DialContext(context.Background(), session.Address, opts...)
			if err != nil {
				return errors.Wrapf(err, "failed to connect to DataCoord(%d) addr: %s", session.ServerID, session.Address)
			}

			client := datapb.NewDataCoordClient(conn)
			result, err := client.ManualCompaction(context.Background(), &milvuspb.ManualCompactionRequest{
				CollectionID: collID,
			})
			if err != nil {
				return errors.Wrap(err, "failed to call ManualCompaction")
			}
			if result.Status.ErrorCode != commonpb.ErrorCode_Success {
				return errors.Newf("ManualCompaction failed, error code = %s, reason = %s", result.Status.ErrorCode.String(), result.Status.Reason)
			}
			fmt.Printf("ManualCompaction trigger success, id: %d\n", result.CompactionID)
			return nil
		}
	}
	return errors.New("datacoord not found in session list")
}
