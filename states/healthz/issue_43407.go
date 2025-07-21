package healthz

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	metakv "github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/birdwatcher/states/mgrpc"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

type iss43407 struct {
	checkItemBase
}

func newIss43407() HealthzCheckItem {
	return &iss43407{
		checkItemBase: checkItemBase{
			name: "ISS43407",
			description: `Checks whether some collection meta is mssing.
In v2.5.14, collection meta may be lost if "RenameCollection" is executed.
This check item try to detect this issue by list all collection from meta
and compare with the list returned from rootcoord.
This check succeeds BEFORE cluster got restarted.
See also: https://github.com/milvus-io/milvus/issues/43407`,
		},
	}
}

func (c iss43407) Check(ctx context.Context, client metakv.MetaKV, basePath string) ([]*HealthzCheckReport, error) {
	collections, err := common.ListCollectionWithoutFields(ctx, client, basePath)
	if err != nil {
		return nil, err
	}

	var results []*HealthzCheckReport
	sessionCli, err := mgrpc.ConnectRootCoord(ctx, client, basePath, 0)
	if err != nil {
		return nil, err
	}

	rcClient := sessionCli.Client

	inMeta := lo.SliceToMap(collections, func(collection *models.Collection) (int64, struct{}) {
		return collection.GetProto().GetID(), struct{}{}
	})

	dbs, err := common.ListDatabase(ctx, client, basePath)
	if err != nil {
		fmt.Println("failed to list database info", err.Error())
		return nil, errors.Wrap(err, "failed to list database info")
	}

	for _, db := range dbs {
		resp, err := rcClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				SourceID: -1,
				TargetID: sessionCli.Session.ServerID,
				MsgType:  commonpb.MsgType_ShowCollections,
			},
			DbName: db.GetProto().GetName(),
		})
		if err != nil {
			fmt.Println(err.Error())
			return nil, err
		}

		for idx, id := range resp.GetCollectionIds() {
			if _, ok := inMeta[id]; !ok {
				results = append(results, &HealthzCheckReport{
					Msg: fmt.Sprintf("Collection %d not found in meta but returned from rootcoord", id),
					Extra: map[string]any{
						"collection_id":   id,
						"collection_name": resp.GetCollectionNames()[idx],
						"database_id":     db.GetProto().GetId(),
						"database_name":   db.GetProto().GetName(),
					},
				})
			}
		}
	}
	return results, nil
}
