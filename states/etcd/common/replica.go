package common

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/milvuspb"
	"github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ListReplica list current replica info
func ListReplica(cli *clientv3.Client, basePath string, collectionID int64) ([]*models.Replica, error) {
	v1Results, err := listReplicas(cli, basePath, func(replica *milvuspb.ReplicaInfo) bool {
		return collectionID == 0 || replica.GetCollectionID() == collectionID
	})
	if err != nil {
		fmt.Println(err.Error())
	}

	v2Results, err := listQCReplicas(cli, basePath, func(replica *querypb.Replica) bool {
		return collectionID == 0 || replica.GetCollectionID() == collectionID
	})
	if err != nil {
		fmt.Println(err.Error())
	}

	results := make([]*models.Replica, 0, len(v1Results)+len(v2Results))
	for _, r := range v1Results {
		shardReplicas := r.GetShardReplicas()
		srs := make([]models.ShardReplica, 0, len(shardReplicas))
		for _, shardReplica := range shardReplicas {
			srs = append(srs, models.ShardReplica{
				LeaderID:   shardReplica.GetLeaderID(),
				LeaderAddr: shardReplica.GetLeaderAddr(),
				NodeIDs:    shardReplica.GetNodeIds(),
			})
		}
		results = append(results, &models.Replica{
			ID:            r.GetReplicaID(),
			CollectionID:  r.GetCollectionID(),
			NodeIDs:       r.GetNodeIds(),
			ResourceGroup: "n/a",
			Version:       "<=2.1.4",
		})
	}

	for _, r := range v2Results {
		results = append(results, &models.Replica{
			ID:            r.GetID(),
			CollectionID:  r.GetCollectionID(),
			NodeIDs:       r.GetNodes(),
			ResourceGroup: "", //TODO
			Version:       ">=2.2.0",
		})
	}
	return results, nil

}
func listReplicas(cli *clientv3.Client, basePath string, filters ...func(*milvuspb.ReplicaInfo) bool) ([]milvuspb.ReplicaInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	prefix := path.Join(basePath, "queryCoord-ReplicaMeta")

	replicas, _, err := ListProtoObjects(ctx, cli, prefix, filters...)

	if err != nil {
		return nil, err
	}

	return replicas, nil
}

func listQCReplicas(cli *clientv3.Client, basePath string, filters ...func(*querypb.Replica) bool) ([]querypb.Replica, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	prefix := path.Join(basePath, "querycoord-replica")

	replicas, _, err := ListProtoObjects(ctx, cli, prefix, filters...)
	if err != nil {
		return nil, err
	}

	return replicas, nil
}
