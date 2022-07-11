package states

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/congqixia/birdwatcher/proto/v2.0/milvuspb"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// getEtcdShowReplica returns command for show querycoord replicas
func getEtcdShowReplica(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "replica",
		Short:   "list current replica information from QueryCoord",
		Aliases: []string{"replicas"},
		RunE: func(cmd *cobra.Command, args []string) error {
			replicas, err := listReplicas(cli, basePath)

			if err != nil {
				return err
			}

			for _, replica := range replicas {
				printReplica(replica)
			}
			return nil
		},
	}

	return cmd
}

func listReplicas(cli *clientv3.Client, basePath string) ([]*milvuspb.ReplicaInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := cli.Get(ctx, path.Join(basePath, "queryCoord-ReplicaMeta"), clientv3.WithPrefix())

	if err != nil {
		return nil, err
	}

	replicas := make([]*milvuspb.ReplicaInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		replica := &milvuspb.ReplicaInfo{}
		if err != proto.Unmarshal(kv.Value, replica) {
			continue
		}
		replicas = append(replicas, replica)
	}

	return replicas, nil
}

func printReplica(replica *milvuspb.ReplicaInfo) {
	fmt.Println("================================================================================")
	fmt.Printf("ReplicaID: %d CollectionID: %d\n", replica.ReplicaID, replica.CollectionID)
	for _, shardReplica := range replica.ShardReplicas {
		fmt.Printf("Channel %s leader %d\n", shardReplica.DmChannelName, shardReplica.LeaderID)
	}
	fmt.Printf("Nodes:%v\n", replica.NodeIds)
}
