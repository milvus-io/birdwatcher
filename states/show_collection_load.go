package states

import (
	"context"
	"path"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const collectionMetaPrefix = "queryCoord-collectionMeta"

func printLoadedCollections(infos []*querypb.CollectionInfo) {
	infos2 := make([]infoWithCollectionID, 0)
	for _, info := range infos {
		infos2 = append(infos2, info)
	}
	printInfoWithCollectionID(infos2)
}

func getLoadedCollectionInfo(cli *clientv3.Client, basePath string) ([]*querypb.CollectionInfo, error) {
	prefix := path.Join(basePath, collectionMetaPrefix)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	ret := make([]*querypb.CollectionInfo, 0)
	for _, kv := range resp.Kvs {
		collectionInfo := &querypb.CollectionInfo{}
		err = proto.Unmarshal(kv.Value, collectionInfo)
		if err != nil {
			return nil, err
		}
		ret = append(ret, collectionInfo)
	}
	return ret, nil
}

func getShowLoadedCollectionCmd(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "collection-loaded",
		Short:   "display information of loaded collection from querycoord",
		Aliases: []string{"collection-load"},
		RunE: func(cmd *cobra.Command, args []string) error {
			collectionLoadInfos, err := getLoadedCollectionInfo(cli, basePath)
			if err != nil {
				return err
			}
			printLoadedCollections(collectionLoadInfos)
			return nil
		},
	}
	return cmd
}
