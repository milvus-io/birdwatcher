package show

import (
	"fmt"
	"sort"

	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	ReplicaMetaPrefix = "queryCoord-ReplicaMeta"
)

func printLoadedCollections(infos []*querypb.CollectionInfo) {
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].GetCollectionID() < infos[j].GetCollectionID()
	})

	for _, info := range infos {
		// TODO beautify output
		fmt.Println(info.String())
	}
}

func printCollectionLoadInfoV2(loadInfov2 querypbv2.CollectionLoadInfo) {
	fmt.Println(loadInfov2.String())
}

// CollectionLoadedCommand return show collection-loaded command.
func CollectionLoadedCommand(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "collection-loaded",
		Short:   "display information of loaded collection from querycoord",
		Aliases: []string{"collection-load"},
		RunE: func(cmd *cobra.Command, args []string) error {
			collectionLoadInfos, err := common.ListLoadedCollectionInfoV2_1(cli, basePath)

			if err != nil {
				return err
			}
			printLoadedCollections(collectionLoadInfos)

			loadInfov2, err := common.ListLoadedCollectionInfoV2_2(cli, basePath)
			if err != nil {
				return err
			}
			for _, info := range loadInfov2 {
				printCollectionLoadInfoV2(info)
			}

			return nil
		},
	}
	return cmd
}
