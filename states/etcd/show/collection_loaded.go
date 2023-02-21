package show

import (
	"context"
	"fmt"
	"sort"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
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

func printCollectionLoaded(info *models.CollectionLoaded) {
	fmt.Printf("Version: [%s]\tCollectionID: %d\n", info.Version, info.CollectionID)
	fmt.Printf("ReplicaNumber: %d", info.ReplicaNumber)
	switch info.Version {
	case models.LTEVersion2_1:
		fmt.Printf("\tInMemoryPercent: %d\n", info.InMemoryPercentage)
	case models.GTEVersion2_2:
		fmt.Printf("\tLoadStatus: %s\n", info.Status.String())
	}
}

// CollectionLoadedCommand return show collection-loaded command.
func CollectionLoadedCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "collection-loaded",
		Short:   "display information of loaded collection from querycoord",
		Aliases: []string{"collection-load"},
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			infos, err := common.ListCollectionLoadedInfo(ctx, cli, basePath, etcdversion.GetVersion())
			if err != nil {
				fmt.Println("failed to list collection load info:", err.Error())
				return
			}

			for _, info := range infos {
				printCollectionLoaded(info)
			}
		},
	}
	return cmd
}
