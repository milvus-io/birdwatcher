package show

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

const (
	ReplicaMetaPrefix = "queryCoord-ReplicaMeta"
)

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

type CollectionLoadedParam struct {
	framework.ParamBase `use:"show collection-loaded" desc:"display information of loaded collection from querycoord" alias:"collection-load"`
	//CollectionID int64 `name:""`
}

// CollectionLoadedCommand return show collection-loaded command.
func (c *ComponentShow) CollectionLoadedCommand(ctx context.Context, p *CollectionLoadedParam) {
	var total int
	infos, err := common.ListCollectionLoadedInfo(ctx, c.client, c.basePath, etcdversion.GetVersion(), func(_ any) bool {
		total++
		return true
	})
	if err != nil {
		fmt.Println("failed to list collection load info:", err.Error())
		return
	}

	for _, info := range infos {
		printCollectionLoaded(info)
	}
	fmt.Printf("--- Collections Loaded: %d\n", len(infos))
}
