package remove

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

var paginationSize = 2000

type ExcludePrefixOptions func(string) bool

type CollectionMetaLeakedParam struct {
	framework.ExecutionParam `use:"remove collection-meta-leaked" desc:"Remove leaked collection meta for collection has been dropped"`
}

func (c *ComponentRemove) CollectionMetaLeakedCommand(ctx context.Context, p *CollectionMetaLeakedParam) error {
	collections, err := common.ListCollections(ctx, c.client, c.basePath)
	if err != nil {
		return fmt.Errorf("failed to list collections: %s", err.Error())
	}

	id2Collection := lo.SliceToMap(collections, func(col *models.Collection) (string, *models.Collection) {
		fmt.Printf("existing collectionID %v\n", col.GetProto().ID)
		return strconv.FormatInt(col.GetProto().ID, 10), col
	})

	cleanMetaFn := func(ctx context.Context, prefix string, opts ...ExcludePrefixOptions) error {
		return c.client.WalkWithPrefix(ctx, prefix, paginationSize, func(k []byte, v []byte) error {
			sKey := string(k)
			for _, opt := range opts {
				if opt(sKey) {
					return nil
				}
			}

			key := sKey[len(prefix):]
			ss := strings.Split(key, "/")
			collectionExist := false
			for _, s := range ss {
				if _, ok := id2Collection[s]; ok {
					collectionExist = true
				}
			}

			if !collectionExist {
				fmt.Println("clean meta key ", sKey)
				if p.Run {
					return c.client.Remove(ctx, sKey)
				}
			}

			return nil
		})
	}

	// remove collection meta
	// meta before database
	collectionMetaPrefix := path.Join(c.basePath, common.CollectionMetaPrefix)
	// with database
	dbCollectionMetaPrefix := path.Join(c.basePath, common.DBCollectionMetaPrefix)
	// remove collection field meta
	fieldsPrefix := path.Join(c.basePath, common.FieldMetaPrefix)
	fieldsSnapShotPrefix := path.Join(c.basePath, common.SnapshotPrefix, common.FieldMetaPrefix)
	// remove collection partition meta
	partitionsPrefix := path.Join(c.basePath, common.PartitionPrefix)
	partitionsSnapShotPrefix := path.Join(c.basePath, common.SnapshotPrefix, common.PartitionPrefix)
	prefixes := []string{
		collectionMetaPrefix,
		dbCollectionMetaPrefix,
		fieldsPrefix,
		fieldsSnapShotPrefix,
		partitionsPrefix,
		partitionsSnapShotPrefix,
	}

	for _, prefix := range prefixes {
		fmt.Printf("start cleaning leaked collection meta, prefix: %s\n", prefix)
		err = cleanMetaFn(ctx, prefix)
		if err != nil {
			return err
		}
		fmt.Printf("clean leaked collection meta done, prefix: %s\n", prefix)
	}

	// remove segment meta
	segmentPrefix := path.Join(c.basePath, common.DCPrefix, common.SegmentMetaPrefix)
	segmentStatsPrefix := path.Join(c.basePath, common.DCPrefix, common.SegmentStatsMetaPrefix)
	fmt.Printf("start cleaning leaked segment meta, prefix: %s, exclude prefix%s\n", segmentPrefix, segmentStatsPrefix)
	err = cleanMetaFn(ctx, segmentPrefix, func(key string) bool {
		return strings.HasPrefix(key, segmentStatsPrefix)
	})
	if err != nil {
		return err
	}

	fmt.Printf("clean leaked segment meta done, prefix: %s\n", segmentPrefix)
	return nil
}
