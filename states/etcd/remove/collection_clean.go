package remove

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/samber/lo"
	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
)

var paginationSize = 2000

type ExcludePrefixOptions func(string) bool

// CollectionCleanCommand returns command to remove
func CollectionCleanCommand(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "collection-meta-leaked",
		Short: "Remove leaked collection meta for collection has been dropped",
		Run: func(cmd *cobra.Command, args []string) {
			run, err := cmd.Flags().GetBool("run")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			collections, err := common.ListCollections(context.TODO(), cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			id2Collection := lo.SliceToMap(collections, func(col *models.Collection) (string, *models.Collection) {
				fmt.Printf("existing collectionID %v\n", col.GetProto().ID)
				return strconv.FormatInt(col.GetProto().ID, 10), col
			})

			cleanMetaFn := func(ctx context.Context, prefix string, opts ...ExcludePrefixOptions) error {
				return cli.WalkWithPrefix(ctx, prefix, paginationSize, func(k []byte, v []byte) error {
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
						if run {
							return cli.Remove(ctx, sKey)
						}
					}

					return nil
				})
			}

			// remove collection meta
			// meta before database
			collectionMetaPrefix := path.Join(basePath, common.CollectionMetaPrefix)
			// with database
			dbCollectionMetaPrefix := path.Join(basePath, common.DBCollectionMetaPrefix)
			// remove collection field meta
			fieldsPrefix := path.Join(basePath, common.FieldMetaPrefix)
			fieldsSnapShotPrefix := path.Join(basePath, common.SnapshotPrefix, common.FieldMetaPrefix)
			// remove collection partition meta
			partitionsPrefix := path.Join(basePath, common.PartitionPrefix)
			partitionsSnapShotPrefix := path.Join(basePath, common.SnapshotPrefix, common.PartitionPrefix)
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
				err = cleanMetaFn(context.TODO(), prefix)
				if err != nil {
					fmt.Println(err.Error())
					return
				}
				fmt.Printf("clean leaked collection meta done, prefix: %s\n", prefix)
			}

			// remove segment meta
			segmentPrefix := path.Join(basePath, common.SegmentMetaPrefix)
			segmentStatsPrefix := path.Join(basePath, common.SegmentStatsMetaPrefix)
			fmt.Printf("start cleaning leaked segment meta, prefix: %s, exclude prefix%s\n", segmentPrefix, segmentStatsPrefix)
			err = cleanMetaFn(context.TODO(), segmentPrefix, func(key string) bool {
				return strings.HasPrefix(key, segmentStatsPrefix)
			})
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			fmt.Printf("clean leaked segment meta done, prefix: %s\n", segmentPrefix)
		},
	}

	cmd.Flags().Bool("run", false, "flags indicating whether to execute removed command")
	return cmd
}
