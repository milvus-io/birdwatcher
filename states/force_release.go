package states

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// getForceReleaseCmd returns command for force-release
// usage: force-release [flags]
func getForceReleaseCmd(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "force-release",
		Short: "Force release the collections from QueryCoord",
		Run: func(cmd *cobra.Command, args []string) {
			// basePath = 'by-dev/meta/'
			// queryCoord prefix = 'queryCoord-'
			now := time.Now()
			err := backupEtcd(cli, basePath, "queryCoord-", string(compQueryCoord), fmt.Sprintf("bw_etcd_querycoord.%s.bak.gz", now.Format("060102-150405")), false)
			if err != nil {
				fmt.Printf("backup etcd failed, error: %v, stop doing force-release\n", err)
			}

			// remove all keys start with [basePath]/queryCoord-
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()
			_, err = cli.Delete(ctx, "queryCoord-", clientv3.WithPrefix())
			if err != nil {
				fmt.Printf("failed to remove queryCoord etcd kv, err: %v\n", err)
			}
			// release all collections from online querynodes

			// maybe? kill session of queryCoord?
		},
	}

	return cmd
}

// getReleaseDroppedCollectionCmd returns command for release-dropped-collection
func getReleaseDroppedCollectionCmd(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "release-dropped-collection",
		Short: "Clean loaded collections meta if it's dropped from QueryCoord",
		Run: func(cmd *cobra.Command, args []string) {
			collectionLoadInfos, err := getLoadedCollectionInfo(cli, basePath)
			if err != nil {
				fmt.Println("failed to list loaded collections", err.Error())
				return
			}

			var missing []int64
			for _, info := range collectionLoadInfos {
				_, err := getCollectionByID(cli, basePath, info.CollectionID)
				if err != nil {
					missing = append(missing, info.CollectionID)
				}
			}
			for _, id := range missing {
				fmt.Printf("Collection %d is missing\n", id)
			}
			run, err := cmd.Flags().GetBool("run")
			if err == nil && run {

				for _, id := range missing {
					fmt.Printf("Start to remove loaded meta from querycoord, collection id %d...\n", id)
					err := releaseQueryCoordLoadMeta(cli, basePath, id)
					if err != nil {
						fmt.Println("failed, err:", err.Error())
					} else {
						fmt.Println(" done.")
					}
				}
			}
		},
	}

	cmd.Flags().Bool("run", false, "flags indicating whether to remove load collection info from meta")
	return cmd
}

func releaseQueryCoordLoadMeta(cli *clientv3.Client, basePath string, collectionID int64) error {
	p := path.Join(basePath, collectionMetaPrefix, fmt.Sprintf("%d", collectionID))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	_, err := cli.Delete(ctx, p)
	return err
}
