package states

import (
	"context"
	"fmt"
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
