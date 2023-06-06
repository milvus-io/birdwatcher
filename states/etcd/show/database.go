package show

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// DatabaseCommand returns show database comand.
func DatabaseCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "database",
		Short: "display Database info from rootcoord meta",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			dbs, err := common.ListDatabase(ctx, cli, basePath)
			if err != nil {
				fmt.Println("failed to list database info", err.Error())
				return
			}

			for _, db := range dbs {
				printDatabaseInfo(db)
			}

			fmt.Printf("--- Total Database(s): %d\n", len(dbs))
		},
	}
	return cmd
}

func printDatabaseInfo(db *models.Database) {
	fmt.Println("=============================")
	fmt.Printf("ID: %d\tName: %s\n", db.ID, db.Name)
	fmt.Printf("TenantID: %s\t State: %s\n", db.TenantID, db.State.String())
}
