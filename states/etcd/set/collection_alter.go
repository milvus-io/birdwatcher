package set

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	schemapbv2 "github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// FieldAlterCommand returns `set collection-alter` command.
func FieldAlterCommand(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "field-alter",
		Short: "set alter-field ",
		Run: func(cmd *cobra.Command, args []string) {
			collectionID, err := cmd.Flags().GetInt64("collectionID")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if collectionID <= 0 {
				fmt.Printf("wrong collection id(%d)\n", collectionID)
				return
			}

			fieldID, err := cmd.Flags().GetInt64("fieldID")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			if fieldID <= 0 {
				fmt.Printf("wrong field id(%d)\n", fieldID)
				return
			}

			run, err := cmd.Flags().GetBool("run")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			dryRun := !run

			clusterKey, err := cmd.Flags().GetBool("clusterKey")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			if fieldID <= 0 {
				fmt.Printf("wrong fieldID(%d)\n", fieldID)
				return
			}

			alterClusterKey := func(field *schemapbv2.FieldSchema) {
				if field.FieldID != fieldID {
					return
				}
				field.IsClusteringKey = clusterKey
			}
			err = common.UpdateField(context.Background(), cli, basePath, collectionID, fieldID, alterClusterKey, dryRun)
			if err != nil {
				fmt.Printf("failed to alter field (%s)\n", err.Error())
				return
			}
		},
	}
	cmd.Flags().Bool("run", false, "flags indicating whether to execute alter command")
	cmd.Flags().Int64("collectionID", 0, "collection id to alter")
	cmd.Flags().Int64("fieldID", 0, "field id to alter")
	cmd.Flags().Bool("clusterKey", false, "flags indicating whether to enable clusterKey")
	return cmd
}
