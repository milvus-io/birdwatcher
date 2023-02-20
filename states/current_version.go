package states

import (
	"fmt"

	"github.com/milvus-io/birdwatcher/models"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/spf13/cobra"
)

// CurrentVersionCommand returns command for show current-version.
func CurrentVersionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "current-version",
		Run: func(_ *cobra.Command, args []string) {
			fmt.Println("Current Version:", etcdversion.GetVersion())
		},
	}
	return cmd
}

// SetCurrentVersionCommand returns command for set current-version.
func SetCurrentVersionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "set",
	}

	subCmd := &cobra.Command{
		Use: "current-version",
		Run: func(_ *cobra.Command, args []string) {
			if len(args) != 1 {
				fmt.Println("invalid parameter numbers")
				return
			}

			newVersion := args[0]
			switch newVersion {
			case models.LTEVersion2_1:
				fallthrough
			case "LTEVersion2_1":
				etcdversion.SetVersion(models.LTEVersion2_1)
			case models.GTEVersion2_2:
				fallthrough
			case "GTEVersion2_2":
				etcdversion.SetVersion(models.GTEVersion2_2)
			default:
				fmt.Println("Invalid version string:", newVersion)
			}
		},
	}

	cmd.AddCommand(subCmd)

	return cmd
}
