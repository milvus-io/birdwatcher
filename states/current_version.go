package states

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
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

type SetCurrentVersionParam struct {
	framework.ParamBase `use:"set current-version" desc:"set current version for etcd meta parsing"`
	newVersion          string
}

func (p *SetCurrentVersionParam) ParseArgs(args []string) error {
	if len(args) != 1 {
		return errors.New("invalid parameter number")
	}
	p.newVersion = args[0]
	return nil
}

func (s *InstanceState) SetCurrentVersionCommand(ctx context.Context, param *SetCurrentVersionParam) error {
	switch param.newVersion {
	case models.LTEVersion2_1:
		fallthrough
	case "LTEVersion2_1":
		etcdversion.SetVersion(models.LTEVersion2_1)
	case models.GTEVersion2_2:
		fallthrough
	case "GTEVersion2_2":
		etcdversion.SetVersion(models.GTEVersion2_2)
	default:
		fmt.Println("Invalid version string:", param.newVersion)
	}
	return nil
}
