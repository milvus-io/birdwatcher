package states

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

type ShowCurrentVersionParam struct {
	framework.ParamBase `use:"show current-version" desc:"display current Milvus Meta data version"`
}

// ShowCurrentVersionCommand returns command for show current-version.
func (app *ApplicationState) ShowCurrentVersionCommand(ctx context.Context, p *ShowCurrentVersionParam) {
	fmt.Println("Current Version:", etcdversion.GetVersion())
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

func (app *ApplicationState) SetCurrentVersionCommand(ctx context.Context, param *SetCurrentVersionParam) error {
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
