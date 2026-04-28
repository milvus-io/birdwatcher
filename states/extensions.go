package states

import (
	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/kv"
)

type Option func(*ApplicationState)

type Extension any

func WithExtensions(exts ...Extension) Option {
	return func(app *ApplicationState) {
		app.extensions = append(app.extensions, exts...)
	}
}

type InstanceContext interface {
	InstanceName() string
	MetaPath() string
	BasePath() string
	Client() kv.MetaKV
	Config() *configs.Config
	State() framework.State
}

type InstanceCobraCommandProvider interface {
	InstanceCobraCommands(ctx InstanceContext) []*cobra.Command
}

type InstanceFunctionCommandProvider interface {
	InstanceFunctionCommands(ctx InstanceContext) []any
}
