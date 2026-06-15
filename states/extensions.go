package states

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/kv"
)

type Option func(*ApplicationState)

type Extension any

type ApplicationContext interface {
	Core() *framework.CmdState
	Config() *configs.Config
	Extensions() []Extension
	State() framework.State
	SetTagNext(tag string, state framework.State)
}

type ApplicationFunctionCommandProvider interface {
	ApplicationFunctionCommands(ctx ApplicationContext) []any
}

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

type ObjectStoreProvider interface {
	GetObjectStore(ctx context.Context, instanceCtx InstanceContext, params ...oss.MinioConnectParam) (*oss.ResolvedObjectStore, error)
}

type ObjectStoreProviderExtension interface {
	ObjectStoreProvider() ObjectStoreProvider
}

func WithObjectStoreProvider(provider ObjectStoreProvider) Option {
	return func(app *ApplicationState) {
		app.objectStoreProvider = provider
	}
}

func resolveObjectStoreProvider(existing ObjectStoreProvider, exts []Extension) ObjectStoreProvider {
	if existing != nil {
		return existing
	}
	for _, ext := range exts {
		provider, ok := ext.(ObjectStoreProviderExtension)
		if !ok {
			continue
		}
		resolved := provider.ObjectStoreProvider()
		if resolved != nil {
			return resolved
		}
	}
	return nil
}
