package states

import (
	"context"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	metakv "github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/birdwatcher/states/storage"
)

type testExtensionMarker struct{}

func TestWithExtensionsStoresExtensions(t *testing.T) {
	ext := &testExtensionMarker{}

	app, ok := Start(&configs.Config{}, false, WithExtensions(ext)).(*ApplicationState)
	require.True(t, ok)
	require.Len(t, app.extensions, 1)
	require.Same(t, ext, app.extensions[0])
}

type testInstanceExtension struct {
	cobraCtx InstanceContext
	fnCtx    InstanceContext
	receiver *testInstanceReceiver
}

func (e *testInstanceExtension) InstanceCobraCommands(ctx InstanceContext) []*cobra.Command {
	e.cobraCtx = ctx
	return []*cobra.Command{{Use: "extension-cobra", Short: "extension cobra command"}}
}

func (e *testInstanceExtension) InstanceFunctionCommands(ctx InstanceContext) []any {
	e.fnCtx = ctx
	e.receiver = &testInstanceReceiver{}
	return []any{e.receiver}
}

type testInstanceFunctionParam struct {
	framework.ParamBase `use:"extension function" desc:"extension function command"`
	Value               string `name:"value" default:"" desc:"value"`
}

type testInstanceReceiver struct {
	called bool
	value  string
}

func (r *testInstanceReceiver) FunctionCommand(ctx context.Context, p *testInstanceFunctionParam) error {
	r.called = true
	r.value = p.Value
	return nil
}

type testApplicationExtension struct {
	ctx      ApplicationContext
	receiver *testApplicationReceiver
}

func (e *testApplicationExtension) ApplicationFunctionCommands(ctx ApplicationContext) []any {
	e.ctx = ctx
	e.receiver = &testApplicationReceiver{}
	return []any{e.receiver}
}

type testApplicationFunctionParam struct {
	framework.ParamBase `use:"extension app" desc:"extension app command"`
	Value               string `name:"value" default:"" desc:"value"`
}

type testApplicationReceiver struct {
	called bool
	value  string
}

func (r *testApplicationReceiver) AppCommand(ctx context.Context, p *testApplicationFunctionParam) error {
	r.called = true
	r.value = p.Value
	return nil
}

func TestApplicationStateMergesExtensionFunctionCommands(t *testing.T) {
	config := &configs.Config{}
	ext := &testApplicationExtension{}
	state, ok := Start(config, false, WithExtensions(ext)).(*ApplicationState)
	require.True(t, ok)

	_, _, err := state.core.RootCmd.Find([]string{"extension", "app"})
	require.NoError(t, err)

	require.Same(t, state, ext.ctx.State())
	require.Same(t, state.core, ext.ctx.Core())
	require.Same(t, config, ext.ctx.Config())
	require.Len(t, ext.ctx.Extensions(), 1)
	require.Same(t, ext, ext.ctx.Extensions()[0])

	state.core.RootCmd.SetArgs([]string{"extension", "app", "--value", "ok"})
	err = state.core.RootCmd.Execute()
	require.NoError(t, err)
	require.True(t, ext.receiver.called)
	require.Equal(t, "ok", ext.receiver.value)
}

func TestApplicationStateProvidesConnectedOSSObjectStore(t *testing.T) {
	config := &configs.Config{}
	app, ok := Start(config, false).(*ApplicationState)
	require.True(t, ok)
	require.Same(t, app, app.objectStoreProvider)

	ossState, err := storage.ConnectOSS(context.Background(), &storage.ConnectOSSParam{
		Bucket:          "test-bucket",
		Address:         "127.0.0.1",
		Port:            "9000",
		CloudProvider:   "aws",
		RootPath:        "test-root",
		SkipBucketCheck: true,
	}, app.core)
	require.NoError(t, err)
	app.states[ossTag] = ossState

	resolved, err := app.GetObjectStore(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, resolved)
	require.Equal(t, "test-bucket", resolved.BucketName)
	require.Equal(t, "test-root", resolved.RootPath)
}

func TestInstanceStateMergesExtensionCommands(t *testing.T) {
	config := &configs.Config{}
	client := metakv.NewFileAuditKV(nil, nil)
	ext := &testInstanceExtension{}
	state := &InstanceState{
		CmdState:     framework.NewCmdState("Milvus(test)", config),
		instanceName: "test-root",
		metaPath:     "custom-meta",
		client:       client,
		config:       config,
		basePath:     "test-root/custom-meta",
		extensions:   []Extension{ext},
	}

	state.SetupCommands()

	_, _, err := state.RootCmd.Find([]string{"extension-cobra"})
	require.NoError(t, err)
	_, _, err = state.RootCmd.Find([]string{"extension", "function"})
	require.NoError(t, err)
	_, _, err = state.RootCmd.Find([]string{"dry-mode"})
	require.NoError(t, err)
	_, _, err = state.RootCmd.Find([]string{"show"})
	require.NoError(t, err)

	require.Same(t, state, ext.cobraCtx)
	require.Same(t, state, ext.fnCtx)
	require.Equal(t, "test-root", ext.cobraCtx.InstanceName())
	require.Equal(t, "custom-meta", ext.cobraCtx.MetaPath())
	require.Equal(t, "test-root/custom-meta", ext.cobraCtx.BasePath())
	require.Same(t, client, ext.cobraCtx.Client())
	require.Same(t, config, ext.cobraCtx.Config())
	require.Same(t, state, ext.cobraCtx.State())

	state.RootCmd.SetArgs([]string{"extension", "function", "--value", "ok"})
	err = state.RootCmd.Execute()
	require.NoError(t, err)
	require.True(t, ext.receiver.called)
	require.Equal(t, "ok", ext.receiver.value)
}

func TestGetInstanceStateInjectsObjectStoreProviderIntoRepair(t *testing.T) {
	config := &configs.Config{}
	app, ok := Start(config, false).(*ApplicationState)
	require.True(t, ok)
	client := metakv.NewFileAuditKV(nil, nil)

	state, ok := GetInstanceState(app.core, client, "test-root", "meta", nil, config, nil, app.objectStoreProvider).(*InstanceState)
	require.True(t, ok)
	require.NotNil(t, state.ComponentRepair)
	require.NotNil(t, state.ComponentRepair.ObjectStoreResolver())
}
