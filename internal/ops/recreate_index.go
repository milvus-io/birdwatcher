package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	"github.com/milvus-io/milvus/client/v3/milvusclient"
)

// recreateIndexRecoveryLoadTimeout bounds only the recovery LoadCollection RPC.
// Once Milvus accepts that RPC, loading continues asynchronously on the server.
const recreateIndexRecoveryLoadTimeout = 30 * time.Second

type RecreateIndexParams struct {
	Collection         string `yaml:"collection"`
	Field              string `yaml:"field"`
	IndexName          string `yaml:"index_name,omitempty"`
	AutoIndex          bool   `yaml:"auto_index,omitempty"`
	ReleaseAndLoad     bool   `yaml:"release_and_load,omitempty"`
	ConfirmDefaultLoad bool   `yaml:"confirm_default_load,omitempty"`
	DryRun             bool   `yaml:"dry_run,omitempty"`
}

func (p *RecreateIndexParams) Execute(ctx context.Context, rc *RunContext) (result any, err error) {
	if rc.Client == nil {
		return nil, errNoClient("recreate_index")
	}
	if strings.TrimSpace(p.Collection) == "" || strings.TrimSpace(p.Field) == "" {
		return nil, fmt.Errorf("recreate_index: `collection` and `field` required")
	}

	indexNames, err := rc.Client.ListIndexes(ctx,
		milvusclient.NewListIndexOption(p.Collection).WithFieldName(p.Field))
	if err != nil {
		return nil, fmt.Errorf("list indexes for %s.%s: %w", p.Collection, p.Field, err)
	}

	indexName, err := selectIndexName(indexNames, p.IndexName)
	if err != nil {
		return nil, fmt.Errorf("recreate_index: %w", err)
	}

	description, err := rc.Client.DescribeIndex(ctx,
		milvusclient.NewDescribeIndexOption(p.Collection, indexName))
	if err != nil {
		return nil, fmt.Errorf("describe index %q: %w", indexName, err)
	}
	if description.Index == nil {
		return nil, fmt.Errorf("describe index %q returned no index definition", indexName)
	}

	sourceParams := description.Params()
	createParams, err := buildRecreateIndexParams(sourceParams, p.AutoIndex)
	if err != nil {
		return nil, fmt.Errorf("index %q: %w", indexName, err)
	}

	loadState, err := rc.Client.GetLoadState(ctx, milvusclient.NewGetLoadStateOption(p.Collection))
	if err != nil {
		return nil, fmt.Errorf("get load state for collection %q: %w", p.Collection, err)
	}

	encodedCreateParams := formatIndexParams(createParams)
	fmt.Fprintf(rc.Out(), "recreate index plan\n")
	fmt.Fprintf(rc.Out(), "  collection: %s\n", p.Collection)
	fmt.Fprintf(rc.Out(), "  field: %s\n", p.Field)
	fmt.Fprintf(rc.Out(), "  index: %s\n", indexName)
	fmt.Fprintf(rc.Out(), "  auto index: %t\n", p.AutoIndex)
	fmt.Fprintf(rc.Out(), "  release and load: %t\n", p.ReleaseAndLoad)
	fmt.Fprintf(rc.Out(), "  default load confirmed: %t\n", p.ConfirmDefaultLoad)
	fmt.Fprintf(rc.Out(), "  source params: %s\n", formatIndexParams(sourceParams))
	fmt.Fprintf(rc.Out(), "  create params: %s\n", encodedCreateParams)
	fmt.Fprintf(rc.Out(), "  load state: %s\n", formatLoadState(loadState.State))

	if p.DryRun {
		indexParamsWarning := legacyIndexParamsWarning(sourceParams, p.AutoIndex)
		if indexParamsWarning != "" {
			fmt.Fprintf(rc.Out(), "WARNING: %s\n", indexParamsWarning)
		}
		loadWarning := defaultLoadBehaviorWarning(p.ReleaseAndLoad)
		if loadWarning != "" {
			fmt.Fprintf(rc.Out(), "WARNING: %s\n", loadWarning)
		}
		loadStateWarning := loadedCollectionWarning(loadState.State, p.ReleaseAndLoad)
		if loadStateWarning != "" {
			fmt.Fprintf(rc.Out(), "WARNING: %s\n", loadStateWarning)
		}
		fmt.Fprintln(rc.Out(), "dry-run: nothing changed")
		fmt.Fprintf(rc.Out(), "next: %s\n", recreateIndexDryRunNextStep(loadState.State, p.ReleaseAndLoad, p.ConfirmDefaultLoad))
		result := map[string]any{
			"collection":           p.Collection,
			"field":                p.Field,
			"index":                indexName,
			"auto_index":           p.AutoIndex,
			"release_and_load":     p.ReleaseAndLoad,
			"confirm_default_load": p.ConfirmDefaultLoad,
			"source_params":        sourceParams,
			"create_params":        createParams,
			"dry_run":              true,
		}
		if indexParamsWarning != "" {
			result["warning"] = indexParamsWarning
		}
		if loadWarning != "" {
			result["load_warning"] = loadWarning
		}
		if loadStateWarning != "" {
			result["load_state_warning"] = loadStateWarning
		}
		return result, nil
	}

	if p.ReleaseAndLoad && !p.ConfirmDefaultLoad {
		return nil, fmt.Errorf("--release-and-load uses server-default LoadCollection settings; " +
			"pass --confirm-default-load to acknowledge the load behavior before executing")
	}
	if !p.ReleaseAndLoad && loadState.State != entity.LoadStateNotLoad {
		return nil, fmt.Errorf("collection %q must be released before recreating index %q; current load state is %s; "+
			"release it manually or pass --release-and-load --confirm-default-load",
			p.Collection, indexName, formatLoadState(loadState.State))
	}
	reloadOnFailure := false
	defer func() {
		if !reloadOnFailure {
			return
		}

		fmt.Fprintf(rc.Out(), "requesting collection %s reload after recreate failure\n", p.Collection)
		if reloadErr := reloadCollectionAfterFailure(ctx, rc, p.Collection); reloadErr != nil {
			restoreErr := fmt.Errorf("restore collection %q after recreate failure: %w", p.Collection, reloadErr)
			if err == nil {
				err = restoreErr
				return
			}
			err = errors.Join(err, restoreErr)
			return
		}
		fmt.Fprintf(rc.Out(), "collection %s reload requested after recreate failure\n", p.Collection)
	}()
	if p.ReleaseAndLoad {
		fmt.Fprintf(rc.Out(), "releasing collection %s\n", p.Collection)
		if err := rc.Client.ReleaseCollection(ctx, milvusclient.NewReleaseCollectionOption(p.Collection)); err != nil {
			return nil, fmt.Errorf("release collection %q before recreating index %q: %w", p.Collection, indexName, err)
		}
		reloadOnFailure = true
		fmt.Fprintf(rc.Out(), "collection %s released\n", p.Collection)
	}

	if err := rc.Client.DropIndex(ctx, milvusclient.NewDropIndexOption(p.Collection, indexName)); err != nil {
		return nil, fmt.Errorf("drop index %q: %w", indexName, err)
	}

	indexDefinition := index.NewGenericIndex(indexName, createParams)
	createOption := milvusclient.NewCreateIndexOption(p.Collection, p.Field, indexDefinition).
		WithIndexName(indexName)
	task, err := rc.Client.CreateIndex(ctx, createOption)
	if err != nil {
		return nil, fmt.Errorf("create index %q after it was dropped: %w; recreate with params %s",
			indexName, err, encodedCreateParams)
	}
	if err := task.Await(ctx); err != nil {
		return nil, fmt.Errorf("wait for recreated index %q: %w; recreate with params %s",
			indexName, err, encodedCreateParams)
	}

	recreated, err := rc.Client.DescribeIndex(ctx,
		milvusclient.NewDescribeIndexOption(p.Collection, indexName))
	if err != nil {
		return nil, fmt.Errorf("verify recreated index %q: %w", indexName, err)
	}
	if recreated.Index == nil {
		return nil, fmt.Errorf("verify recreated index %q: describe returned no index definition", indexName)
	}
	if !recreatedIndexParamsMatch(createParams, recreated.Params(), p.AutoIndex) {
		return nil, fmt.Errorf("recreated index %q does not match requested user index parameters: want %s, got %s",
			indexName, encodedCreateParams, formatIndexParams(recreated.Params()))
	}
	if p.ReleaseAndLoad {
		reloadOnFailure = false
		fmt.Fprintf(rc.Out(), "loading collection %s\n", p.Collection)
		if err := loadCollectionAndWait(ctx, rc, p.Collection); err != nil {
			return nil, fmt.Errorf("index %q was recreated, but failed to load collection: %w", indexName, err)
		}
		fmt.Fprintf(rc.Out(), "collection %s loaded\n", p.Collection)
	}

	fmt.Fprintf(rc.Out(), "index %s recreated on %s.%s\n", indexName, p.Collection, p.Field)
	return map[string]any{
		"collection":           p.Collection,
		"field":                p.Field,
		"index":                indexName,
		"auto_index":           p.AutoIndex,
		"release_and_load":     p.ReleaseAndLoad,
		"confirm_default_load": p.ConfirmDefaultLoad,
		"source_params":        sourceParams,
		"create_params":        createParams,
		"dry_run":              false,
	}, nil
}

func loadCollectionAndWait(ctx context.Context, rc *RunContext, collection string) error {
	loadTask, err := rc.Client.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(collection))
	if err != nil {
		return fmt.Errorf("start loading collection %q: %w", collection, err)
	}
	if err := loadTask.Await(ctx); err != nil {
		return fmt.Errorf("wait for collection %q to load: %w", collection, err)
	}
	return nil
}

// reloadCollectionAfterFailure submits recovery with a context detached from
// command cancellation. If the command is still active, it also waits for the
// load to finish. After cancellation, a successful submission is sufficient:
// Milvus continues the accepted LoadCollection request asynchronously.
func reloadCollectionAfterFailure(ctx context.Context, rc *RunContext, collection string) error {
	recoveryCtx, cancel := newRecoveryLoadContext(ctx)
	loadTask, err := rc.Client.LoadCollection(recoveryCtx, milvusclient.NewLoadCollectionOption(collection))
	cancel()
	if err != nil {
		return fmt.Errorf("start loading collection %q: %w", collection, err)
	}

	if ctx.Err() != nil {
		return nil
	}
	if err := loadTask.Await(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("wait for collection %q to load: %w", collection, err)
	}
	return nil
}

func newRecoveryLoadContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), recreateIndexRecoveryLoadTimeout)
}

func buildRecreateIndexParams(sourceParams map[string]string, autoIndex bool) (map[string]string, error) {
	if !autoIndex {
		if len(sourceParams) == 0 {
			return nil, fmt.Errorf("has no user index parameters; refusing to drop it")
		}
		if _, ok := sourceParams[index.IndexTypeKey]; !ok {
			return nil, fmt.Errorf("has no %q parameter; refusing to drop it", index.IndexTypeKey)
		}
		return maps.Clone(sourceParams), nil
	}

	createParams := map[string]string{
		index.IndexTypeKey: string(index.AUTOINDEX),
	}
	if metricType := strings.TrimSpace(sourceParams[index.MetricTypeKey]); metricType != "" {
		createParams[index.MetricTypeKey] = metricType
	}
	return createParams, nil
}

// recreatedIndexParamsMatch allows Milvus to add its default metric when an
// AUTOINDEX request omitted metric_type, but rejects any other unexpected key.
func recreatedIndexParamsMatch(expected, actual map[string]string, autoIndex bool) bool {
	if !autoIndex || expected[index.MetricTypeKey] != "" {
		return maps.Equal(expected, actual)
	}

	if actual[index.IndexTypeKey] != string(index.AUTOINDEX) {
		return false
	}
	for key := range actual {
		if key != index.IndexTypeKey && key != index.MetricTypeKey {
			return false
		}
	}
	return true
}

// legacyIndexParamsWarning detects the fallback representation returned when
// legacy index metadata has no UserIndexParams. In that case DescribeIndex
// exposes AUTOINDEX plus metric_type and cannot recover the physical params.
func legacyIndexParamsWarning(sourceParams map[string]string, autoIndex bool) string {
	if autoIndex || !strings.EqualFold(strings.TrimSpace(sourceParams[index.IndexTypeKey]), string(index.AUTOINDEX)) {
		return ""
	}
	for key := range sourceParams {
		if key != index.IndexTypeKey && key != index.MetricTypeKey {
			return ""
		}
	}
	return "DescribeIndex returned only AUTOINDEX user parameters; the public API cannot distinguish a genuine " +
		"AUTOINDEX from legacy metadata with missing user parameters, so the original physical index parameters " +
		"may not be recoverable; verify the raw index metadata before executing"
}

func defaultLoadBehaviorWarning(releaseAndLoad bool) string {
	if !releaseAndLoad {
		return ""
	}
	return "--release-and-load uses server-default LoadCollection settings for both the normal reload and " +
		"failure recovery; it does not preserve the previous replica count, resource groups, load fields, or " +
		"skip-dynamic-field setting; confirm this load behavior with --confirm-default-load before executing with --run"
}

func loadedCollectionWarning(state entity.LoadStateCode, releaseAndLoad bool) string {
	if releaseAndLoad || state == entity.LoadStateNotLoad {
		return ""
	}
	return fmt.Sprintf("collection is %s; execution requires a manual release or "+
		"--release-and-load --confirm-default-load", formatLoadState(state))
}

func recreateIndexDryRunNextStep(state entity.LoadStateCode, releaseAndLoad, confirmLoad bool) string {
	switch {
	case !releaseAndLoad && state != entity.LoadStateNotLoad:
		return "release the collection manually, or add --release-and-load --confirm-default-load; then rerun with --run"
	case releaseAndLoad && !confirmLoad:
		return "after accepting the server-default load behavior, rerun with --run --confirm-default-load"
	default:
		return "after reviewing the plan and warnings, rerun with --run"
	}
}

func selectIndexName(indexNames []string, requested string) (string, error) {
	requested = strings.TrimSpace(requested)
	names := append([]string(nil), indexNames...)
	sort.Strings(names)

	if requested != "" {
		for _, name := range names {
			if name == requested {
				return name, nil
			}
		}
		return "", fmt.Errorf("index %q does not belong to the requested field; available indexes: %v", requested, names)
	}

	switch len(names) {
	case 0:
		return "", fmt.Errorf("no index found for the requested field")
	case 1:
		return names[0], nil
	default:
		return "", fmt.Errorf("multiple indexes found for the requested field; specify --index-name from %v", names)
	}
}

func formatIndexParams(params map[string]string) string {
	encoded, err := json.Marshal(params)
	if err != nil {
		return fmt.Sprintf("%v", params)
	}
	return string(encoded)
}

func formatLoadState(state entity.LoadStateCode) string {
	switch state {
	case entity.LoadStateLoading:
		return "Loading"
	case entity.LoadStateLoaded:
		return "Loaded"
	case entity.LoadStateUnloading:
		return "Unloading"
	case entity.LoadStateNotLoad:
		return "NotLoad"
	default:
		return fmt.Sprintf("Unknown(%d)", state)
	}
}

func init() {
	Register("recreate_index", func() Op { return &RecreateIndexParams{} })
}
