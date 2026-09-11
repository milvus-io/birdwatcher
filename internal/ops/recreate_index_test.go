package ops

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
)

func TestNewRecoveryLoadContextIgnoresParentCancellation(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()

	recoveryCtx, cancelRecovery := newRecoveryLoadContext(parent)
	defer cancelRecovery()

	require.NoError(t, recoveryCtx.Err())
	deadline, ok := recoveryCtx.Deadline()
	require.True(t, ok)
	remaining := time.Until(deadline)
	assert.Positive(t, remaining)
	assert.LessOrEqual(t, remaining, recreateIndexRecoveryLoadTimeout)
}

func TestBuildRecreateIndexParams(t *testing.T) {
	sourceParams := map[string]string{
		index.IndexTypeKey:  "HNSW",
		index.MetricTypeKey: "COSINE",
		"M":                 "32",
		"efConstruction":    "360",
		"mmap.enabled":      "true",
	}

	t.Run("preserve current params", func(t *testing.T) {
		params, err := buildRecreateIndexParams(sourceParams, false)
		require.NoError(t, err)
		assert.Equal(t, sourceParams, params)

		params["M"] = "64"
		assert.Equal(t, "32", sourceParams["M"])
	})

	t.Run("convert to auto index", func(t *testing.T) {
		params, err := buildRecreateIndexParams(sourceParams, true)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{
			index.IndexTypeKey:  string(index.AUTOINDEX),
			index.MetricTypeKey: "COSINE",
		}, params)
	})

	t.Run("convert without metric type", func(t *testing.T) {
		params, err := buildRecreateIndexParams(map[string]string{
			index.IndexTypeKey: "INVERTED",
		}, true)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{
			index.IndexTypeKey: string(index.AUTOINDEX),
		}, params)
	})

	t.Run("convert empty params to auto index", func(t *testing.T) {
		params, err := buildRecreateIndexParams(nil, true)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{
			index.IndexTypeKey: string(index.AUTOINDEX),
		}, params)
	})

	t.Run("reject empty params in preserve mode", func(t *testing.T) {
		_, err := buildRecreateIndexParams(nil, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no user index parameters")
	})

	t.Run("reject missing index type in preserve mode", func(t *testing.T) {
		_, err := buildRecreateIndexParams(map[string]string{
			index.MetricTypeKey: "L2",
		}, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), index.IndexTypeKey)
	})
}

func TestRecreatedIndexParamsMatch(t *testing.T) {
	autoIndexParams := map[string]string{
		index.IndexTypeKey: string(index.AUTOINDEX),
	}

	assert.True(t, recreatedIndexParamsMatch(autoIndexParams, autoIndexParams, true))
	assert.True(t, recreatedIndexParamsMatch(autoIndexParams, map[string]string{
		index.IndexTypeKey:  string(index.AUTOINDEX),
		index.MetricTypeKey: "L2",
	}, true))
	assert.False(t, recreatedIndexParamsMatch(autoIndexParams, map[string]string{
		index.IndexTypeKey: "HNSW",
	}, true))
	assert.False(t, recreatedIndexParamsMatch(autoIndexParams, map[string]string{
		index.IndexTypeKey: string(index.AUTOINDEX),
		"M":                "32",
	}, true))
	assert.False(t, recreatedIndexParamsMatch(autoIndexParams, map[string]string{
		index.IndexTypeKey:  string(index.AUTOINDEX),
		index.MetricTypeKey: "L2",
	}, false))
}

func TestLegacyIndexParamsWarning(t *testing.T) {
	ambiguousParams := map[string]string{
		index.IndexTypeKey:  string(index.AUTOINDEX),
		index.MetricTypeKey: "L2",
	}

	assert.Contains(t, legacyIndexParamsWarning(ambiguousParams, false), "cannot distinguish")
	assert.Empty(t, legacyIndexParamsWarning(ambiguousParams, true))
	assert.Empty(t, legacyIndexParamsWarning(map[string]string{
		index.IndexTypeKey:  "HNSW",
		index.MetricTypeKey: "L2",
	}, false))
	assert.Empty(t, legacyIndexParamsWarning(map[string]string{
		index.IndexTypeKey:  string(index.AUTOINDEX),
		index.MetricTypeKey: "L2",
		"mmap.enabled":      "true",
	}, false))
}

func TestDefaultLoadBehaviorWarning(t *testing.T) {
	warning := defaultLoadBehaviorWarning(true)
	assert.Contains(t, warning, "server-default LoadCollection settings")
	assert.Contains(t, warning, "replica count")
	assert.Contains(t, warning, "resource groups")
	assert.Contains(t, warning, "--confirm-default-load")
	assert.Contains(t, warning, "before executing with --run")
	assert.Empty(t, defaultLoadBehaviorWarning(false))
}

func TestLoadedCollectionWarning(t *testing.T) {
	assert.Contains(t, loadedCollectionWarning(entity.LoadStateLoaded, false), "--release-and-load")
	assert.Empty(t, loadedCollectionWarning(entity.LoadStateNotLoad, false))
	assert.Empty(t, loadedCollectionWarning(entity.LoadStateLoaded, true))
}

func TestRecreateIndexDryRunNextStep(t *testing.T) {
	assert.Contains(t,
		recreateIndexDryRunNextStep(entity.LoadStateLoaded, false, false),
		"--release-and-load --confirm-default-load",
	)
	assert.Contains(t,
		recreateIndexDryRunNextStep(entity.LoadStateLoaded, true, false),
		"--run --confirm-default-load",
	)
	assert.Equal(t,
		"after reviewing the plan and warnings, rerun with --run",
		recreateIndexDryRunNextStep(entity.LoadStateNotLoad, false, false),
	)
}

func TestSelectIndexName(t *testing.T) {
	t.Run("single index", func(t *testing.T) {
		name, err := selectIndexName([]string{"vector_idx"}, "")
		require.NoError(t, err)
		assert.Equal(t, "vector_idx", name)
	})

	t.Run("explicit index", func(t *testing.T) {
		name, err := selectIndexName([]string{"scalar_idx", "vector_idx"}, "vector_idx")
		require.NoError(t, err)
		assert.Equal(t, "vector_idx", name)
	})

	t.Run("missing index", func(t *testing.T) {
		_, err := selectIndexName(nil, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no index found")
	})

	t.Run("ambiguous indexes", func(t *testing.T) {
		_, err := selectIndexName([]string{"z_idx", "a_idx"}, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "[a_idx z_idx]")
	})

	t.Run("explicit index not on field", func(t *testing.T) {
		_, err := selectIndexName([]string{"vector_idx"}, "other_idx")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not belong")
	})
}

func TestFormatIndexParams(t *testing.T) {
	assert.Equal(t,
		`{"index_type":"AUTOINDEX","metric_type":"COSINE"}`,
		formatIndexParams(map[string]string{
			"metric_type": "COSINE",
			"index_type":  "AUTOINDEX",
		}),
	)
}

func TestFormatLoadState(t *testing.T) {
	tests := []struct {
		name  string
		state entity.LoadStateCode
		want  string
	}{
		{name: "loading", state: entity.LoadStateLoading, want: "Loading"},
		{name: "loaded", state: entity.LoadStateLoaded, want: "Loaded"},
		{name: "unloading", state: entity.LoadStateUnloading, want: "Unloading"},
		{name: "not loaded", state: entity.LoadStateNotLoad, want: "NotLoad"},
		{name: "unknown", state: entity.LoadStateCode(99), want: "Unknown(99)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, formatLoadState(test.state))
		})
	}
}
