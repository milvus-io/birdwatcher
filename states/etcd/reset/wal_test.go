package reset

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValidateWALTypeRejectsOtherWALs pins the scope. pulsar and kafka are external services
// with their own tooling and their own retention; clearing them is not birdwatcher's call, and
// silently accepting the flag would suggest otherwise.
func TestValidateWALTypeRejectsOtherWALs(t *testing.T) {
	for _, ok := range []string{"woodpecker", "wp", "WoodPecker", "WP"} {
		assert.NoError(t, validateWALType(ok), "should accept %q", ok)
	}
	for _, bad := range []string{"pulsar", "kafka", "rocksmq", "", "woodpeckerr"} {
		err := validateWALType(bad)
		require.Error(t, err, "should reject %q", bad)
		assert.Contains(t, err.Error(), "only woodpecker",
			"the error must say what is supported, not just that the input was wrong")
	}
}

// TestBuildWoodpeckerConfigRootsMetadataAtTheInstance is the one mapping that has to be right:
// woodpecker keeps its metadata at {etcd.rootPath}/{meta-prefix}, a SIBLING of Milvus's meta
// path. Deriving it from basePath instead would aim the clear at
// {instance}/meta/woodpecker — a prefix that holds nothing, so the command would report
// success having deleted nothing.
func TestBuildWoodpeckerConfigRootsMetadataAtTheInstance(t *testing.T) {
	cfg, err := buildWoodpeckerConfig("by-dev", &ResetWALParam{
		MetaPrefix:     "woodpecker",
		StorageType:    "minio",
		MinioAddress:   "minio",
		MinioPort:      9000,
		MinioAccessKey: "ak",
		MinioSecretKey: "sk",
		MinioBucket:    "a-bucket",
		MinioRootPath:  "files",
	})
	require.NoError(t, err)

	assert.Equal(t, "by-dev", cfg.Etcd.RootPath)
	assert.Equal(t, "woodpecker", cfg.Woodpecker.Meta.Prefix)

	assert.Equal(t, "a-bucket", cfg.Minio.BucketName)
	assert.Equal(t, "files/wp", cfg.Minio.RootPath)
	assert.Equal(t, "minio", cfg.Minio.Address)
	assert.Equal(t, 9000, cfg.Minio.Port)

	assert.False(t, cfg.Minio.CreateBucket,
		"a cleanup that creates the bucket it cannot find would hide a wrong argument")
}

// TestBuildWoodpeckerConfigHonoursStorageType covers the three deployment shapes: the data
// lives somewhere different in each, and woodpecker dispatches on this value.
func TestBuildWoodpeckerConfigHonoursStorageType(t *testing.T) {
	for _, tc := range []struct {
		storageType               string
		local, minioMode, svcMode bool
	}{
		{"local", true, false, false},
		{"minio", false, true, false},
		{"service", false, false, true},
	} {
		cfg, err := buildWoodpeckerConfig("by-dev", &ResetWALParam{
			MetaPrefix: "woodpecker", StorageType: tc.storageType,
		})
		require.NoError(t, err)
		assert.Equal(t, tc.local, cfg.Woodpecker.Storage.IsStorageLocal(), tc.storageType)
		assert.Equal(t, tc.minioMode, cfg.Woodpecker.Storage.IsStorageMinio(), tc.storageType)
		assert.Equal(t, tc.svcMode, cfg.Woodpecker.Storage.IsStorageService(), tc.storageType)
	}
}

// TestBuildWoodpeckerConfigAppendsTheWpSuffix pins the object storage root against Milvus's
// own layout. Milvus writes WAL objects under "{minio.rootPath}/wp", so a config built from
// the bare rootPath points at an empty prefix — and, worse, ships that same wrong rootPath to
// the log-store nodes, whose local reclaim then removes a directory that does not exist. Every
// tier reports success and none of the data is touched, which is the one failure mode this
// command must never have.
func TestBuildWoodpeckerConfigAppendsTheWpSuffix(t *testing.T) {
	cfg, err := buildWoodpeckerConfig("by-dev", &ResetWALParam{
		MetaPrefix:    "woodpecker",
		StorageType:   "service",
		MinioBucket:   "milvus-bucket",
		MinioRootPath: "file",
	})
	require.NoError(t, err)
	assert.Equal(t, "file/wp", cfg.Minio.RootPath)
}

func TestWoodpeckerRootPathIsIdempotentlyJoined(t *testing.T) {
	// A trailing slash is what an operator pasting from a path gets; it must not produce
	// "files//wp", which lists as a different prefix in object storage.
	assert.Equal(t, "files/wp", woodpeckerRootPath("files/"))
	assert.Equal(t, "files/wp", woodpeckerRootPath("files"))
}

// TestRequireWALMetadataRefusesAWrongPrefix pins the guard against the most dangerous argument
// this command takes. A wrong --meta-prefix scopes every enumeration away from the real WAL,
// so without this the run deletes nothing, clears nothing, and still prints "wal is empty.
// Next: reset checkpoint" — and following that instruction rewrites every position to earliest
// against a WAL that still holds all of its data.
func TestRequireWALMetadataRefusesAWrongPrefix(t *testing.T) {
	ctx := context.Background()
	const instance = "guard-configured"
	require.NoError(t, testKV.Save(ctx, instance+"/woodpecker/version", "1"))
	defer testKV.RemoveWithPrefix(ctx, instance)

	c := NewComponent(testKV, nil, instance+"/meta", instance)

	require.NoError(t, c.requireWALMetadata(ctx, "woodpecker"),
		"the configured prefix holds metadata, so the run must proceed")

	err := c.requireWALMetadata(ctx, "not-the-wal")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no wal metadata found")
	assert.Contains(t, err.Error(), instance+"/not-the-wal",
		"the message must name the location it looked at")
}

// TestRequireWALMetadataAllowsAnAlreadyClearedInstance keeps the idempotency contract intact:
// ClearMeta re-seeds the version key, so re-running a completed clear must still be allowed.
func TestRequireWALMetadataAllowsAnAlreadyClearedInstance(t *testing.T) {
	ctx := context.Background()
	const instance = "guard-cleared"
	require.NoError(t, testKV.Save(ctx, instance+"/woodpecker/version", "1"))
	require.NoError(t, testKV.Save(ctx, instance+"/woodpecker/logidgen", "42"))
	defer testKV.RemoveWithPrefix(ctx, instance)

	c := NewComponent(testKV, nil, instance+"/meta", instance)
	assert.NoError(t, c.requireWALMetadata(ctx, "woodpecker"))
}

// TestRequireWALMetadataPointsAtTheLegacyPrefix distinguishes "there is no WAL" from "the WAL
// is somewhere else", because the operator's next move differs.
func TestRequireWALMetadataPointsAtTheLegacyPrefix(t *testing.T) {
	ctx := context.Background()
	const instance = "guard-legacy"
	require.NoError(t, testKV.Save(ctx, legacyWALMetaPrefix+"/version", "1"))
	defer testKV.RemoveWithPrefix(ctx, legacyWALMetaPrefix)

	c := NewComponent(testKV, nil, instance+"/meta", instance)
	err := c.requireWALMetadata(ctx, "woodpecker")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "legacy prefix")
}

// TestBuildDeleteOptionsRejectsBadInput pins that a mistyped flag fails loudly. The Go API
// tolerates a zero value because that is what an uninitialised struct field looks like; here
// the value came from a person typing it, and quietly substituting a default would hide the
// typo behind a run that appears to have honored it.
func TestBuildDeleteOptionsRejectsBadInput(t *testing.T) {
	base := func() *ResetWALParam {
		return &ResetWALParam{MarkAttempts: 3, MarkAttemptTimeout: "2m"}
	}

	opts, err := buildDeleteOptions(base())
	require.NoError(t, err)
	assert.Len(t, opts, 2, "attempts and timeout, with skipping left off")

	p := base()
	p.SkipUnreachableNodes = true
	opts, err = buildDeleteOptions(p)
	require.NoError(t, err)
	assert.Len(t, opts, 3, "skipping is only added when asked for")

	p = base()
	p.MarkAttemptTimeout = "2 minutes"
	_, err = buildDeleteOptions(p)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mark-attempt-timeout")

	p = base()
	p.MarkAttemptTimeout = "0s"
	_, err = buildDeleteOptions(p)
	require.Error(t, err, "a zero timeout would switch off the bound it exists to provide")

	p = base()
	p.MarkAttempts = 0
	_, err = buildDeleteOptions(p)
	require.Error(t, err, "zero attempts means retry forever in the retry helper")
}

// TestResiduePathMatchesTheNodeLayout pins the hint against the node's own localLogDataDir
// layout, {storage.rootPath}/{bucket}/{rootPath}/{logId}. The whole point of printing it is
// that an operator can paste it into a shell on the node.
func TestResiduePathMatchesTheNodeLayout(t *testing.T) {
	r := residuePath{nodeStorageRoot: "/woodpecker/data", bucket: "milvus-bucket", rootPath: "file/wp"}

	assert.Equal(t, "/woodpecker/data/milvus-bucket/file/wp/17/", r.forLogs([]int64{17}))
	assert.Equal(t, "/woodpecker/data/milvus-bucket/file/wp/{17, 19, 23}/",
		r.forLogs([]int64{17, 19, 23}))
}
