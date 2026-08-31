package reset

import (
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
