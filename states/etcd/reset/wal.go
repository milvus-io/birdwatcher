package reset

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/cockroachdb/errors"
	wpconfig "github.com/zilliztech/woodpecker/common/config"
	wp "github.com/zilliztech/woodpecker/woodpecker"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// legacyWALMetaPrefix is where woodpecker metadata lived before the prefix became
// configurable: a top-level key, not under the instance root. Woodpecker still falls back to
// it when it finds metadata there, so this command has to recognize it too.
const legacyWALMetaPrefix = "woodpecker"

type ResetWALParam struct {
	framework.ExecutionParam `use:"reset wal" desc:"delete the WAL's data and metadata so the instance can be restarted onto an empty WAL. Run this BEFORE 'reset checkpoint'. MILVUS MUST BE STOPPED, but the WAL service, etcd and object storage must stay UP: the WAL nodes are what delete their own local data."`

	MQType string `name:"mq-type" default:"woodpecker" desc:"WAL type to clear; only woodpecker is supported"`

	// Woodpecker reads its own metadata under {etcd.rootPath}/{meta-prefix}, and its data
	// under {bucket}/{root-path}/{logId}/. birdwatcher knows the etcd side already; the
	// object-storage side has to be supplied because Milvus keeps those credentials in its
	// own config, not in etcd.
	MetaPrefix  string `name:"meta-prefix" default:"woodpecker" desc:"woodpecker metadata prefix under the instance's etcd root"`
	StorageType string `name:"storage-type" default:"minio" desc:"woodpecker storage backend: minio, service, or local"`

	MinioAddress   string `name:"minio-address" default:"localhost" desc:"object storage host"`
	MinioPort      int64  `name:"minio-port" default:"9000" desc:"object storage port"`
	MinioAccessKey string `name:"minio-access-key" default:"minioadmin" desc:"object storage access key"`
	MinioSecretKey string `name:"minio-secret-key" default:"minioadmin" desc:"object storage secret key"`
	MinioUseSSL    bool   `name:"minio-use-ssl" default:"false" desc:"connect to object storage over TLS"`
	MinioBucket    string `name:"minio-bucket" default:"a-bucket" desc:"object storage bucket holding the WAL data"`
	MinioRootPath  string `name:"minio-root-path" default:"files" desc:"Milvus's minio.rootPath, copied verbatim from milvus.yaml; the woodpecker \"/wp\" suffix is added automatically"`
}

// ResetWALCommand empties an instance's write-ahead log.
//
// It exists because "switch a stopped instance onto a different WAL" and "restore an instance
// from a cold backup" both assume the WAL is already empty, and nothing in the operator's
// toolbox could make that true: deleting a log left its objects behind, the local reclaim was
// asynchronous with no completion signal, and there was no way to clear the instance-level
// metadata at all.
//
// What has to be running, and what must not be:
//
//	stop milvus                 ← nothing writes to the WAL any more
//	                              (etcd, object storage AND the WAL service stay up)
//	reset wal                   ← this command: the WAL becomes genuinely empty
//	reset checkpoint            ← rewinds Milvus's positions to the new WAL's earliest
//	start milvus
//
// The WAL service must stay up. Its nodes hold the staged local copy of the log, and only a
// node can delete its own disk — the command fences each one and waits for it to reclaim.
// With the WAL service down, the fan-out cannot resolve any node and the command refuses
// rather than deleting the metadata that would let anyone find that data later.
//
// The ordering against `reset checkpoint` matters just as much: run them the other way round
// and `reset checkpoint` points Milvus at "earliest" on a WAL that still holds truncated
// history, which is not the same position and does not fail loudly.
//
// The log id counter is deliberately preserved (ClearMetaExceptLogIdGen). logId appears in the
// object-storage and node-local data paths, so restarting it would let a new log reuse a
// directory an old log's objects may still occupy — and residue cannot be ruled out, because a
// node can be permanently down when its log is deleted. Keeping the counter monotonic makes
// that collision structurally impossible rather than contingent on the deletion having been
// complete.
//
// Idempotent: deletion re-enumerates what is left and the metadata clear is a prefix wipe plus
// overwrites, so an interrupted run is resumed by running it again.
func (c *ComponentReset) ResetWALCommand(ctx context.Context, p *ResetWALParam) error {
	if err := validateWALType(p.MQType); err != nil {
		return err
	}
	cfg, err := buildWoodpeckerConfig(c.instanceName, p)
	if err != nil {
		return err
	}

	fmt.Printf("=== reset wal (%s) ===\n", p.MQType)
	fmt.Printf("  metadata  %s/%s\n", cfg.Etcd.RootPath, cfg.Woodpecker.Meta.Prefix)
	if !cfg.Woodpecker.Storage.IsStorageLocal() {
		fmt.Printf("  data      %s/%s/{logId}/\n", cfg.Minio.BucketName, cfg.Minio.RootPath)
	}

	if err := c.requireWALMetadata(ctx, cfg.Woodpecker.Meta.Prefix); err != nil {
		return err
	}

	etcdCli := kv.MustGetETCDClient(c.client)
	client, err := wp.NewClient(ctx, cfg, etcdCli, false)
	if err != nil {
		return errors.Wrap(err, "failed to open woodpecker client")
	}
	defer client.Close(ctx)

	logs, err := client.GetAllLogs(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to list wal logs")
	}
	fmt.Printf("  %d log(s) to delete\n\n", len(logs))
	for _, name := range logs {
		fmt.Printf("  [log] %s\n", name)
	}

	if !p.Run {
		fmt.Printf("\ndry run: nothing deleted. Re-run with --run=true to apply.\n")
		fmt.Printf("Make sure Milvus is STOPPED and the WAL service is still UP before applying.\n")
		return nil
	}

	// Synchronous: the call returns only once every node has reclaimed its local data and the
	// objects are gone, so "the WAL is empty" is something the next step can rely on.
	stats, err := client.DeleteAllLogsSync(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to delete wal logs; re-run until it succeeds")
	}
	fmt.Printf("deleted %d log(s): %d object(s), %d of %d node fence(s) had local data\n",
		stats.Logs, stats.ObjectsDeleted, stats.NodesWithLocalData, stats.NodesFenced)
	// Deleting metadata while finding nothing on either data tier is what a wrong
	// bucket/rootPath looks like: the node's reclaim removes a directory that is not there
	// and the object prefix lists empty, so the run reports success either way. It is not
	// proof of a mistake -- an already-empty WAL looks identical -- so say it and move on.
	if stats.Logs > 0 && stats.ObjectsDeleted == 0 && stats.NodesWithLocalData == 0 {
		fmt.Printf("\nWARNING: no data found on any tier. If this WAL was not already empty,\n")
		fmt.Printf("         check --minio-bucket and --minio-root-path: %s/%s\n",
			cfg.Minio.BucketName, cfg.Minio.RootPath)
	}

	if err := client.ClearMetaExceptLogIdGen(ctx); err != nil {
		return errors.Wrap(err, "failed to clear wal metadata; re-run until it succeeds")
	}
	fmt.Printf("cleared wal metadata (log id counter preserved)\n")
	fmt.Printf("\nwal is empty. Next: reset checkpoint --target-wal %s\n", p.MQType)
	return nil
}

func validateWALType(mqType string) error {
	switch strings.ToLower(mqType) {
	case "woodpecker", "wp":
		return nil
	default:
		return errors.Newf("unsupported mq type %q: only woodpecker can be cleared today. "+
			"Other WALs are external services with their own tooling", mqType)
	}
}

// buildWoodpeckerConfig assembles the configuration woodpecker needs to find the same data
// Milvus was using. Everything etcd-side is derived from the connected instance; the object
// storage side comes from flags, because Milvus keeps those credentials in its own config
// file rather than in etcd.
func buildWoodpeckerConfig(instanceName string, p *ResetWALParam) (*wpconfig.Configuration, error) {
	cfg, err := wpconfig.NewConfiguration()
	if err != nil {
		return nil, errors.Wrap(err, "failed to build woodpecker configuration")
	}
	cfg.Etcd.RootPath = instanceName
	cfg.Woodpecker.Meta.Prefix = p.MetaPrefix
	cfg.Woodpecker.Storage.Type = p.StorageType

	cfg.Minio.Address = p.MinioAddress
	cfg.Minio.Port = int(p.MinioPort)
	cfg.Minio.AccessKeyID = p.MinioAccessKey
	cfg.Minio.SecretAccessKey = p.MinioSecretKey
	cfg.Minio.UseSSL = p.MinioUseSSL
	cfg.Minio.BucketName = p.MinioBucket
	cfg.Minio.RootPath = woodpeckerRootPath(p.MinioRootPath)
	// Never create a bucket while cleaning up: if the bucket is missing the arguments are
	// wrong, and inventing one would hide that.
	cfg.Minio.CreateBucket = false
	return cfg, nil
}

// woodpeckerRootPath derives woodpecker's object storage root from Milvus's.
//
// Milvus appends a "wp" segment unconditionally when it builds woodpecker's config
// (pkg/streaming/walimpls/impls/wp/builder.go: `Minio.RootPath = fmt.Sprintf("%s/wp", ...)`),
// keeping WAL objects out of the binlog tree that shares the bucket. There is no Milvus
// setting that turns it off, so this is a fixed part of the layout rather than a default.
//
// Deriving it here rather than asking the operator for the final path is deliberate. The
// obvious thing to paste into --minio-root-path is the rootPath from milvus.yaml, and that
// value is wrong by exactly this suffix. A wrong root does not fail loudly: the node's local
// reclaim is an os.RemoveAll on a path that simply does not exist, and the object prefix
// lists empty, so every tier reports success while all of the data survives.
func woodpeckerRootPath(milvusRootPath string) string {
	return path.Join(milvusRootPath, "wp")
}

// requireWALMetadata refuses to run against a metadata prefix that holds no WAL.
//
// A wrong --meta-prefix is the most dangerous argument this command takes, because it is the
// one mistake that produces a confident success with nothing done. Every enumeration is
// scoped to the prefix, so a wrong one lists no logs, deletes nothing, re-seeds the
// instance-level keys somewhere harmless, and prints "wal is empty. Next: reset checkpoint".
// An operator who follows that instruction rewrites every position to earliest against a WAL
// that still holds all of its data — the exact corruption this tool exists to prevent.
//
// The probe is the version key, which woodpecker writes at init and ClearMeta re-seeds. That
// second part matters: it keeps a re-run of a completed clear working, which the idempotency
// contract depends on. An instance that never started woodpecker has no key either, and
// refusing there is right too — there is no WAL to clear.
//
// Woodpecker also falls back to a legacy top-level "woodpecker" prefix when it finds metadata
// there, so that location is accepted as well rather than reported as missing.
func (c *ComponentReset) requireWALMetadata(ctx context.Context, metaPrefix string) error {
	configured := path.Join(c.instanceName, metaPrefix, "version")
	found, err := c.keyExists(ctx, configured)
	if err != nil {
		return err
	}
	if found {
		return nil
	}
	// woodpecker's own legacy-prefix detection, mirrored: metadata written before the prefix
	// became configurable lives at a top-level "woodpecker", outside the instance root.
	legacy := path.Join(legacyWALMetaPrefix, "version")
	found, err = c.keyExists(ctx, legacy)
	if err != nil {
		return err
	}
	if found {
		return errors.Newf("no wal metadata at %s, but found it at the legacy prefix %s. "+
			"Run with --meta-prefix pointing at the legacy location, or migrate it first",
			path.Join(c.instanceName, metaPrefix), legacyWALMetaPrefix)
	}
	return errors.Newf("no wal metadata found under %s. Check --meta-prefix (Milvus config "+
		"woodpecker.meta.prefix) and the instance selected at connect time. Refusing to "+
		"continue: clearing the wrong prefix reports success while the real WAL keeps its data",
		path.Join(c.instanceName, metaPrefix))
}
