package states

import (
	"context"
	"errors"
	"fmt"
	"io"

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/ossutil"
	"github.com/milvus-io/birdwatcher/storage"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
	storagemanifest "github.com/milvus-io/birdwatcher/storage/manifest"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type scanManifest struct {
	data *storagemanifest.Manifest
	open func(string, string) (storagecommon.ReadSeeker, error)
}

func loadScanManifest(ctx context.Context, segment *models.Segment, internalStore oss.ObjectStore, rootPath string,
	externalStore oss.ObjectStore, externalLocation ossutil.ExternalSourceLocation, readBufferSize int,
) (*scanManifest, error) {
	ref, err := storagemanifest.ParseReference(segment.GetManifestPath())
	if err != nil {
		return nil, fmt.Errorf("segment %d: %w", segment.GetID(), err)
	}
	ref.BasePath = oss.ResolveObjectKey(rootPath, ref.BasePath)
	m := &storagemanifest.Manifest{}
	if ref.Version != 0 {
		obj, err := openScanObject(ctx, internalStore, ref.ObjectPath())
		if err != nil {
			return nil, fmt.Errorf("open segment %d manifest %s: %w", segment.GetID(), ref.ObjectPath(), err)
		}
		if closer, ok := obj.(io.Closer); ok {
			defer closer.Close()
		}
		end := storagecommon.ScanOperation(ctx, "manifest.parse path=%q version=%d", storagecommon.ScanLogPath(ref.ObjectPath()), ref.Version)
		m, err = storagemanifest.Parse(obj)
		end(err)
		if err != nil {
			return nil, fmt.Errorf("parse segment %d manifest %s: %w", segment.GetID(), ref.ObjectPath(), err)
		}
	}
	storagecommon.ScanDebug(ctx, "manifest.loaded version=%d groups=%d deltalogs=%d", ref.Version, len(m.ColumnGroups), len(m.DeltaLogs))
	resolver := ossutil.NewManifestPathResolver(internalStore, ref.BasePath, externalStore, externalLocation)
	return &scanManifest{data: m, open: func(name, section string) (storagecommon.ReadSeeker, error) {
		store, key, _, err := resolver.Resolve(name, section)
		if err != nil {
			return nil, err
		}
		if store == nil {
			return nil, fmt.Errorf("manifest file %s has no configured object store", name)
		}
		obj, err := openScanObject(ctx, store, key)
		if err != nil {
			return nil, err
		}
		end := storagecommon.ScanOperation(ctx, "object.buffer.init path=%q bytes=%d", storagecommon.ScanLogPath(key), readBufferSize)
		buffered, err := storagecommon.NewBufferedReadSeeker(obj, readBufferSize)
		end(err)
		if err != nil {
			if closer, ok := obj.(io.Closer); ok {
				closer.Close()
			}
			return nil, err
		}
		return buffered, nil
	}}, nil
}

func addScanDeletes(ctx context.Context, segment *models.Segment, m *scanManifest, pkType schemapb.DataType,
	open func(string) (storagecommon.ReadSeeker, error), records map[any]uint64,
) (err error) {
	end := storagecommon.ScanOperation(ctx, "deletes.load segment=%d manifest=%t", segment.GetID(), m != nil)
	defer func() { end(err, fmt.Sprintf("unique_pks=%d", len(records))) }()
	add := func(pk any, ts uint64) {
		if old, ok := records[pk]; !ok || ts > old {
			records[pk] = ts
		}
	}
	if m != nil {
		for _, delta := range m.data.DeltaLogs {
			if delta.NumEntries < 0 || (delta.NumEntries > 0 && delta.Path == "") {
				return fmt.Errorf("segment %d has invalid manifest deltalog %+v", segment.GetID(), delta)
			}
			if delta.NumEntries == 0 {
				storagecommon.ScanDebug(ctx, "deltalog.skip path=%q reason=zero_entries", storagecommon.ScanLogPath(delta.Path))
				continue
			}
			if delta.Type != storagemanifest.DeltaLogTypePrimaryKey {
				return fmt.Errorf("segment %d: unsupported manifest deltalog type %s", segment.GetID(), delta.Type)
			}
			obj, err := m.open(delta.Path, "_delta")
			if err != nil {
				return fmt.Errorf("open deltalog %s: %w", delta.Path, err)
			}
			n, err := storage.ReadDeltalogRecords(ctx, obj, pkType, add)
			if err != nil {
				return fmt.Errorf("read deltalog %s: %w", delta.Path, err)
			}
			if n != delta.NumEntries {
				return fmt.Errorf("deltalog %s has %d rows, manifest declares %d", delta.Path, n, delta.NumEntries)
			}
			storagecommon.ScanDebug(ctx, "deltalog.loaded path=%q rows=%d unique_pks=%d", storagecommon.ScanLogPath(delta.Path), n, len(records))
		}
		return nil
	}
	metadataEnd := storagecommon.ScanOperation(ctx, "metadata.legacy_deltalogs segment=%d", segment.GetID())
	fields := segment.GetDeltalogs()
	metadataEnd(nil, fmt.Sprintf("fields=%d", len(fields)))
	for _, field := range fields {
		for _, log := range field.Binlogs {
			obj, err := open(log.LogPath)
			if err != nil {
				return fmt.Errorf("open deltalog %s: %w", log.LogPath, err)
			}
			n, err := storage.ReadDeltalogRecords(ctx, obj, pkType, add)
			if err != nil {
				return fmt.Errorf("read deltalog %s: %w", log.LogPath, err)
			}
			storagecommon.ScanDebug(ctx, "deltalog.loaded path=%q rows=%d unique_pks=%d", storagecommon.ScanLogPath(log.LogPath), n, len(records))
		}
	}
	return nil
}

func scanSegmentMatches(segment *models.Segment, p *ScanBinlogParams) bool {
	if segment.GetCollectionID() != p.CollectionID || (!p.IncludeUnhealthy && segment.GetState() == commonpb.SegmentState_Dropped) {
		return false
	}
	if segment.GetLevel() == datapb.SegmentLevel_L0 {
		return !p.IgnoreDelete && (p.PartitionID == 0 || segment.GetPartitionID() == -1 || segment.GetPartitionID() == p.PartitionID)
	}
	return (p.SegmentID == 0 || segment.GetID() == p.SegmentID) && (p.PartitionID == 0 || segment.GetPartitionID() == p.PartitionID)
}

func scanL0Applies(l0, segment *models.Segment) bool {
	return l0.GetInsertChannel() == segment.GetInsertChannel() && (l0.GetPartitionID() == -1 || l0.GetPartitionID() == segment.GetPartitionID())
}

var errScanOutputLimit = errors.New("scan output limit reached")

func runBinlogScan(ctx context.Context, segments []*models.Segment, workers int, work func(context.Context, *models.Segment) error) error {
	if workers <= 0 {
		return fmt.Errorf("worker count must be positive")
	}
	g, scanCtx := errgroup.WithContext(ctx)
	jobs := make(chan *models.Segment)
	g.Go(func() error {
		defer close(jobs)
		for _, segment := range segments {
			end := storagecommon.ScanOperation(ctx, "queue.dispatch segment=%d", segment.GetID())
			select {
			case jobs <- segment:
				end(nil)
			case <-scanCtx.Done():
				end(scanCtx.Err())
				return scanCtx.Err()
			}
		}
		return nil
	})
	for worker := range workers {
		g.Go(func() error {
			for {
				select {
				case <-scanCtx.Done():
					return scanCtx.Err()
				case segment, ok := <-jobs:
					if !ok {
						return nil
					}
					jobCtx := storagecommon.WithScanScope(scanCtx, "worker=%d segment=%d", worker, segment.GetID())
					end := storagecommon.ScanOperation(jobCtx, "segment.scan storage_version=%d rows=%d", segment.GetStorageVersion(), segment.GetNumOfRows())
					err := work(jobCtx, segment)
					end(err)
					if err != nil {
						if err == io.EOF {
							return errScanOutputLimit
						}
						return fmt.Errorf("scan segment %d: %w", segment.GetID(), err)
					}
				}
			}
		})
	}
	err := g.Wait()
	if errors.Is(err, errScanOutputLimit) {
		return nil
	}
	if err != nil {
		return err
	}
	return ctx.Err()
}
