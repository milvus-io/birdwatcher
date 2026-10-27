package states

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/ossutil"
	"github.com/milvus-io/birdwatcher/storage"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/birdwatcher/storage/tasks"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	milvuscommon "github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type ScanBinlogParams struct {
	oss.OSSOverrideParam
	framework.DataSetParam `use:"scan-binlog" desc:"scan binlog to check data"`
	CollectionID           int64    `name:"collection" default:"0"`
	PartitionID            int64    `name:"partition" default:"0"`
	SegmentID              int64    `name:"segment" default:"0"`
	Fields                 []string `name:"fields"`
	Field                  string   `name:"field" desc:"single target field required by count-distinct; automatically included in the read fields"`
	Expr                   string   `name:"expr"`
	MinioAddress           string   `name:"minioAddr"`
	SkipBucketCheck        bool     `name:"skipBucketCheck" default:"false" desc:"skip bucket exist check due to permission issue"`
	Action                 string   `name:"action" default:"count" desc:"action to execute: count, count-distinct, locate, dedup, or detect-vector-nulls" values:"count,count-distinct,locate,dedup,detect-vector-nulls"`
	IgnoreDelete           bool     `name:"ignoreDelete" default:"false" desc:"ignore delete logic"`
	IncludeUnhealthy       bool     `name:"includeUnhealthy" default:"false" desc:"also check dropped segments"`
	WorkerNum              int64    `name:"workerNum" default:"4" desc:"worker num"`
	ReadBufferSize         int64    `name:"readBufferSize" default:"10485760" desc:"internal v3 OSS read buffer size in bytes per open data/delta object; 0 uses the default 10 MiB"`
	BatchSize              int64    `name:"batchSize" default:"1024" desc:"Arrow record batch size for detect-vector-nulls action"`
	Exact                  bool     `name:"exact" default:"false" desc:"read every vector row and validate vector length; otherwise null-free Parquet row groups are inferred from metadata"`
	OutputLimit            int64    `name:"outputLimit" default:"10" desc:"output limit"`
	Verbose                bool     `name:"verbose" default:"false" desc:"log scan stages, workers, batches and object I/O to stderr; report pending operations every 10 seconds"`
}

func (s *InstanceState) ScanBinlogCommand(ctx context.Context, p *ScanBinlogParams) (result *framework.PresetResultSet, err error) {
	if p.Verbose {
		var stop func()
		ctx, stop = storagecommon.WithScanDebug(ctx, os.Stderr)
		defer stop()
	}
	ctx = storagecommon.WithScanScope(ctx, "collection=%d action=%s", p.CollectionID, p.Action)
	finish := storagecommon.ScanOperation(ctx, "command.scan partition=%d segment=%d workers=%d ignore_delete=%t", p.PartitionID, p.SegmentID, p.WorkerNum, p.IgnoreDelete)
	defer func() { finish(err) }()
	if p.WorkerNum <= 0 {
		return nil, errors.New("--workerNum must be positive")
	}
	if p.ReadBufferSize < 0 || int64(int(p.ReadBufferSize)) != p.ReadBufferSize {
		return nil, errors.New("--readBufferSize must be nonnegative and fit in int")
	}
	readBufferSize := int(p.ReadBufferSize)
	if readBufferSize == 0 {
		readBufferSize = storagecommon.DefaultReadBufferSize
	}
	storagecommon.ScanDebug(ctx, "v3.read_buffer bytes=%d", readBufferSize)
	action := strings.ToLower(p.Action)
	if action == "count-distinct" && strings.TrimSpace(p.Field) == "" {
		return nil, errors.New("--action count-distinct requires --field")
	}
	if action != "count-distinct" && p.Field != "" {
		return nil, errors.New("--field is only valid with --action count-distinct; use --fields to select read fields for other actions")
	}
	end := storagecommon.ScanOperation(ctx, "metadata.collection")
	collection, err := common.GetCollectionByIDVersion(ctx, s.client, s.basePath, p.CollectionID)
	end(err)
	if err != nil {
		return nil, err
	}
	if strings.EqualFold(p.Action, "detect-vector-nulls") {
		end := storagecommon.ScanOperation(ctx, "external.detect-vector-nulls")
		report, err := s.scanExternalVectorNullSegments(ctx, collection, p)
		end(err)
		if err != nil {
			return nil, err
		}
		return framework.NewPresetResultSet(report, p.GetFormat()), nil
	}
	fmt.Println("=== Checking collection schema ===")
	pkField, ok := collection.GetPKField()
	if !ok {
		return nil, errors.New("pk field not found")
	}
	fmt.Printf("PK Field [%d] %s\n", pkField.FieldID, pkField.Name)
	var scanTask tasks.ScanTask
	var distinctField *schemapb.FieldSchema
	switch action {
	case "count":
		scanTask = tasks.NewCountTask()
	case "count-distinct":
		for _, field := range collection.GetProto().GetSchema().GetFields() {
			if field.GetName() == strings.TrimSpace(p.Field) {
				distinctField = field
				break
			}
		}
		if distinctField == nil {
			return nil, errors.Newf("count-distinct field %q not found in collection %d", p.Field, p.CollectionID)
		}
		if distinctField.GetName() == milvuscommon.VirtualPKFieldName {
			return nil, errors.Newf("count-distinct does not support the virtual PK field %q; the virtual PK is derived from row offsets and is not a readable column", distinctField.GetName())
		}
		scanTask, err = tasks.NewCountDistinctTask(distinctField)
		if err != nil {
			return nil, err
		}
		storagecommon.ScanDebug(ctx, "distinct.target field_id=%d field_name=%q type=%s", distinctField.GetFieldID(), distinctField.GetName(), distinctField.GetDataType())
	case "locate":
		scanTask = tasks.NewLocateTask(p.OutputLimit, pkField)
	case "dedup":
		scanTask = tasks.NewDedupTask(p.OutputLimit, pkField)
	default:
		return nil, errors.Newf("unknown action: %s", p.Action)
	}

	fieldsMap := make(map[string]struct{})
	for _, field := range p.Fields {
		fieldsMap[field] = struct{}{}
	}
	if distinctField != nil {
		fieldsMap[distinctField.GetName()] = struct{}{}
	}

	fields := make(map[int64]*schemapb.FieldSchema)

	for _, fieldSchema := range collection.GetProto().Schema.Fields {
		// timestamp field id
		if fieldSchema.FieldID == 1 {
			fields[fieldSchema.FieldID] = fieldSchema
			continue
		}
		if fieldSchema.IsPrimaryKey {
			fmt.Printf("Output PK Field %s field id %d\n", fieldSchema.Name, fieldSchema.FieldID)
			fields[fieldSchema.FieldID] = fieldSchema
			continue
		}
		if _, ok := fieldsMap[fieldSchema.Name]; ok {
			fmt.Printf("Output Field %s field id %d\n", fieldSchema.Name, fieldSchema.FieldID)
			fields[fieldSchema.FieldID] = fieldSchema
		}
	}

	// Timestamp is required even when omitted from the persisted schema.
	if fields[1] == nil && !isExternalCollection(collection) {
		fields[1] = &schemapb.FieldSchema{FieldID: 1, Name: "Timestamp", DataType: schemapb.DataType_Int64}
	}
	selected := lo.Keys(fields)
	slices.Sort(selected)
	storagecommon.ScanDebug(ctx, "fields.selected ids=%v", selected)
	end = storagecommon.ScanOperation(ctx, "metadata.segments")
	segments, err := common.ListSegments(ctx, s.client, s.basePath, func(segment *models.Segment) bool {
		return scanSegmentMatches(segment, p)
	})
	end(err, fmt.Sprintf("segments=%d", len(segments)))
	if err != nil {
		return nil, err
	}

	end = storagecommon.ScanOperation(ctx, "object_store.internal.init")
	resolvedStore, err := oss.NewResolvedObjectStoreWithOverrides(ctx, p.OSSOverrideParam, p.MinioAddress, p.SkipBucketCheck, s.GetObjectStore)
	end(err)
	if err != nil {
		fmt.Println("Failed to create client,", err.Error())
		return nil, err
	}
	rootPath := resolvedStore.RootPath

	// External collections: build the external store with credentials from the
	// collection (role_arn / external_id / ...) instead of minio configuration.
	// A single manifest may mix external data files with internal function
	// output files (e.g. sparse vectors from varchar), so we route per-file.
	external := isExternalCollection(collection)
	var externalStore *oss.ResolvedObjectStore
	var externalLocation ossutil.ExternalSourceLocation
	if external {
		end = storagecommon.ScanOperation(ctx, "object_store.external.init")
		externalStore, externalLocation, err = ossutil.NewResolvedExternalObjectStoreFromCollection(ctx, collection, p.SkipBucketCheck)
		end(err)
		if err != nil {
			fmt.Println("Failed to create external store,", err.Error())
			return nil, err
		}
		fmt.Printf("External Source: %s\n", collection.GetProto().GetSchema().GetExternalSource())
	}

	fmt.Printf("=== start to execute \"%s\" task with filter expresion: \"%s\" ===\n", p.Action, p.Expr)
	fmt.Printf("=== worker num: %d, skip delete: %t ===\n", p.WorkerNum, p.IgnoreDelete)

	var exprFilter *storage.ExprFilter
	if p.Expr != "" {
		exprFilter, err = storage.NewExprFilter(fields, p.Expr)
		if err != nil {
			return nil, err
		}
	}

	getObjectFor := func(scanCtx context.Context) func(string) (storagecommon.ReadSeeker, error) {
		return func(binlogPath string) (storagecommon.ReadSeeker, error) {
			if external && externalStore != nil && isExternalPath(binlogPath) {
				key, err := ossutil.ResolveExternalObjectKey(externalLocation, binlogPath)
				if err != nil {
					return nil, err
				}
				return openScanObject(scanCtx, externalStore.Store, key)
			}
			return openScanObject(scanCtx, resolvedStore.Store, oss.ResolveObjectKey(rootPath, binlogPath))
		}
	}
	loadManifest := func(scanCtx context.Context, segment *models.Segment) (*scanManifest, error) {
		if segment.GetManifestPath() == "" && segment.GetStorageVersion() != 3 {
			return nil, nil
		}
		var extStore oss.ObjectStore
		if externalStore != nil {
			extStore = externalStore.Store
		}
		return loadScanManifest(scanCtx, segment, resolvedStore.Store, rootPath, extStore, externalLocation, readBufferSize)
	}

	l0Segments := lo.Filter(segments, func(segment *models.Segment, _ int) bool {
		return segment.Level == datapb.SegmentLevel_L0
	})
	normalSegments := lo.Filter(segments, func(segment *models.Segment, _ int) bool {
		return segment.Level != datapb.SegmentLevel_L0
	})
	storagecommon.ScanDebug(ctx, "segments.selected normal=%d l0=%d", len(normalSegments), len(l0Segments))

	type l0DeleteSet struct {
		segment *models.Segment
		records map[any]uint64
	}
	var l0Deletes []l0DeleteSet
	if !p.IgnoreDelete {
		for _, segment := range l0Segments {
			if !lo.SomeBy(normalSegments, func(target *models.Segment) bool { return scanL0Applies(segment, target) }) {
				storagecommon.ScanDebug(ctx, "l0.skip segment=%d reason=no_applicable_target", segment.GetID())
				continue
			}
			l0Ctx := storagecommon.WithScanScope(ctx, "l0_segment=%d", segment.GetID())
			m, err := loadManifest(l0Ctx, segment)
			if err != nil {
				return nil, err
			}
			records := make(map[any]uint64)
			if err := addScanDeletes(l0Ctx, segment, m, schemapb.DataType(pkField.DataType), getObjectFor(l0Ctx), records); err != nil {
				return nil, err
			}
			l0Deletes = append(l0Deletes, l0DeleteSet{segment: segment, records: records})
		}
	} else {
		storagecommon.ScanDebug(ctx, "deletes.skipped reason=ignoreDelete")
	}

	workFn := func(scanCtx context.Context, segment *models.Segment) error {
		if segment.GetStorageVersion() > 3 {
			return errors.Newf("unsupported storage version: %d", segment.GetStorageVersion())
		}
		m, err := loadManifest(scanCtx, segment)
		if err != nil {
			return err
		}
		getObject := getObjectFor(scanCtx)
		var filters []storage.EntryFilter
		if !p.IgnoreDelete {
			deletedRecords := make(map[any]uint64)
			end := storagecommon.ScanOperation(scanCtx, "deletes.merge_l0 sets=%d", len(l0Deletes))
			for _, set := range l0Deletes {
				if !scanL0Applies(set.segment, segment) {
					continue
				}
				for pk, ts := range set.records {
					if old, ok := deletedRecords[pk]; !ok || ts > old {
						deletedRecords[pk] = ts
					}
				}
			}
			end(nil, fmt.Sprintf("unique_pks=%d", len(deletedRecords)))
			if err := addScanDeletes(scanCtx, segment, m, schemapb.DataType(pkField.DataType), getObject, deletedRecords); err != nil {
				return err
			}
			filters = append(filters, storage.NewDeltalogFilter(deletedRecords))
		}
		if exprFilter != nil {
			filters = append(filters, exprFilter)
		}

		// External collections store data in manifest-referenced files that may
		// mix external source files and internal function-output files. Route
		// them through the manifest resolver instead of the binlog iterator.
		if external && externalStore != nil && segment.GetManifestPath() != "" {
			storagecommon.ScanDebug(scanCtx, "reader.route external_manifest")
			return scanExternalSegment(scanCtx,
				resolvedStore.Store,
				rootPath,
				externalStore.Store,
				externalLocation,
				segment,
				collection.GetProto().GetSchema(),
				fields,
				filters,
				scanTask,
			)
		}

		iter := storage.NewSegmentIterator(segment,
			collection.GetProto().GetSchema(),
			filters,
			fields,
			getObject,
			scanTask)

		if m != nil {
			end := storagecommon.ScanOperation(scanCtx, "reader.manifest.init groups=%d", len(m.data.ColumnGroups))
			reader, err := storage.NewManifestSegmentReader(segment, m.data, fields, func(name string) (storagecommon.ReadSeeker, error) {
				return m.open(name, "_data")
			})
			end(err)
			if err != nil {
				return err
			}
			return iter.RangeReader(scanCtx, reader)
		}
		storagecommon.ScanDebug(scanCtx, "reader.route legacy storage_version=%d", segment.GetStorageVersion())
		return iter.Range(scanCtx)
	}

	num := atomic.NewInt64(0)
	err = runBinlogScan(ctx, normalSegments, int(p.WorkerNum), func(scanCtx context.Context, segment *models.Segment) error {
		if err := workFn(scanCtx, segment); err != nil {
			return err
		}
		fmt.Printf("%d/%d done, current counter: %d\n", num.Inc(), len(normalSegments), scanTask.Counter())
		return nil
	})
	if err != nil {
		return nil, err
	}

	scanTask.Summary()

	return nil, nil
}
