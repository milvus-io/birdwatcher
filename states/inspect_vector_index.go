package states

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/bits"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	binlogv1 "github.com/milvus-io/birdwatcher/storage/binlog/v1"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	vectorValidDataCount = "valid_data_count"
	vectorValidData      = "valid_data"
)

type InspectVectorIndexParams struct {
	framework.DataSetParam `use:"inspect-vector-index" desc:"inspect vector index validity bitmaps and count indexed versus null rows"`
	CollectionID           int64  `name:"collection" default:"0" desc:"collection id to filter with"`
	PartitionID            int64  `name:"partition" default:"0" desc:"partition id to filter with"`
	SegmentID              int64  `name:"segment" default:"0" desc:"segment id to filter with"`
	FieldID                int64  `name:"field" default:"0" desc:"field id to filter with"`
	IndexID                int64  `name:"indexID" default:"0" desc:"collection index id to filter with"`
	BuildID                int64  `name:"buildID" default:"0" desc:"index build id to filter with"`
	MinioAddress           string `name:"minioAddr" default:"" desc:"override minio address"`
	SkipBucketCheck        bool   `name:"skipBucketCheck" default:"false" desc:"skip bucket existence check due to permission restrictions"`
	IncludeDeleted         bool   `name:"includeDeleted" default:"false" desc:"include deleted SegmentIndex metadata"`
	MismatchOnly           bool   `name:"mismatchOnly" default:"false" desc:"only output segment_id, segment_rows, and valid_rows when valid_rows differs from segment_rows; requires field"`
}

type VectorIndexValidityRecord struct {
	CollectionID    int64    `json:"collection_id"`
	PartitionID     int64    `json:"partition_id"`
	SegmentID       int64    `json:"segment_id"`
	FieldID         int64    `json:"field_id"`
	FieldName       string   `json:"field_name,omitempty"`
	FieldType       string   `json:"field_type,omitempty"`
	Nullable        bool     `json:"nullable"`
	IndexID         int64    `json:"index_id"`
	IndexType       string   `json:"index_type,omitempty"`
	BuildID         int64    `json:"build_id"`
	IndexVersion    int64    `json:"index_version"`
	IndexState      string   `json:"index_state"`
	FailReason      string   `json:"fail_reason,omitempty"`
	SegmentRows     int64    `json:"segment_rows"`
	LogicalRows     int64    `json:"logical_rows"`
	ValidRows       int64    `json:"valid_rows"`
	NullRows        int64    `json:"null_rows"`
	BitmapPresent   bool     `json:"bitmap_present"`
	CountSource     string   `json:"count_source"`
	Status          string   `json:"status"`
	Message         string   `json:"message,omitempty"`
	ObjectKeys      []string `json:"object_keys,omitempty"`
	InspectionError string   `json:"inspection_error,omitempty"`
}

type VectorIndexValidityReport struct {
	Records []*VectorIndexValidityRecord `json:"records"`
}

type SegmentIndexRowsMismatchRecord struct {
	SegmentID   int64 `json:"segment_id"`
	SegmentRows int64 `json:"segment_rows"`
	ValidRows   int64 `json:"valid_rows"`
}

type SegmentIndexRowsMismatchReport struct {
	Records             []*SegmentIndexRowsMismatchRecord `json:"records"`
	UninspectedSegments int64                             `json:"uninspected_segments"`
}

func (r *SegmentIndexRowsMismatchReport) Entities() any {
	return r
}

func (r *SegmentIndexRowsMismatchReport) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatJSON:
		return framework.MarshalJSON(r)
	case framework.FormatLine:
		var sb strings.Builder
		appendLine := func(data []byte) {
			if sb.Len() > 0 {
				sb.WriteByte('\n')
			}
			sb.Write(data)
		}
		for _, record := range r.Records {
			data, err := json.Marshal(record)
			if err != nil {
				continue
			}
			appendLine(data)
		}
		if r.UninspectedSegments > 0 {
			data, _ := json.Marshal(struct {
				UninspectedSegments int64 `json:"uninspected_segments"`
			}{UninspectedSegments: r.UninspectedSegments})
			appendLine(data)
		}
		return sb.String()
	default:
		var sb strings.Builder
		fmt.Fprintf(&sb, "Vector index row mismatches: %d segment(s)", len(r.Records))
		if r.UninspectedSegments > 0 {
			fmt.Fprintf(&sb, "\nWarning: %d Finished segment index(es) could not be inspected", r.UninspectedSegments)
		}
		for _, record := range r.Records {
			fmt.Fprintf(&sb, "\nsegment=%d segment_rows=%d valid_rows=%d",
				record.SegmentID, record.SegmentRows, record.ValidRows)
		}
		return sb.String()
	}
}

func (r *SegmentIndexRowsMismatchReport) TableHeaders() table.Row {
	return table.Row{"Segment", "Segment rows", "Valid rows"}
}

func (r *SegmentIndexRowsMismatchReport) TableRows() []table.Row {
	rows := make([]table.Row, 0, len(r.Records))
	for _, record := range r.Records {
		rows = append(rows, table.Row{record.SegmentID, record.SegmentRows, record.ValidRows})
	}
	return rows
}

func (r *SegmentIndexRowsMismatchReport) TableTitle() string {
	return fmt.Sprintf(
		"Vector index row mismatches: %d segment(s), uninspected: %d",
		len(r.Records), r.UninspectedSegments,
	)
}

func buildSegmentIndexRowsMismatchReport(report *VectorIndexValidityReport) *SegmentIndexRowsMismatchReport {
	mismatches := &SegmentIndexRowsMismatchReport{
		Records: make([]*SegmentIndexRowsMismatchRecord, 0),
	}
	for _, record := range report.Records {
		if record.IndexState != commonpb.IndexState_Finished.String() {
			continue
		}
		if record.InspectionError != "" ||
			(!record.BitmapPresent && record.Status != "INFERRED_ALL_VALID") {
			mismatches.UninspectedSegments++
			continue
		}
		if record.ValidRows == record.SegmentRows {
			continue
		}
		mismatches.Records = append(mismatches.Records, &SegmentIndexRowsMismatchRecord{
			SegmentID:   record.SegmentID,
			SegmentRows: record.SegmentRows,
			ValidRows:   record.ValidRows,
		})
	}
	return mismatches
}

func (r *VectorIndexValidityReport) Entities() any {
	return r
}

func (r *VectorIndexValidityReport) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatJSON:
		return framework.MarshalJSON(r)
	case framework.FormatLine:
		var sb strings.Builder
		for _, record := range r.Records {
			data, err := json.Marshal(record)
			if err != nil {
				continue
			}
			sb.Write(data)
			sb.WriteByte('\n')
		}
		return strings.TrimSuffix(sb.String(), "\n")
	default:
		return r.printPlain()
	}
}

func (r *VectorIndexValidityReport) TableHeaders() table.Row {
	return table.Row{"Segment", "Field", "Index", "Build", "Index state", "Nullable", "Logical", "Valid", "Null", "Source", "Status"}
}

func (r *VectorIndexValidityReport) TableRows() []table.Row {
	rows := make([]table.Row, 0, len(r.Records))
	for _, record := range r.Records {
		rows = append(rows, table.Row{
			record.SegmentID,
			fmt.Sprintf("%s(%d)", record.FieldName, record.FieldID),
			fmt.Sprintf("%s(%d)", record.IndexType, record.IndexID),
			record.BuildID,
			record.IndexState,
			record.Nullable,
			record.LogicalRows,
			record.ValidRows,
			record.NullRows,
			record.CountSource,
			record.Status,
		})
	}
	return rows
}

func (r *VectorIndexValidityReport) TableTitle() string {
	return fmt.Sprintf("Vector index validity: %d record(s)", len(r.Records))
}

func (r *VectorIndexValidityReport) printPlain() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Vector index validity: %d record(s)\n", len(r.Records))
	for _, record := range r.Records {
		fmt.Fprintf(&sb,
			"segment=%d field=%s(%d) index=%s(%d) build=%d index_state=%s logical=%d valid=%d null=%d source=%s status=%s\n",
			record.SegmentID, record.FieldName, record.FieldID, record.IndexType, record.IndexID,
			record.BuildID, record.IndexState, record.LogicalRows, record.ValidRows, record.NullRows,
			record.CountSource, record.Status)
		if record.FailReason != "" {
			fmt.Fprintf(&sb, "  fail reason: %s\n", record.FailReason)
		}
		if record.Message != "" {
			fmt.Fprintf(&sb, "  message: %s\n", record.Message)
		}
		if record.InspectionError != "" {
			fmt.Fprintf(&sb, "  error: %s\n", record.InspectionError)
		}
		for _, objectKey := range record.ObjectKeys {
			fmt.Fprintf(&sb, "  object: %s\n", objectKey)
		}
	}
	return strings.TrimSuffix(sb.String(), "\n")
}

type vectorIndexMetadata struct {
	segmentIndexes []*indexpb.SegmentIndex
	fieldIndexes   map[v3IndexKey]*indexpb.IndexInfo
	collections    map[int64]*models.Collection
}

func (s *InstanceState) InspectVectorIndexCommand(ctx context.Context, p *InspectVectorIndexParams) (*framework.PresetResultSet, error) {
	if p.CollectionID == 0 && p.SegmentID == 0 && p.BuildID == 0 {
		return nil, fmt.Errorf("at least one of collection, segment, or buildID must be specified")
	}
	if p.MismatchOnly && p.FieldID == 0 {
		return nil, fmt.Errorf("mismatchOnly requires field because valid_rows is field-specific")
	}

	metadata, err := s.loadVectorIndexMetadata(ctx, p)
	if err != nil {
		return nil, err
	}
	report := &VectorIndexValidityReport{Records: make([]*VectorIndexValidityRecord, 0)}
	var resolvedStore *oss.ResolvedObjectStore
	for _, segmentIndex := range metadata.segmentIndexes {
		fieldIndex := metadata.fieldIndexes[v3IndexKey{
			collectionID: segmentIndex.GetCollectionID(),
			indexID:      segmentIndex.GetIndexID(),
		}]
		if fieldIndex == nil {
			continue
		}
		if p.FieldID != 0 && fieldIndex.GetFieldID() != p.FieldID {
			continue
		}

		field := findVectorField(metadata.collections[segmentIndex.GetCollectionID()], fieldIndex.GetFieldID())
		if field == nil || !typeutil.IsVectorType(field.GetDataType()) {
			continue
		}
		if vectorIndexNeedsObjectStore(segmentIndex) && resolvedStore == nil {
			params := []oss.MinioConnectParam{oss.WithSkipCheckBucket(p.SkipBucketCheck)}
			if p.MinioAddress != "" {
				params = append(params, oss.WithMinioAddr(p.MinioAddress))
			}
			resolvedStore, err = s.GetObjectStore(ctx, params...)
			if err != nil {
				return nil, err
			}
		}

		record := inspectVectorIndexValidity(ctx, resolvedStore, segmentIndex, fieldIndex, field)
		report.Records = append(report.Records, record)
	}

	if len(report.Records) == 0 {
		return nil, fmt.Errorf("no vector SegmentIndex matched the supplied filters")
	}
	sort.Slice(report.Records, func(i, j int) bool {
		if report.Records[i].SegmentID != report.Records[j].SegmentID {
			return report.Records[i].SegmentID < report.Records[j].SegmentID
		}
		if report.Records[i].FieldID != report.Records[j].FieldID {
			return report.Records[i].FieldID < report.Records[j].FieldID
		}
		return report.Records[i].IndexID < report.Records[j].IndexID
	})

	if p.MismatchOnly {
		return framework.NewPresetResultSet(
			buildSegmentIndexRowsMismatchReport(report), framework.NameFormat(p.Format)), nil
	}
	return framework.NewPresetResultSet(report, framework.NameFormat(p.Format)), nil
}

func vectorIndexNeedsObjectStore(segmentIndex *indexpb.SegmentIndex) bool {
	if segmentIndex.GetState() != commonpb.IndexState_Finished {
		return false
	}
	artifacts, err := classifyVectorValidityArtifacts(segmentIndex.GetIndexFileKeys())
	return err == nil && artifacts.kind != vectorValidityAbsent
}

func (s *InstanceState) loadVectorIndexMetadata(ctx context.Context, p *InspectVectorIndexParams) (*vectorIndexMetadata, error) {
	segmentIndexes, err := common.ListSegmentIndex(ctx, s.client, s.basePath, func(index *models.SegmentIndex) bool {
		proto := index.GetProto()
		return (p.IncludeDeleted || !proto.GetDeleted()) &&
			(p.CollectionID == 0 || proto.GetCollectionID() == p.CollectionID) &&
			(p.PartitionID == 0 || proto.GetPartitionID() == p.PartitionID) &&
			(p.SegmentID == 0 || proto.GetSegmentID() == p.SegmentID) &&
			(p.IndexID == 0 || proto.GetIndexID() == p.IndexID) &&
			(p.BuildID == 0 || proto.GetBuildID() == p.BuildID)
	})
	if err != nil {
		return nil, err
	}
	wantedIndexes := make(map[v3IndexKey]struct{}, len(segmentIndexes))
	wantedCollections := make(map[int64]struct{}, len(segmentIndexes))
	for _, index := range segmentIndexes {
		proto := index.GetProto()
		wantedIndexes[v3IndexKey{collectionID: proto.GetCollectionID(), indexID: proto.GetIndexID()}] = struct{}{}
		wantedCollections[proto.GetCollectionID()] = struct{}{}
	}
	fieldIndexes, err := common.ListIndex(ctx, s.client, s.basePath, func(index *models.FieldIndex) bool {
		info := index.GetProto().GetIndexInfo()
		if info == nil {
			return false
		}
		_, ok := wantedIndexes[v3IndexKey{collectionID: info.GetCollectionID(), indexID: info.GetIndexID()}]
		return ok
	})
	if err != nil {
		return nil, err
	}
	collections, err := common.ListCollections(ctx, s.client, s.basePath, func(collection *models.Collection) bool {
		_, ok := wantedCollections[collection.GetProto().GetID()]
		return ok
	})
	if err != nil {
		return nil, err
	}

	metadata := &vectorIndexMetadata{
		segmentIndexes: make([]*indexpb.SegmentIndex, 0, len(segmentIndexes)),
		fieldIndexes:   make(map[v3IndexKey]*indexpb.IndexInfo),
		collections:    make(map[int64]*models.Collection),
	}
	for _, index := range segmentIndexes {
		metadata.segmentIndexes = append(metadata.segmentIndexes, index.GetProto())
	}
	for _, index := range fieldIndexes {
		info := index.GetProto().GetIndexInfo()
		if info != nil {
			metadata.fieldIndexes[v3IndexKey{collectionID: info.GetCollectionID(), indexID: info.GetIndexID()}] = info
		}
	}
	for _, collection := range collections {
		metadata.collections[collection.GetProto().GetID()] = collection
	}
	return metadata, nil
}

func findVectorField(collection *models.Collection, fieldID int64) *schemapb.FieldSchema {
	if collection == nil || collection.GetProto().GetSchema() == nil {
		return nil
	}
	for _, field := range collection.GetProto().GetSchema().GetFields() {
		if field.GetFieldID() == fieldID {
			return field
		}
	}
	return nil
}

func inspectVectorIndexValidity(ctx context.Context, resolvedStore *oss.ResolvedObjectStore, segmentIndex *indexpb.SegmentIndex, fieldIndex *indexpb.IndexInfo, field *schemapb.FieldSchema) *VectorIndexValidityRecord {
	indexType := segmentIndex.GetIndexType()
	if indexType == "" {
		indexType = common.GetKVPair(fieldIndex.GetIndexParams(), "index_type")
	}
	record := &VectorIndexValidityRecord{
		CollectionID: segmentIndex.GetCollectionID(),
		PartitionID:  segmentIndex.GetPartitionID(),
		SegmentID:    segmentIndex.GetSegmentID(),
		FieldID:      field.GetFieldID(),
		FieldName:    field.GetName(),
		FieldType:    field.GetDataType().String(),
		Nullable:     field.GetNullable(),
		IndexID:      segmentIndex.GetIndexID(),
		IndexType:    indexType,
		BuildID:      segmentIndex.GetBuildID(),
		IndexVersion: segmentIndex.GetIndexVersion(),
		IndexState:   segmentIndex.GetState().String(),
		FailReason:   segmentIndex.GetFailReason(),
		SegmentRows:  segmentIndex.GetNumRows(),
		Status:       "ERROR",
	}
	if segmentIndex.GetState() != commonpb.IndexState_Finished {
		if segmentIndex.GetState() == commonpb.IndexState_Failed {
			record.Status = "INDEX_FAILED"
			record.Message = "index build failed; validity artifacts are not inspected"
		} else {
			record.Status = "INDEX_NOT_FINISHED"
			record.Message = fmt.Sprintf(
				"index build state is %s; validity artifacts are only inspected for Finished indexes",
				segmentIndex.GetState(),
			)
		}
		return record
	}

	artifacts, err := classifyVectorValidityArtifacts(segmentIndex.GetIndexFileKeys())
	if err != nil {
		record.InspectionError = err.Error()
		return record
	}
	if artifacts.kind == vectorValidityAbsent {
		if field.GetNullable() {
			record.InspectionError = "nullable vector index has no valid_data artifacts"
			return record
		}
		if segmentIndex.GetNumRows() < 0 {
			record.InspectionError = fmt.Sprintf("SegmentIndex has invalid num_rows %d", segmentIndex.GetNumRows())
			return record
		}
		record.LogicalRows = segmentIndex.GetNumRows()
		record.ValidRows = segmentIndex.GetNumRows()
		record.NullRows = 0
		record.CountSource = "segment_index_metadata"
		record.Status = "INFERRED_ALL_VALID"
		record.Message = "non-nullable vector index has no validity bitmap; valid rows are inferred from SegmentIndex.num_rows"
		return record
	}
	if resolvedStore == nil {
		record.InspectionError = "object store is required to inspect valid_data artifacts"
		return record
	}

	var totalRows, validRows uint64
	switch artifacts.kind {
	case vectorValidityMemory:
		countPayload, countKeys, err := readVectorArtifactParts(ctx, resolvedStore, segmentIndex, artifacts.countParts)
		record.ObjectKeys = append(record.ObjectKeys, countKeys...)
		if err != nil {
			record.InspectionError = err.Error()
			return record
		}
		bitmapPayload, bitmapKeys, err := readVectorArtifactParts(ctx, resolvedStore, segmentIndex, artifacts.dataParts)
		record.ObjectKeys = append(record.ObjectKeys, bitmapKeys...)
		if err != nil {
			record.InspectionError = err.Error()
			return record
		}
		totalRows, validRows, err = decodeMemoryVectorValidity(countPayload, bitmapPayload)
		if err != nil {
			record.InspectionError = err.Error()
			return record
		}
		record.CountSource = "memory_valid_data_bitmap"
	case vectorValidityDisk:
		payload, objectKeys, err := readVectorArtifactParts(ctx, resolvedStore, segmentIndex, artifacts.dataParts)
		record.ObjectKeys = append(record.ObjectKeys, objectKeys...)
		if err != nil {
			record.InspectionError = err.Error()
			return record
		}
		totalRows, validRows, err = decodeDiskVectorValidity(payload)
		if err != nil {
			record.InspectionError = err.Error()
			return record
		}
		record.CountSource = "disk_valid_data_bitmap"
	}

	if totalRows > math.MaxInt64 || validRows > math.MaxInt64 {
		record.InspectionError = "valid_data row count exceeds int64"
		return record
	}
	record.LogicalRows = int64(totalRows)
	record.ValidRows = int64(validRows)
	record.NullRows = int64(totalRows - validRows)
	record.BitmapPresent = true
	record.Status = "OK"
	if record.LogicalRows != segmentIndex.GetNumRows() {
		record.Status = "COUNT_MISMATCH"
		record.Message = fmt.Sprintf("valid_data logical rows %d differ from SegmentIndex.num_rows %d", record.LogicalRows, segmentIndex.GetNumRows())
	}
	return record
}

type vectorValidityKind int

const (
	vectorValidityAbsent vectorValidityKind = iota
	vectorValidityMemory
	vectorValidityDisk
)

type vectorArtifactPart struct {
	key   string
	exact bool
	index int
}

type vectorValidityArtifacts struct {
	kind       vectorValidityKind
	countParts []vectorArtifactPart
	dataParts  []vectorArtifactPart
}

func classifyVectorValidityArtifacts(keys []string) (vectorValidityArtifacts, error) {
	artifacts := vectorValidityArtifacts{kind: vectorValidityAbsent}
	for _, key := range keys {
		name := path.Base(strings.TrimSpace(key))
		if exact, index, ok := parseVectorArtifactName(name, vectorValidDataCount); ok {
			artifacts.countParts = append(artifacts.countParts, vectorArtifactPart{key: key, exact: exact, index: index})
			continue
		}
		if exact, index, ok := parseVectorArtifactName(name, vectorValidData); ok {
			artifacts.dataParts = append(artifacts.dataParts, vectorArtifactPart{key: key, exact: exact, index: index})
		}
	}

	if err := validateAndSortVectorArtifactParts(artifacts.countParts, vectorValidDataCount); err != nil {
		return artifacts, err
	}
	if err := validateAndSortVectorArtifactParts(artifacts.dataParts, vectorValidData); err != nil {
		return artifacts, err
	}

	switch {
	case len(artifacts.countParts) > 0 && len(artifacts.dataParts) > 0:
		artifacts.kind = vectorValidityMemory
	case len(artifacts.countParts) > 0:
		return artifacts, fmt.Errorf("vector index has %s but no %s artifact", vectorValidDataCount, vectorValidData)
	case len(artifacts.dataParts) > 0 && !artifacts.dataParts[0].exact:
		artifacts.kind = vectorValidityDisk
	case len(artifacts.dataParts) > 0:
		return artifacts, fmt.Errorf("vector index has %s but no %s artifact", vectorValidData, vectorValidDataCount)
	}
	return artifacts, nil
}

func parseVectorArtifactName(name, prefix string) (bool, int, bool) {
	if name == prefix {
		return true, 0, true
	}
	if !strings.HasPrefix(name, prefix+"_") {
		return false, 0, false
	}
	index, err := strconv.Atoi(strings.TrimPrefix(name, prefix+"_"))
	if err != nil || index < 0 {
		return false, 0, false
	}
	return false, index, true
}

func validateAndSortVectorArtifactParts(parts []vectorArtifactPart, name string) error {
	if len(parts) == 0 {
		return nil
	}
	exact := 0
	for _, part := range parts {
		if part.exact {
			exact++
		}
	}
	if exact > 0 {
		if len(parts) != 1 {
			return fmt.Errorf("%s has both an unsliced object and slice objects", name)
		}
		return nil
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].index < parts[j].index })
	for index, part := range parts {
		if part.index != index {
			return fmt.Errorf("%s slices are incomplete: expected slice %d, got %d", name, index, part.index)
		}
	}
	return nil
}

func readVectorArtifactParts(ctx context.Context, resolvedStore *oss.ResolvedObjectStore, segmentIndex *indexpb.SegmentIndex, parts []vectorArtifactPart) ([]byte, []string, error) {
	var payload []byte
	objectKeys := make([]string, 0, len(parts))
	for _, part := range parts {
		filename := path.Base(strings.TrimSpace(part.key))
		resolved := buildV3ResolvedPath(int32(segmentIndex.GetIndexStorePathVersion()), segmentIndex, filename, resolvedStore.RootPath)
		objectKey := normalizeV3AdvertisedPath(resolvedStore.RootPath, part.key, resolved)
		objectKey = oss.ResolveObjectKey("", objectKey)
		data, err := readVectorIndexObject(ctx, resolvedStore.Store, objectKey)
		if err != nil {
			return nil, objectKeys, fmt.Errorf("read index object %s: %w", objectKey, err)
		}
		objectKeys = append(objectKeys, objectKey)
		payload = append(payload, data...)
	}
	return payload, objectKeys, nil
}

func readVectorIndexObject(ctx context.Context, store oss.ObjectStore, objectKey string) ([]byte, error) {
	object, err := store.Open(ctx, objectKey)
	if err != nil {
		return nil, err
	}
	if closer, ok := object.(io.Closer); ok {
		defer closer.Close()
	}

	reader, descriptor, err := binlogv1.NewIndexReader(object)
	if err != nil {
		return nil, err
	}
	if value, ok := descriptor.Extras["edek"]; ok && fmt.Sprint(value) != "" {
		return nil, fmt.Errorf("encrypted index object is not supported")
	}
	switch descriptor.PayloadDataType {
	case schemapb.DataType_None:
		return reader.NextRawEventReader(object)
	case schemapb.DataType_Int8:
		payloads, err := reader.NextEventReader(object, descriptor.PayloadDataType)
		if err != nil {
			return nil, err
		}
		if len(payloads) != 1 {
			return nil, fmt.Errorf("expected one index payload, got %d", len(payloads))
		}
		return payloads[0], nil
	default:
		return nil, fmt.Errorf("unsupported index payload data type %s", descriptor.PayloadDataType.String())
	}
}

func decodeMemoryVectorValidity(countPayload, bitmapPayload []byte) (uint64, uint64, error) {
	if len(countPayload) != 8 {
		return 0, 0, fmt.Errorf("valid_data_count payload must be 8 bytes, got %d", len(countPayload))
	}
	totalRows := binary.LittleEndian.Uint64(countPayload)
	validRows, err := countVectorValidityBitmap(totalRows, bitmapPayload)
	return totalRows, validRows, err
}

func decodeDiskVectorValidity(payload []byte) (uint64, uint64, error) {
	if len(payload) < 8 {
		return 0, 0, fmt.Errorf("disk valid_data payload must contain an 8-byte count, got %d bytes", len(payload))
	}
	totalRows := binary.LittleEndian.Uint64(payload[:8])
	validRows, err := countVectorValidityBitmap(totalRows, payload[8:])
	return totalRows, validRows, err
}

func countVectorValidityBitmap(totalRows uint64, bitmap []byte) (uint64, error) {
	requiredBytes := totalRows / 8
	if totalRows%8 != 0 {
		requiredBytes++
	}
	if requiredBytes > uint64(len(bitmap)) {
		return 0, fmt.Errorf("valid_data bitmap needs %d bytes for %d rows, got %d", requiredBytes, totalRows, len(bitmap))
	}
	if requiredBytes > uint64(math.MaxInt) {
		return 0, fmt.Errorf("valid_data bitmap is too large")
	}

	fullBytes := totalRows / 8
	var validRows uint64
	for _, value := range bitmap[:int(fullBytes)] {
		validRows += uint64(bits.OnesCount8(value))
	}
	if remainder := totalRows % 8; remainder != 0 {
		mask := byte((uint16(1) << remainder) - 1)
		validRows += uint64(bits.OnesCount8(bitmap[fullBytes] & mask))
	}
	return validRows, nil
}
