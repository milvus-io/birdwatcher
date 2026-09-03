package states

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/bitutil"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/cockroachdb/errors"
	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type ExternalVectorNullRange struct {
	SegmentID          int64  `json:"segment_id"`
	ObjectKey          string `json:"object_key"`
	StartIndex         int64  `json:"start_index"`
	EndIndex           int64  `json:"end_index"`
	ArrowType          string `json:"arrow_type,omitempty"`
	Rows               int64  `json:"rows"`
	ValidRows          int64  `json:"valid_rows"`
	MetadataNoNullRows int64  `json:"metadata_no_null_rows"`
	RowNullRows        int64  `json:"row_null_rows"`
	FullNullRows       int64  `json:"full_null_rows"`
	PartialNullRows    int64  `json:"partial_null_rows"`
	InvalidLengthRows  int64  `json:"invalid_length_rows"`
	InspectionError    string `json:"inspection_error,omitempty"`
}

func (f *ExternalVectorNullRange) hasIssue() bool {
	return f.RowNullRows > 0 || f.FullNullRows > 0 || f.PartialNullRows > 0 ||
		f.InvalidLengthRows > 0 || f.InspectionError != ""
}

type ExternalVectorNullReport struct {
	CollectionID       int64                      `json:"collection_id"`
	CollectionName     string                     `json:"collection_name"`
	FieldID            int64                      `json:"field_id"`
	FieldName          string                     `json:"field_name"`
	ExternalField      string                     `json:"external_field"`
	FieldType          string                     `json:"field_type"`
	Nullable           bool                       `json:"nullable"`
	Dimension          int64                      `json:"dimension"`
	ExternalSource     string                     `json:"external_source"`
	Format             string                     `json:"format"`
	SourcePrefix       string                     `json:"source_prefix"`
	Exact              bool                       `json:"exact"`
	SegmentsMatched    int64                      `json:"segments_matched"`
	FilesFound         int64                      `json:"files_found"`
	FilesScanned       int64                      `json:"files_scanned"`
	RowGroupsScanned   int64                      `json:"row_groups_scanned"`
	RowGroupsSkipped   int64                      `json:"row_groups_skipped"`
	RangesFound        int64                      `json:"ranges_found"`
	RangesScanned      int64                      `json:"ranges_scanned"`
	RangesWithIssues   int64                      `json:"ranges_with_issues"`
	RangesFailed       int64                      `json:"ranges_failed"`
	Rows               int64                      `json:"rows"`
	ValidRows          int64                      `json:"valid_rows"`
	MetadataNoNullRows int64                      `json:"metadata_no_null_rows"`
	RowNullRows        int64                      `json:"row_null_rows"`
	FullNullRows       int64                      `json:"full_null_rows"`
	PartialNullRows    int64                      `json:"partial_null_rows"`
	InvalidLengthRows  int64                      `json:"invalid_length_rows"`
	Ranges             []*ExternalVectorNullRange `json:"ranges,omitempty"`
}

func (r *ExternalVectorNullReport) Entities() any {
	return r
}

func (r *ExternalVectorNullReport) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatJSON:
		return framework.MarshalJSON(r)
	case framework.FormatLine:
		var sb strings.Builder
		summary := *r
		summary.Ranges = nil
		payload, err := json.Marshal(&summary)
		if err == nil {
			sb.Write(payload)
		}
		for _, result := range r.Ranges {
			payload, err := json.Marshal(result)
			if err != nil {
				continue
			}
			if sb.Len() > 0 {
				sb.WriteByte('\n')
			}
			sb.Write(payload)
		}
		return sb.String()
	default:
		return r.printPlain()
	}
}

func (r *ExternalVectorNullReport) TableHeaders() table.Row {
	return table.Row{"Segment", "Object range", "Rows", "Valid", "Metadata no-null", "Row null", "Full null", "Partial null", "Invalid length", "Error"}
}

func (r *ExternalVectorNullReport) TableRows() []table.Row {
	rows := make([]table.Row, 0, len(r.Ranges))
	for _, result := range r.Ranges {
		rows = append(rows, table.Row{
			result.SegmentID,
			fmt.Sprintf("%s[%d,%d)", result.ObjectKey, result.StartIndex, result.EndIndex),
			result.Rows,
			result.ValidRows,
			result.MetadataNoNullRows,
			result.RowNullRows,
			result.FullNullRows,
			result.PartialNullRows,
			result.InvalidLengthRows,
			result.InspectionError,
		})
	}
	return rows
}

func (r *ExternalVectorNullReport) TableTitle() string {
	return fmt.Sprintf(
		"External vector nulls: collection=%d field=%s format=%s segments=%d files=%d/%d ranges=%d/%d rows=%d metadata_no_null=%d row_null=%d full_null=%d partial_null=%d invalid_length=%d failed=%d",
		r.CollectionID,
		r.FieldName,
		r.Format,
		r.SegmentsMatched,
		r.FilesScanned,
		r.FilesFound,
		r.RangesScanned,
		r.RangesFound,
		r.Rows,
		r.MetadataNoNullRows,
		r.RowNullRows,
		r.FullNullRows,
		r.PartialNullRows,
		r.InvalidLengthRows,
		r.RangesFailed,
	)
}

func (r *ExternalVectorNullReport) printPlain() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "External vector null scan\n")
	fmt.Fprintf(&sb, "Collection: %s(%d)\n", r.CollectionName, r.CollectionID)
	fmt.Fprintf(&sb, "Field: %s(%d) -> %s | type=%s nullable=%t dim=%d\n",
		r.FieldName, r.FieldID, r.ExternalField, r.FieldType, r.Nullable, r.Dimension)
	fmt.Fprintf(&sb, "Source: %s\n", r.ExternalSource)
	fmt.Fprintf(&sb, "Scope: format=%s prefix=%s segments=%d files=%d/%d ranges=%d/%d failed=%d with_issues=%d\n",
		r.Format, r.SourcePrefix, r.SegmentsMatched, r.FilesScanned, r.FilesFound,
		r.RangesScanned, r.RangesFound, r.RangesFailed, r.RangesWithIssues)
	fmt.Fprintf(&sb, "I/O: exact=%t row_groups_scanned=%d row_groups_skipped=%d\n",
		r.Exact, r.RowGroupsScanned, r.RowGroupsSkipped)
	fmt.Fprintf(&sb, "Definitions: row_null=parent list is null; full_null=expected-length parent-valid list with all child elements null; partial_null=expected-length parent-valid list with mixed valid/null child elements\n")
	fmt.Fprintf(&sb, "Rows: total=%d valid=%d metadata_no_null=%d row_null=%d full_null=%d partial_null=%d invalid_length=%d",
		r.Rows, r.ValidRows, r.MetadataNoNullRows, r.RowNullRows, r.FullNullRows, r.PartialNullRows, r.InvalidLengthRows)
	if r.MetadataNoNullRows > 0 {
		fmt.Fprintf(&sb, "\nNote: metadata_no_null rows are proven null-free by Parquet statistics; vector lengths were not read or validated")
	}
	if r.RangesFailed > 0 {
		fmt.Fprintf(&sb, "\nWarning: %d range(s) failed inspection; aggregate row counts are incomplete", r.RangesFailed)
	}
	for _, result := range r.Ranges {
		fmt.Fprintf(&sb,
			"\n  segment=%d object=%s range=[%d,%d) type=%s rows=%d valid=%d metadata_no_null=%d row_null=%d full_null=%d partial_null=%d invalid_length=%d",
			result.SegmentID,
			result.ObjectKey,
			result.StartIndex,
			result.EndIndex,
			result.ArrowType,
			result.Rows,
			result.ValidRows,
			result.MetadataNoNullRows,
			result.RowNullRows,
			result.FullNullRows,
			result.PartialNullRows,
			result.InvalidLengthRows,
		)
		if result.InspectionError != "" {
			fmt.Fprintf(&sb, " error=%q", result.InspectionError)
		}
	}
	return sb.String()
}

func (s *InstanceState) scanExternalVectorNullSegments(ctx context.Context, collection *models.Collection, p *ScanBinlogParams) (*ExternalVectorNullReport, error) {
	if collection == nil || collection.GetProto() == nil || collection.GetProto().GetSchema() == nil {
		return nil, errors.New("collection metadata does not contain a schema")
	}
	collectionInfo := collection.GetProto()
	schema := collectionInfo.GetSchema()
	if strings.TrimSpace(schema.GetExternalSource()) == "" {
		return nil, errors.Newf(
			"collection %d is not an external collection: external_source is empty", collectionInfo.GetID())
	}

	if len(p.Fields) != 1 || strings.TrimSpace(p.Fields[0]) == "" {
		return nil, errors.New("detect-vector-nulls action requires exactly one field name in --fields")
	}
	if p.WorkerNum <= 0 || p.WorkerNum > 64 {
		return nil, errors.New("--workerNum must be between 1 and 64")
	}
	if p.BatchSize <= 0 {
		return nil, errors.New("--batchSize must be positive")
	}

	field, externalField, dim, err := resolveExternalVectorField(schema, strings.TrimSpace(p.Fields[0]))
	if err != nil {
		return nil, err
	}
	spec, err := parseExternalSpec(schema.GetExternalSpec())
	if err != nil {
		return nil, err
	}
	format := strings.ToLower(strings.TrimSpace(spec.Format))
	if format == "" {
		format = "parquet"
	}
	if format != "parquet" && format != "lance-table" {
		return nil, errors.Newf("external collection format %s is not supported", format)
	}
	if format == "lance-table" {
		if err := ensureLanceVectorScannerAvailable(); err != nil {
			return nil, err
		}
	}

	segments, err := common.ListSegments(ctx, s.client, s.basePath, func(segment *models.Segment) bool {
		return (p.SegmentID == 0 || p.SegmentID == segment.ID) &&
			p.CollectionID == segment.CollectionID &&
			(p.PartitionID == 0 || p.PartitionID == segment.PartitionID) &&
			(p.IncludeUnhealthy || segment.State != commonpb.SegmentState_Dropped)
	})
	if err != nil {
		return nil, err
	}
	segments, err = selectExternalCollectionSegments(segments, p.SegmentID)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return nil, errors.New("no external collection segments matched the supplied filters")
	}

	manifestParams := []oss.MinioConnectParam{oss.WithSkipCheckBucket(p.SkipBucketCheck)}
	if p.MinioAddress != "" {
		manifestParams = append(manifestParams, oss.WithMinioAddr(p.MinioAddress))
	}
	manifestStore, err := s.GetObjectStore(ctx, manifestParams...)
	if err != nil {
		return nil, err
	}

	var externalStore oss.ObjectStore
	var externalRootPath string
	var location externalSourceLocation
	if format == "parquet" {
		store, _, rootPath, resolvedLocation, err := newExternalObjectStore(
			ctx, schema.GetExternalSource(), spec, p.SkipBucketCheck)
		if err != nil {
			return nil, err
		}
		externalStore = store
		externalRootPath = rootPath
		location = resolvedLocation
	} else {
		location, err = externalSourceLocationForSpec(schema.GetExternalSource(), spec)
		if err != nil {
			return nil, err
		}
		externalRootPath = location.RootPath
	}

	ranges := make([]externalVectorSegmentRange, 0)
	for _, segment := range segments {
		segmentRanges, err := externalVectorRangesForSegment(
			ctx,
			manifestStore.Store,
			manifestStore.RootPath,
			location,
			segment,
			field.GetFieldID(),
			externalField,
			format,
		)
		if err != nil {
			return nil, errors.Wrapf(err, "resolve vector source ranges for segment %d", segment.GetID())
		}
		ranges = append(ranges, segmentRanges...)
	}
	if len(ranges) == 0 {
		return nil, errors.Newf("no source ranges found for field %s in matched segment manifests", field.GetName())
	}
	filesFound := make(map[string]struct{})
	for _, sourceRange := range ranges {
		filesFound[sourceRange.ObjectKey] = struct{}{}
	}

	report := &ExternalVectorNullReport{
		CollectionID:    collectionInfo.GetID(),
		CollectionName:  schema.GetName(),
		FieldID:         field.GetFieldID(),
		FieldName:       field.GetName(),
		ExternalField:   externalField,
		FieldType:       field.GetDataType().String(),
		Nullable:        field.GetNullable(),
		Dimension:       dim,
		ExternalSource:  redactExternalVectorSource(schema.GetExternalSource()),
		Format:          format,
		SourcePrefix:    externalRootPath,
		Exact:           p.Exact || format == "lance-table",
		SegmentsMatched: int64(len(segments)),
		FilesFound:      int64(len(filesFound)),
		RangesFound:     int64(len(ranges)),
		Ranges:          make([]*ExternalVectorNullRange, 0),
	}

	var results <-chan externalVectorObjectResult
	if format == "lance-table" {
		results = scanLanceVectorNullRanges(
			ctx, ranges, externalField, field.GetDataType(), dim,
			int(p.WorkerNum), p.BatchSize, location, spec,
		)
	} else {
		results = scanExternalVectorNullRanges(
			ctx, externalStore, ranges, externalField, field.GetDataType(), dim,
			int(p.WorkerNum), p.BatchSize, p.Exact,
		)
	}
	for objectResult := range results {
		report.FilesScanned++
		report.RowGroupsScanned += objectResult.RowGroupsScanned
		report.RowGroupsSkipped += objectResult.RowGroupsSkipped
		for _, result := range objectResult.Ranges {
			report.RangesScanned++
			report.Rows += result.Rows
			report.ValidRows += result.ValidRows
			report.MetadataNoNullRows += result.MetadataNoNullRows
			report.RowNullRows += result.RowNullRows
			report.FullNullRows += result.FullNullRows
			report.PartialNullRows += result.PartialNullRows
			report.InvalidLengthRows += result.InvalidLengthRows
			if result.InspectionError != "" {
				report.RangesFailed++
			}
			if result.hasIssue() {
				report.RangesWithIssues++
				report.Ranges = append(report.Ranges, result)
			}
		}
	}
	sort.Slice(report.Ranges, func(i, j int) bool {
		if report.Ranges[i].SegmentID != report.Ranges[j].SegmentID {
			return report.Ranges[i].SegmentID < report.Ranges[j].SegmentID
		}
		if report.Ranges[i].ObjectKey != report.Ranges[j].ObjectKey {
			return report.Ranges[i].ObjectKey < report.Ranges[j].ObjectKey
		}
		return report.Ranges[i].StartIndex < report.Ranges[j].StartIndex
	})

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return report, nil
}

func selectExternalCollectionSegments(segments []*models.Segment, requestedSegmentID int64) ([]*models.Segment, error) {
	externalSegments := make([]*models.Segment, 0, len(segments))
	requestedSegmentWithoutManifest := false
	for _, segment := range segments {
		if strings.TrimSpace(segment.GetManifestPath()) == "" {
			if requestedSegmentID != 0 {
				requestedSegmentWithoutManifest = true
			}
			continue
		}
		externalSegments = append(externalSegments, segment)
	}
	if requestedSegmentWithoutManifest && len(externalSegments) == 0 {
		return nil, errors.Newf(
			"segment %d is not an external collection segment: manifest_path is empty",
			requestedSegmentID,
		)
	}
	return externalSegments, nil
}

func resolveExternalVectorField(schema *schemapb.CollectionSchema, fieldName string) (*schemapb.FieldSchema, string, int64, error) {
	for _, field := range schema.GetFields() {
		if field.GetName() != fieldName {
			continue
		}
		if !typeutil.IsFixDimVectorType(field.GetDataType()) {
			return nil, "", 0, errors.Newf(
				"field %s has unsupported type %s; only fixed-dimension vector fields are supported",
				fieldName, field.GetDataType())
		}
		externalField := strings.TrimSpace(field.GetExternalField())
		if externalField == "" {
			return nil, "", 0, errors.Newf("field %s is not mapped to an external source column", fieldName)
		}
		dim, err := typeutil.GetDim(field)
		if err != nil {
			return nil, "", 0, errors.Wrapf(err, "get dimension for field %s", fieldName)
		}
		if dim <= 0 {
			return nil, "", 0, errors.Newf("field %s has invalid dimension %d", fieldName, dim)
		}
		return field, externalField, dim, nil
	}
	return nil, "", 0, errors.Newf("field %s not found in collection schema", fieldName)
}

type externalVectorSegmentRange struct {
	SegmentID  int64
	ObjectKey  string
	Format     string
	StartIndex int64
	EndIndex   int64
}

func externalVectorRangesForSegment(ctx context.Context, manifestStore oss.ObjectStore, manifestRootPath string, location externalSourceLocation, segment *models.Segment, fieldID int64, externalField, sourceFormat string) ([]externalVectorSegmentRange, error) {
	if segment.GetManifestPath() == "" {
		return nil, errors.New("segment does not have a refresh manifest")
	}
	var manifestRef struct {
		Ver      int    `json:"ver"`
		BasePath string `json:"base_path"`
	}
	if err := json.Unmarshal([]byte(segment.GetManifestPath()), &manifestRef); err != nil {
		return nil, errors.Wrap(err, "parse segment manifest reference")
	}
	manifestBasePath := oss.ResolveObjectKey(manifestRootPath, manifestRef.BasePath)
	manifestPath := path.Join(manifestBasePath, "_metadata", fmt.Sprintf("manifest-%d.avro", manifestRef.Ver))
	obj, err := manifestStore.Open(ctx, manifestPath)
	if err != nil {
		return nil, errors.Wrapf(err, "open manifest %s", manifestPath)
	}
	if closer, ok := obj.(interface{ Close() error }); ok {
		defer closer.Close()
	}
	m, err := parseManifest(obj)
	if err != nil {
		return nil, errors.Wrapf(err, "parse manifest %s", manifestPath)
	}

	fieldIDString := strconv.FormatInt(fieldID, 10)
	ranges := make([]externalVectorSegmentRange, 0)
	for _, columnGroup := range m.ColumnGroups {
		if !containsManifestColumn(columnGroup.Columns, fieldIDString, externalField) {
			continue
		}
		columnGroupFormat := strings.ToLower(strings.TrimSpace(columnGroup.Format))
		if columnGroupFormat == "" {
			columnGroupFormat = sourceFormat
		}
		if columnGroupFormat != sourceFormat {
			return nil, errors.Newf(
				"manifest column group format %s does not match external source format %s",
				columnGroupFormat,
				sourceFormat,
			)
		}
		for _, sourceFile := range columnGroup.Files {
			if sourceFile.StartIndex < 0 || sourceFile.EndIndex <= sourceFile.StartIndex {
				return nil, errors.Newf(
					"invalid source range [%d, %d) for %s",
					sourceFile.StartIndex,
					sourceFile.EndIndex,
					redactExternalVectorObjectKey(sourceFile.Path),
				)
			}
			var objectKey string
			if sourceFormat == "lance-table" {
				objectKey, err = resolveExternalManifestLancePath(location, sourceFile.Path)
			} else {
				objectKey, err = resolveExternalManifestObjectKey(location, manifestRef.BasePath, sourceFile.Path)
			}
			if err != nil {
				return nil, errors.New(sanitizeExternalVectorInspectionError(err, sourceFile.Path))
			}
			ranges = append(ranges, externalVectorSegmentRange{
				SegmentID:  segment.GetID(),
				ObjectKey:  objectKey,
				Format:     sourceFormat,
				StartIndex: sourceFile.StartIndex,
				EndIndex:   sourceFile.EndIndex,
			})
		}
	}
	if len(ranges) == 0 {
		return nil, errors.Newf("field %s(%d) is not present in the segment manifest", externalField, fieldID)
	}
	return ranges, nil
}

func containsManifestColumn(columns []string, candidates ...string) bool {
	for _, column := range columns {
		for _, candidate := range candidates {
			if column == candidate {
				return true
			}
		}
	}
	return false
}

func resolveExternalManifestLancePath(location externalSourceLocation, rawPath string) (string, error) {
	trimmed := strings.TrimSpace(rawPath)
	if trimmed == "" {
		return "", errors.New("manifest Lance path is empty")
	}
	if strings.Contains(trimmed, "://") {
		return trimmed, nil
	}
	parts := strings.SplitN(trimmed, "?", 2)
	objectPath := strings.ReplaceAll(parts[0], "ROOT_PATH", location.RootPath)
	objectPath = strings.TrimPrefix(path.Clean("/"+objectPath), "/")
	if location.RootPath != "" && objectPath != location.RootPath &&
		!strings.HasPrefix(objectPath, location.RootPath+"/") {
		objectPath = path.Join(location.RootPath, objectPath)
	}
	if location.Scheme == "" || location.Host == "" || location.Bucket == "" {
		return "", errors.New("cannot resolve relative Lance path without source scheme, endpoint, and bucket")
	}
	resolved := fmt.Sprintf("%s://%s/%s/%s", location.Scheme, location.Host, location.Bucket, objectPath)
	if len(parts) == 2 {
		resolved += "?" + parts[1]
	}
	return resolved, nil
}

func buildLancePropertyValues(location externalSourceLocation, spec externalSourceSpec, batchSize int64) (map[string]string, error) {
	provider := spec.CloudProvider
	if provider == "" {
		provider = inferLegacyCloudProviderFromScheme(location.Scheme)
	}
	if provider == "" {
		return nil, errors.Newf("cannot determine Lance cloud provider for scheme %s", location.Scheme)
	}
	if spec.Anonymous && provider != externalspec.CloudProviderGCP {
		return nil, errors.Newf("anonymous Lance access is not supported for cloud provider %s", provider)
	}

	useSSL := location.Scheme != externalspec.SchemeMinIO
	if spec.UseSSL != nil {
		useSSL = *spec.UseSSL
	}
	useVirtualHost := false
	if spec.UseVirtualHost != nil {
		useVirtualHost = *spec.UseVirtualHost
	}
	useIAM := spec.UseIAM
	if spec.RoleARN == "" && spec.AccessKeyID == "" && spec.AccessKeyValue == "" &&
		spec.GCPTargetServiceAccount == "" && spec.AzureCredentialEndpoint == "" && !spec.Anonymous {
		useIAM = true
	}

	externalPrefix := "extfs.birdwatcher."
	values := map[string]string{
		externalPrefix + "address":                    location.Host,
		externalPrefix + "bucket_name":                location.Bucket,
		externalPrefix + "root_path":                  location.RootPath,
		externalPrefix + "storage_type":               "remote",
		externalPrefix + "access_key_id":              spec.AccessKeyID,
		externalPrefix + "access_key_value":           spec.AccessKeyValue,
		externalPrefix + "iam_endpoint":               spec.IAMEndpoint,
		externalPrefix + "region":                     spec.Region,
		externalPrefix + "ssl_ca_cert":                spec.SSLCACert,
		externalPrefix + "role_arn":                   spec.RoleARN,
		externalPrefix + "session_name":               spec.RoleSessionName,
		externalPrefix + "external_id":                spec.ExternalID,
		externalPrefix + "gcp_target_service_account": spec.GCPTargetServiceAccount,
		externalPrefix + "azure_client_id":            spec.AzureClientID,
		externalPrefix + "azure_tenant_id":            spec.AzureTenantID,
		externalPrefix + "azure_credential_endpoint":  spec.AzureCredentialEndpoint,
		externalPrefix + "use_ssl":                    strconv.FormatBool(useSSL),
		externalPrefix + "use_iam":                    strconv.FormatBool(useIAM),
		externalPrefix + "use_virtual_host":           strconv.FormatBool(useVirtualHost),
		externalPrefix + "request_timeout_ms":         "10000",
		externalPrefix + "max_connections":            "100",
		"reader.logical_chunk_rows":                   strconv.FormatInt(batchSize, 10),
		"reader.record_batch_max_rows":                strconv.FormatInt(batchSize, 10),
		"reader.metadata_cache.enable":                "true",
	}
	// minio is a Milvus-only sentinel used to select Milvus-form URI parsing.
	// milvus-storage uses the AWS backend for S3-compatible stores and rejects
	// minio as a cloud_provider value.
	if provider != externalspec.CloudProviderMinIO {
		values[externalPrefix+"cloud_provider"] = provider
	}
	if spec.LoadFrequency > 0 {
		values[externalPrefix+"load_frequency"] = strconv.Itoa(spec.LoadFrequency)
	}
	if spec.Anonymous && provider == externalspec.CloudProviderGCP {
		values[externalPrefix+"gcp_native_without_auth"] = "true"
	}
	return values, nil
}

type externalVectorObjectResult struct {
	Ranges           []*ExternalVectorNullRange
	RowGroupsScanned int64
	RowGroupsSkipped int64
}

type externalVectorObjectJob struct {
	ObjectKey string
	Ranges    []externalVectorSegmentRange
}

type externalVectorScanInterval struct {
	StartIndex   int64
	EndIndex     int64
	RangeIndexes []int
}

func buildExternalVectorScanIntervals(ranges []externalVectorSegmentRange) ([]externalVectorScanInterval, error) {
	type indexedRange struct {
		index      int
		rangeValue externalVectorSegmentRange
	}
	indexed := make([]indexedRange, len(ranges))
	for i, sourceRange := range ranges {
		if sourceRange.StartIndex < 0 || sourceRange.EndIndex <= sourceRange.StartIndex {
			return nil, errors.Newf(
				"invalid external vector row range [%d, %d)", sourceRange.StartIndex, sourceRange.EndIndex)
		}
		indexed[i] = indexedRange{index: i, rangeValue: sourceRange}
	}
	sort.Slice(indexed, func(i, j int) bool {
		if indexed[i].rangeValue.StartIndex != indexed[j].rangeValue.StartIndex {
			return indexed[i].rangeValue.StartIndex < indexed[j].rangeValue.StartIndex
		}
		if indexed[i].rangeValue.EndIndex != indexed[j].rangeValue.EndIndex {
			return indexed[i].rangeValue.EndIndex < indexed[j].rangeValue.EndIndex
		}
		return indexed[i].index < indexed[j].index
	})

	intervals := make([]externalVectorScanInterval, 0, len(indexed))
	for _, item := range indexed {
		if len(intervals) == 0 || item.rangeValue.StartIndex > intervals[len(intervals)-1].EndIndex {
			intervals = append(intervals, externalVectorScanInterval{
				StartIndex:   item.rangeValue.StartIndex,
				EndIndex:     item.rangeValue.EndIndex,
				RangeIndexes: []int{item.index},
			})
			continue
		}

		current := &intervals[len(intervals)-1]
		current.EndIndex = max(current.EndIndex, item.rangeValue.EndIndex)
		current.RangeIndexes = append(current.RangeIndexes, item.index)
	}
	return intervals, nil
}

func scanExternalVectorNullRanges(ctx context.Context, store oss.ObjectStore, ranges []externalVectorSegmentRange, externalField string, fieldType schemapb.DataType, dim int64, workers int, batchSize int64, exact bool) <-chan externalVectorObjectResult {
	results := make(chan externalVectorObjectResult)
	objectRanges := make(map[string][]externalVectorSegmentRange)
	for _, sourceRange := range ranges {
		objectRanges[sourceRange.ObjectKey] = append(objectRanges[sourceRange.ObjectKey], sourceRange)
	}
	keys := make([]string, 0, len(objectRanges))
	for objectKey := range objectRanges {
		keys = append(keys, objectKey)
	}
	sort.Strings(keys)
	jobs := make(chan externalVectorObjectJob)
	workers = min(workers, len(keys))
	if workers == 0 {
		close(results)
		return results
	}

	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for job := range jobs {
				result := inspectExternalVectorNullObject(
					ctx, store, job, externalField, fieldType, dim, batchSize, exact)
				select {
				case results <- result:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, objectKey := range keys {
			job := externalVectorObjectJob{ObjectKey: objectKey, Ranges: objectRanges[objectKey]}
			select {
			case jobs <- job:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()
	return results
}

func inspectExternalVectorNullObject(ctx context.Context, store oss.ObjectStore, job externalVectorObjectJob, externalField string, fieldType schemapb.DataType, dim, batchSize int64, exact bool) externalVectorObjectResult {
	objectResult := externalVectorObjectResult{Ranges: make([]*ExternalVectorNullRange, len(job.Ranges))}
	for i, sourceRange := range job.Ranges {
		objectResult.Ranges[i] = &ExternalVectorNullRange{
			SegmentID:  sourceRange.SegmentID,
			ObjectKey:  redactExternalVectorObjectKey(sourceRange.ObjectKey),
			StartIndex: sourceRange.StartIndex,
			EndIndex:   sourceRange.EndIndex,
		}
	}
	failAll := func(err error) externalVectorObjectResult {
		message := sanitizeExternalVectorInspectionError(err, job.ObjectKey)
		for _, result := range objectResult.Ranges {
			if result.InspectionError == "" {
				result.InspectionError = message
			}
		}
		return objectResult
	}

	obj, err := store.Open(ctx, job.ObjectKey)
	if err != nil {
		return failAll(err)
	}
	if closer, ok := obj.(interface{ Close() error }); ok {
		defer closer.Close()
	}
	pqReader, err := file.NewParquetReader(obj)
	if err != nil {
		return failAll(err)
	}
	defer pqReader.Close()
	arrowReader, err := pqarrow.NewFileReader(
		pqReader,
		pqarrow.ArrowReadProperties{BatchSize: batchSize},
		memory.DefaultAllocator,
	)
	if err != nil {
		return failAll(err)
	}
	leafIndices, arrowType, err := projectedExternalFieldLeaves(pqReader, arrowReader, externalField)
	if err != nil {
		return failAll(err)
	}
	for _, result := range objectResult.Ranges {
		result.ArrowType = arrowType
	}

	rowGroupStarts := make([]int64, pqReader.NumRowGroups())
	rowGroupEnds := make([]int64, pqReader.NumRowGroups())
	var rowOffset int64
	for rowGroup := range pqReader.NumRowGroups() {
		rowGroupStarts[rowGroup] = rowOffset
		rowOffset += pqReader.MetaData().RowGroup(rowGroup).NumRows()
		rowGroupEnds[rowGroup] = rowOffset
	}
	if rowOffset != pqReader.NumRows() {
		return failAll(errors.Newf(
			"parquet row group total %d does not match file row count %d", rowOffset, pqReader.NumRows()))
	}
	type indexedRange struct {
		index int
		start int64
		end   int64
	}
	validRanges := make([]indexedRange, 0, len(job.Ranges))
	for i, sourceRange := range job.Ranges {
		if sourceRange.StartIndex < 0 || sourceRange.EndIndex <= sourceRange.StartIndex ||
			sourceRange.EndIndex > pqReader.NumRows() {
			objectResult.Ranges[i].InspectionError = fmt.Sprintf(
				"invalid parquet row range [%d, %d) for file with %d rows",
				sourceRange.StartIndex, sourceRange.EndIndex, pqReader.NumRows())
			continue
		}
		validRanges = append(validRanges, indexedRange{
			index: i,
			start: sourceRange.StartIndex,
			end:   sourceRange.EndIndex,
		})
	}
	sort.Slice(validRanges, func(i, j int) bool {
		if validRanges[i].start != validRanges[j].start {
			return validRanges[i].start < validRanges[j].start
		}
		if validRanges[i].end != validRanges[j].end {
			return validRanges[i].end < validRanges[j].end
		}
		return validRanges[i].index < validRanges[j].index
	})

	var activeRanges []indexedRange
	nextRange := 0
	for rowGroup := range pqReader.NumRowGroups() {
		rowGroupStart := rowGroupStarts[rowGroup]
		rowGroupEnd := rowGroupEnds[rowGroup]
		stillActive := activeRanges[:0]
		for _, sourceRange := range activeRanges {
			if sourceRange.end > rowGroupStart {
				stillActive = append(stillActive, sourceRange)
			}
		}
		activeRanges = stillActive
		for nextRange < len(validRanges) && validRanges[nextRange].start < rowGroupEnd {
			if validRanges[nextRange].end > rowGroupStart {
				activeRanges = append(activeRanges, validRanges[nextRange])
			}
			nextRange++
		}

		overlapping := make([]int, 0, len(activeRanges))
		for _, sourceRange := range activeRanges {
			overlapping = append(overlapping, sourceRange.index)
		}
		if len(overlapping) == 0 {
			continue
		}
		if !exact && parquetRowGroupHasNoNulls(pqReader, rowGroup, leafIndices) {
			objectResult.RowGroupsSkipped++
			for _, rangeIndex := range overlapping {
				sourceRange := job.Ranges[rangeIndex]
				rows := min(sourceRange.EndIndex, rowGroupEnds[rowGroup]) -
					max(sourceRange.StartIndex, rowGroupStarts[rowGroup])
				objectResult.Ranges[rangeIndex].Rows += rows
				objectResult.Ranges[rangeIndex].MetadataNoNullRows += rows
			}
			continue
		}

		objectResult.RowGroupsScanned++
		rr, err := arrowReader.GetRecordReader(ctx, leafIndices, []int{rowGroup})
		if err != nil {
			return failAll(err)
		}
		streamOffset := rowGroupStarts[rowGroup]
		for rr.Next() {
			record := rr.Record()
			if record.NumCols() != 1 {
				rr.Release()
				return failAll(errors.Newf(
					"projected field %s returned %d Arrow columns", externalField, record.NumCols()))
			}
			batchEnd := streamOffset + record.NumRows()
			for _, rangeIndex := range overlapping {
				sourceRange := job.Ranges[rangeIndex]
				overlapStart := max(sourceRange.StartIndex, streamOffset)
				overlapEnd := min(sourceRange.EndIndex, batchEnd)
				if overlapStart >= overlapEnd {
					continue
				}
				projected := array.NewSlice(
					record.Column(0), overlapStart-streamOffset, overlapEnd-streamOffset)
				counts, classifyErr := classifyExternalVectorArray(projected, fieldType, dim)
				projected.Release()
				if classifyErr != nil {
					rr.Release()
					return failAll(classifyErr)
				}
				addExternalVectorNullCounts(objectResult.Ranges[rangeIndex], counts)
			}
			streamOffset = batchEnd
		}
		readerErr := rr.Err()
		rr.Release()
		if readerErr != nil && !errors.Is(readerErr, io.EOF) {
			return failAll(readerErr)
		}
	}

	for i, sourceRange := range job.Ranges {
		result := objectResult.Ranges[i]
		if result.InspectionError == "" && result.Rows != sourceRange.EndIndex-sourceRange.StartIndex {
			result.InspectionError = fmt.Sprintf(
				"range row count mismatch: expected %d, scanned %d",
				sourceRange.EndIndex-sourceRange.StartIndex,
				result.Rows,
			)
		}
	}
	return objectResult
}

func redactExternalVectorObjectKey(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "<redacted>"
	}
	u.User = nil
	query := make(url.Values)
	for _, rawID := range u.Query()["fragment_id"] {
		if _, err := strconv.ParseUint(rawID, 10, 64); err == nil {
			query.Add("fragment_id", rawID)
		}
	}
	u.RawQuery = query.Encode()
	u.ForceQuery = false
	u.Fragment = ""
	return u.String()
}

func sanitizeExternalVectorInspectionError(err error, objectPaths ...string) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	for _, objectPath := range objectPaths {
		trimmed := strings.TrimSpace(objectPath)
		if trimmed == "" {
			continue
		}
		message = strings.ReplaceAll(message, trimmed, redactExternalVectorObjectKey(trimmed))
	}
	return message
}

func redactExternalVectorSource(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" ||
		externalspec.ValidateExternalSource(raw) != nil {
		return "<redacted>"
	}
	return raw
}

// parquetRowGroupHasNoNulls is deliberately conservative: missing or invalid
// statistics force a data read. A zero null count proves that neither parent
// nor child values are null, but it does not prove that vector lengths match.
func parquetRowGroupHasNoNulls(reader *file.Reader, rowGroup int, leafIndices []int) bool {
	metadata := reader.MetaData().RowGroup(rowGroup)
	for _, leafIndex := range leafIndices {
		columnChunk, err := metadata.ColumnChunk(leafIndex)
		if err != nil {
			return false
		}
		statistics, err := columnChunk.Statistics()
		if err != nil || statistics == nil || !statistics.HasNullCount() || statistics.NullCount() != 0 {
			return false
		}
	}
	return true
}

func addExternalVectorNullCounts(result *ExternalVectorNullRange, counts externalVectorNullCounts) {
	result.Rows += counts.Rows
	result.ValidRows += counts.ValidRows
	result.RowNullRows += counts.RowNullRows
	result.FullNullRows += counts.FullNullRows
	result.PartialNullRows += counts.PartialNullRows
	result.InvalidLengthRows += counts.InvalidLengthRows
}

func projectedExternalFieldLeaves(pqReader *file.Reader, reader *pqarrow.FileReader, externalField string) ([]int, string, error) {
	for i := range reader.Manifest.Fields {
		field := &reader.Manifest.Fields[i]
		if field.Field == nil || field.Field.Name != externalField {
			continue
		}
		leaves := make([]int, 0, 1)
		parquetSchema := pqReader.MetaData().Schema
		for columnIndex := 0; columnIndex < parquetSchema.NumColumns(); columnIndex++ {
			columnPath := parquetSchema.Column(columnIndex).ColumnPath()
			if len(columnPath) > 0 && columnPath[0] == externalField {
				leaves = append(leaves, columnIndex)
			}
		}
		if len(leaves) == 0 {
			return nil, "", errors.Newf("external field %s has no parquet leaf columns", externalField)
		}
		return leaves, field.Field.Type.String(), nil
	}
	return nil, "", errors.Newf("external field %s not found in parquet schema", externalField)
}

type externalVectorNullCounts struct {
	Rows              int64
	ValidRows         int64
	RowNullRows       int64
	FullNullRows      int64
	PartialNullRows   int64
	InvalidLengthRows int64
}

func classifyExternalVectorArray(values arrow.Array, fieldType schemapb.DataType, dim int64) (externalVectorNullCounts, error) {
	var counts externalVectorNullCounts
	counts.Rows = int64(values.Len())

	if list, ok := values.(array.ListLike); ok {
		return classifyExternalVectorList(list, fieldType, dim)
	}

	switch typed := values.(type) {
	case *array.FixedSizeBinary:
		expectedWidth, err := externalVectorByteWidth(fieldType, dim)
		if err != nil {
			return counts, err
		}
		actualWidth := int64(typed.DataType().(*arrow.FixedSizeBinaryType).ByteWidth)
		for i := 0; i < typed.Len(); i++ {
			if typed.IsNull(i) {
				counts.RowNullRows++
			} else if actualWidth != expectedWidth {
				counts.InvalidLengthRows++
			} else {
				counts.ValidRows++
			}
		}
		return counts, nil
	case *array.Binary:
		return classifyVariableBinaryVector(typed.Len(), typed.IsNull, func(i int) int64 {
			return int64(typed.ValueLen(i))
		}, fieldType, dim)
	case *array.LargeBinary:
		return classifyVariableBinaryVector(typed.Len(), typed.IsNull, func(i int) int64 {
			return int64(typed.ValueLen(i))
		}, fieldType, dim)
	case *array.BinaryView:
		return classifyVariableBinaryVector(typed.Len(), typed.IsNull, func(i int) int64 {
			return int64(typed.ValueLen(i))
		}, fieldType, dim)
	default:
		return counts, errors.Newf(
			"unsupported Arrow type %s for external vector null inspection", values.DataType())
	}
}

func classifyExternalVectorList(values array.ListLike, fieldType schemapb.DataType, dim int64) (externalVectorNullCounts, error) {
	counts := externalVectorNullCounts{Rows: int64(values.Len())}
	child := values.ListValues()
	expectedLength, err := expectedExternalVectorListLength(fieldType, dim, child.DataType())
	if err != nil {
		return counts, err
	}
	for i := 0; i < values.Len(); i++ {
		if values.IsNull(i) {
			counts.RowNullRows++
			continue
		}
		start, end := values.ValueOffsets(i)
		length := end - start
		if length != expectedLength {
			counts.InvalidLengthRows++
			continue
		}
		valid := countValidChildValues(child, start, end)
		switch {
		case valid == length:
			counts.ValidRows++
		case valid == 0:
			counts.FullNullRows++
		default:
			counts.PartialNullRows++
		}
	}
	return counts, nil
}

func classifyVariableBinaryVector(length int, isNull func(int) bool, valueLength func(int) int64, fieldType schemapb.DataType, dim int64) (externalVectorNullCounts, error) {
	counts := externalVectorNullCounts{Rows: int64(length)}
	expectedWidth, err := externalVectorByteWidth(fieldType, dim)
	if err != nil {
		return counts, err
	}
	for i := 0; i < length; i++ {
		if isNull(i) {
			counts.RowNullRows++
		} else if valueLength(i) != expectedWidth {
			counts.InvalidLengthRows++
		} else {
			counts.ValidRows++
		}
	}
	return counts, nil
}

func countValidChildValues(values arrow.Array, start, end int64) int64 {
	length := end - start
	if length == 0 || values.NullN() == 0 {
		return length
	}
	return int64(bitutil.CountSetBits(
		values.NullBitmapBytes(),
		values.Data().Offset()+int(start),
		int(length),
	))
}

func expectedExternalVectorListLength(fieldType schemapb.DataType, dim int64, childType arrow.DataType) (int64, error) {
	if err := validateExternalVectorElementType(fieldType, childType); err != nil {
		return 0, err
	}
	if fieldType == schemapb.DataType_BinaryVector ||
		fieldType == schemapb.DataType_BFloat16Vector ||
		childType.ID() == arrow.UINT8 {
		return externalVectorByteWidth(fieldType, dim)
	}
	return dim, nil
}

func validateExternalVectorElementType(fieldType schemapb.DataType, childType arrow.DataType) error {
	actual := childType.ID()
	var expected arrow.Type
	switch fieldType {
	case schemapb.DataType_FloatVector:
		expected = arrow.FLOAT32
	case schemapb.DataType_Int8Vector:
		expected = arrow.INT8
	case schemapb.DataType_Float16Vector:
		expected = arrow.FLOAT16
	case schemapb.DataType_BinaryVector, schemapb.DataType_BFloat16Vector:
		expected = arrow.UINT8
	default:
		return errors.Newf("unsupported external vector type %s", fieldType)
	}
	if actual != expected && actual != arrow.UINT8 {
		return errors.Newf(
			"vector list element type mismatch: expected %s or raw uint8, actual %s",
			expected, childType)
	}
	return nil
}

func externalVectorByteWidth(fieldType schemapb.DataType, dim int64) (int64, error) {
	switch fieldType {
	case schemapb.DataType_FloatVector:
		return dim * 4, nil
	case schemapb.DataType_BinaryVector:
		if dim%8 != 0 {
			return 0, errors.Newf("binary vector dimension %d is not divisible by 8", dim)
		}
		return dim / 8, nil
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		return dim * 2, nil
	case schemapb.DataType_Int8Vector:
		return dim, nil
	default:
		return 0, errors.Newf("unsupported external vector type %s", fieldType)
	}
}
