//go:build LANCE && cgo

package states

/*
#cgo pkg-config: milvus-storage

#include <stdlib.h>
#include "milvus-storage/ffi_c.h"

static int bw_lance_stream_next(struct ArrowArrayStream* stream, struct ArrowArray* out) {
    return stream->get_next(stream, out);
}

static const char* bw_lance_stream_error(struct ArrowArrayStream* stream) {
    return stream->get_last_error == NULL ? NULL : stream->get_last_error(stream);
}

static int bw_lance_array_released(struct ArrowArray* array) {
    return array->release == NULL;
}

static void bw_lance_stream_release(struct ArrowArrayStream* stream) {
    if (stream->release != NULL) {
        stream->release(stream);
    }
}
*/
import "C"

import (
	"context"
	"sort"
	"strings"
	"sync"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/cdata"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func ensureLanceVectorScannerAvailable() error {
	return nil
}

type lanceSchemaCandidateError struct {
	err error
}

func (e *lanceSchemaCandidateError) Error() string {
	return e.err.Error()
}

func (e *lanceSchemaCandidateError) Unwrap() error {
	return e.err
}

func scanLanceVectorNullRanges(
	ctx context.Context,
	ranges []externalVectorSegmentRange,
	externalField string,
	fieldType schemapb.DataType,
	dim int64,
	workers int,
	batchSize int64,
	location externalSourceLocation,
	spec externalSourceSpec,
) <-chan externalVectorObjectResult {
	results := make(chan externalVectorObjectResult)
	datasetRanges := make(map[string][]externalVectorSegmentRange)
	for _, sourceRange := range ranges {
		datasetKey := lanceDatasetKey(sourceRange.ObjectKey)
		datasetRanges[datasetKey] = append(datasetRanges[datasetKey], sourceRange)
	}
	keys := make([]string, 0, len(datasetRanges))
	for datasetKey := range datasetRanges {
		keys = append(keys, datasetKey)
	}
	sort.Strings(keys)
	jobs := make(chan lanceVectorDatasetJob)
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
				result := inspectLanceVectorNullDataset(
					ctx, job, externalField, fieldType, dim, batchSize, location, spec)
				for _, objectResult := range splitLanceVectorNullResult(job, result) {
					select {
					case results <- objectResult:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, datasetKey := range keys {
			select {
			case jobs <- lanceVectorDatasetJob{DatasetKey: datasetKey, Ranges: datasetRanges[datasetKey]}:
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

type lanceVectorDatasetJob struct {
	DatasetKey string
	Ranges     []externalVectorSegmentRange
}

type lanceVectorScanInterval struct {
	ObjectKey    string
	StartIndex   int64
	EndIndex     int64
	RangeIndexes []int
}

func lanceDatasetKey(objectKey string) string {
	const fragmentDelimiter = "?fragment_id="
	if index := strings.Index(objectKey, fragmentDelimiter); index >= 0 {
		return objectKey[:index]
	}
	return objectKey
}

func inspectLanceVectorNullDataset(
	ctx context.Context,
	job lanceVectorDatasetJob,
	externalField string,
	fieldType schemapb.DataType,
	dim int64,
	batchSize int64,
	location externalSourceLocation,
	spec externalSourceSpec,
) externalVectorObjectResult {
	var lastErr error
	for _, dataType := range lanceVectorCandidateTypes(fieldType, dim) {
		result, err := inspectLanceVectorNullDatasetWithType(
			ctx, job, externalField, fieldType, dim, batchSize, location, spec, dataType)
		if err == nil {
			return result
		}
		lastErr = err
		if !shouldTryNextLanceVectorType(err) {
			break
		}
	}
	if lastErr == nil {
		lastErr = errors.Newf("no Arrow schema candidate for vector type %s", fieldType)
	}
	result := externalVectorObjectResult{Ranges: newExternalVectorNullResults(job.Ranges)}
	message := sanitizeExternalVectorInspectionError(lastErr, job.DatasetKey)
	for _, item := range result.Ranges {
		item.InspectionError = message
	}
	return result
}

func inspectLanceVectorNullDatasetWithType(
	ctx context.Context,
	job lanceVectorDatasetJob,
	externalField string,
	fieldType schemapb.DataType,
	dim int64,
	batchSize int64,
	location externalSourceLocation,
	spec externalSourceSpec,
	dataType arrow.DataType,
) (externalVectorObjectResult, error) {
	result := externalVectorObjectResult{Ranges: newExternalVectorNullResults(job.Ranges)}
	if len(job.Ranges) == 0 {
		return result, errors.New("Lance dataset scan job does not contain ranges")
	}
	intervals, err := buildLanceVectorScanIntervals(job.Ranges)
	if err != nil {
		return result, err
	}

	schema := arrow.NewSchema([]arrow.Field{{Name: externalField, Type: dataType, Nullable: true}}, nil)
	for _, item := range result.Ranges {
		item.ArrowType = dataType.String()
	}

	stream, closeStream, err := openLanceVectorStream(
		intervals, externalField, schema, batchSize, location, spec)
	if err != nil {
		return result, err
	}
	defer closeStream()

	candidateValidated := false
	intervalIndex := 0
	var intervalRowsRead int64
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		var cArray cdata.CArrowArray
		if errno := C.bw_lance_stream_next(
			(*C.struct_ArrowArrayStream)(unsafe.Pointer(stream)),
			(*C.struct_ArrowArray)(unsafe.Pointer(&cArray)),
		); errno != 0 {
			return result, errors.Newf(
				"read Lance record batch: errno=%d message=%s",
				int(errno),
				C.GoString(C.bw_lance_stream_error((*C.struct_ArrowArrayStream)(unsafe.Pointer(stream)))),
			)
		}
		if C.bw_lance_array_released((*C.struct_ArrowArray)(unsafe.Pointer(&cArray))) != 0 {
			break
		}
		record, err := cdata.ImportCRecordBatchWithSchema(&cArray, schema)
		if err != nil {
			cdata.ReleaseCArrowArray(&cArray)
			importErr := errors.Wrap(err, "import Lance record batch")
			if !candidateValidated {
				return result, markLanceSchemaCandidateError(importErr)
			}
			return result, importErr
		}
		candidateValidated = true
		if record.NumCols() != 1 {
			record.Release()
			return result, errors.Newf(
				"projected Lance field %s returned %d Arrow columns", externalField, record.NumCols())
		}
		if err := classifyLanceRecordBatch(
			record, intervals, &intervalIndex, &intervalRowsRead,
			job.Ranges, result.Ranges, fieldType, dim,
		); err != nil {
			record.Release()
			return result, err
		}
		record.Release()
	}
	if intervalIndex != len(intervals) || intervalRowsRead != 0 {
		return result, errors.Newf(
			"Lance row count mismatch: completed %d of %d scan intervals",
			intervalIndex, len(intervals))
	}
	for i, sourceRange := range job.Ranges {
		if result.Ranges[i].Rows != sourceRange.EndIndex-sourceRange.StartIndex {
			return result, errors.Newf(
				"Lance range row count mismatch: expected %d, scanned %d",
				sourceRange.EndIndex-sourceRange.StartIndex,
				result.Ranges[i].Rows,
			)
		}
	}
	return result, nil
}

func buildLanceVectorScanIntervals(ranges []externalVectorSegmentRange) ([]lanceVectorScanInterval, error) {
	objectRangeIndexes := make(map[string][]int)
	for index, sourceRange := range ranges {
		objectRangeIndexes[sourceRange.ObjectKey] = append(objectRangeIndexes[sourceRange.ObjectKey], index)
	}
	objectKeys := make([]string, 0, len(objectRangeIndexes))
	for objectKey := range objectRangeIndexes {
		objectKeys = append(objectKeys, objectKey)
	}
	sort.Strings(objectKeys)

	var scans []lanceVectorScanInterval
	for _, objectKey := range objectKeys {
		globalIndexes := objectRangeIndexes[objectKey]
		objectRanges := make([]externalVectorSegmentRange, len(globalIndexes))
		for index, globalIndex := range globalIndexes {
			objectRanges[index] = ranges[globalIndex]
		}
		intervals, err := buildExternalVectorScanIntervals(objectRanges)
		if err != nil {
			return nil, err
		}
		for _, interval := range intervals {
			rangeIndexes := make([]int, len(interval.RangeIndexes))
			for index, localIndex := range interval.RangeIndexes {
				rangeIndexes[index] = globalIndexes[localIndex]
			}
			scans = append(scans, lanceVectorScanInterval{
				ObjectKey:    objectKey,
				StartIndex:   interval.StartIndex,
				EndIndex:     interval.EndIndex,
				RangeIndexes: rangeIndexes,
			})
		}
	}
	return scans, nil
}

func classifyLanceRecordBatch(
	record arrow.Record,
	intervals []lanceVectorScanInterval,
	intervalIndex *int,
	intervalRowsRead *int64,
	sourceRanges []externalVectorSegmentRange,
	results []*ExternalVectorNullRange,
	fieldType schemapb.DataType,
	dim int64,
) error {
	var recordOffset int64
	for recordOffset < record.NumRows() {
		if *intervalIndex >= len(intervals) {
			return errors.Newf("Lance stream returned %d unexpected trailing rows", record.NumRows()-recordOffset)
		}
		interval := intervals[*intervalIndex]
		intervalRows := interval.EndIndex - interval.StartIndex
		remainingRows := intervalRows - *intervalRowsRead
		batchRows := min(record.NumRows()-recordOffset, remainingRows)
		sourceStart := interval.StartIndex + *intervalRowsRead
		sourceEnd := sourceStart + batchRows
		for _, rangeIndex := range interval.RangeIndexes {
			sourceRange := sourceRanges[rangeIndex]
			overlapStart := max(sourceRange.StartIndex, sourceStart)
			overlapEnd := min(sourceRange.EndIndex, sourceEnd)
			if overlapStart >= overlapEnd {
				continue
			}
			projectedStart := recordOffset + overlapStart - sourceStart
			projectedEnd := recordOffset + overlapEnd - sourceStart
			projected := array.NewSlice(record.Column(0), projectedStart, projectedEnd)
			counts, err := classifyExternalVectorArray(projected, fieldType, dim)
			projected.Release()
			if err != nil {
				return err
			}
			addExternalVectorNullCounts(results[rangeIndex], counts)
		}
		recordOffset += batchRows
		*intervalRowsRead += batchRows
		if *intervalRowsRead == intervalRows {
			(*intervalIndex)++
			*intervalRowsRead = 0
		}
	}
	return nil
}

func splitLanceVectorNullResult(job lanceVectorDatasetJob, result externalVectorObjectResult) []externalVectorObjectResult {
	objectRangeIndexes := make(map[string][]int)
	for index, sourceRange := range job.Ranges {
		objectRangeIndexes[sourceRange.ObjectKey] = append(objectRangeIndexes[sourceRange.ObjectKey], index)
	}
	objectKeys := make([]string, 0, len(objectRangeIndexes))
	for objectKey := range objectRangeIndexes {
		objectKeys = append(objectKeys, objectKey)
	}
	sort.Strings(objectKeys)

	objectResults := make([]externalVectorObjectResult, 0, len(objectKeys))
	for _, objectKey := range objectKeys {
		ranges := make([]*ExternalVectorNullRange, 0, len(objectRangeIndexes[objectKey]))
		for _, index := range objectRangeIndexes[objectKey] {
			ranges = append(ranges, result.Ranges[index])
		}
		objectResults = append(objectResults, externalVectorObjectResult{Ranges: ranges})
	}
	return objectResults
}

func openLanceVectorStream(
	intervals []lanceVectorScanInterval,
	externalField string,
	schema *arrow.Schema,
	batchSize int64,
	location externalSourceLocation,
	spec externalSourceSpec,
) (*cdata.CArrowArrayStream, func(), error) {
	if len(intervals) == 0 {
		return nil, nil, errors.New("Lance reader requires at least one scan interval")
	}
	properties, err := newLanceProperties(location, spec, batchSize)
	if err != nil {
		return nil, nil, err
	}
	defer C.loon_properties_free(properties)

	columns, freeColumns := newCStringArray([]string{externalField})
	defer freeColumns()
	pathsToRead := make([]string, len(intervals))
	starts := make([]C.int64_t, len(intervals))
	ends := make([]C.int64_t, len(intervals))
	for index, interval := range intervals {
		pathsToRead[index] = interval.ObjectKey
		starts[index] = C.int64_t(interval.StartIndex)
		ends[index] = C.int64_t(interval.EndIndex)
	}
	paths, freePaths := newCStringArray(pathsToRead)
	defer freePaths()
	format := C.CString("lance-table")
	defer C.free(unsafe.Pointer(format))
	var columnGroups *C.LoonColumnGroups
	ffiResult := C.loon_column_groups_create(
		(**C.char)(unsafe.Pointer(columns)),
		1,
		format,
		(**C.char)(unsafe.Pointer(paths)),
		(*C.int64_t)(unsafe.Pointer(&starts[0])),
		(*C.int64_t)(unsafe.Pointer(&ends[0])),
		C.size_t(len(intervals)),
		&columnGroups,
	)
	if err := consumeLanceFFIResult(&ffiResult); err != nil {
		return nil, nil, errors.Wrap(err, "create Lance column groups")
	}
	defer C.loon_column_groups_destroy(columnGroups)

	var cSchema cdata.CArrowSchema
	cdata.ExportArrowSchema(schema, &cSchema)
	defer cdata.ReleaseCArrowSchema(&cSchema)
	neededColumns, freeNeededColumns := newCStringArray([]string{externalField})
	defer freeNeededColumns()
	var reader C.LoonReaderHandle
	ffiResult = C.loon_reader_new(
		columnGroups,
		(*C.struct_ArrowSchema)(unsafe.Pointer(&cSchema)),
		(**C.char)(unsafe.Pointer(neededColumns)),
		1,
		properties,
		&reader,
	)
	if err := consumeLanceFFIResult(&ffiResult); err != nil {
		return nil, nil, markLanceSchemaCandidateError(errors.Wrap(err, "open Lance reader"))
	}
	closeReader := true
	defer func() {
		if closeReader {
			C.loon_reader_destroy(reader)
		}
	}()

	var stream cdata.CArrowArrayStream
	ffiResult = C.loon_get_record_batch_reader(
		reader,
		nil,
		(*C.struct_ArrowArrayStream)(unsafe.Pointer(&stream)),
	)
	if err := consumeLanceFFIResult(&ffiResult); err != nil {
		return nil, nil, errors.Wrap(err, "open Lance record stream")
	}
	closeReader = false
	cleanup := func() {
		C.bw_lance_stream_release((*C.struct_ArrowArrayStream)(unsafe.Pointer(&stream)))
		C.loon_reader_destroy(reader)
	}
	return &stream, cleanup, nil
}

func newLanceProperties(location externalSourceLocation, spec externalSourceSpec, batchSize int64) (*C.LoonProperties, error) {
	values, err := buildLancePropertyValues(location, spec, batchSize)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(values))
	for key, value := range values {
		if value != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	propertyValues := make([]string, len(keys))
	for i, key := range keys {
		propertyValues[i] = values[key]
	}
	cKeys, freeKeys := newCStringArray(keys)
	defer freeKeys()
	cValues, freeValues := newCStringArray(propertyValues)
	defer freeValues()
	properties := &C.LoonProperties{}
	ffiResult := C.loon_properties_create(
		(**C.char)(unsafe.Pointer(cKeys)),
		(**C.char)(unsafe.Pointer(cValues)),
		C.size_t(len(keys)),
		properties,
	)
	if err := consumeLanceFFIResult(&ffiResult); err != nil {
		return nil, errors.Wrap(err, "create Lance storage properties")
	}
	return properties, nil
}

func newCStringArray(values []string) (**C.char, func()) {
	if len(values) == 0 {
		return nil, func() {}
	}
	memory := C.malloc(C.size_t(len(values)) * C.size_t(unsafe.Sizeof(uintptr(0))))
	items := unsafe.Slice((**C.char)(memory), len(values))
	for i, value := range values {
		items[i] = C.CString(value)
	}
	return (**C.char)(memory), func() {
		for _, item := range items {
			C.free(unsafe.Pointer(item))
		}
		C.free(memory)
	}
}

func consumeLanceFFIResult(result *C.LoonFFIResult) error {
	if result == nil {
		return errors.New("milvus-storage returned a nil result")
	}
	defer C.loon_ffi_free_result(result)
	if C.loon_ffi_is_success(result) != 0 {
		return nil
	}
	return errors.Newf(
		"milvus-storage error %d: %s",
		int(result.err_code),
		C.GoString(C.loon_ffi_get_errmsg(result)),
	)
}

func lanceVectorCandidateTypes(fieldType schemapb.DataType, dim int64) []arrow.DataType {
	var semantic arrow.DataType
	switch fieldType {
	case schemapb.DataType_FloatVector:
		semantic = arrow.PrimitiveTypes.Float32
	case schemapb.DataType_Int8Vector:
		semantic = arrow.PrimitiveTypes.Int8
	case schemapb.DataType_Float16Vector:
		semantic = arrow.FixedWidthTypes.Float16
	case schemapb.DataType_BFloat16Vector, schemapb.DataType_BinaryVector:
		semantic = arrow.PrimitiveTypes.Uint8
	default:
		return nil
	}
	expectedLength, err := expectedExternalVectorListLength(fieldType, dim, semantic)
	if err != nil || expectedLength > int64(^uint32(0)>>1) {
		return nil
	}
	candidates := []arrow.DataType{
		arrow.FixedSizeListOf(int32(expectedLength), semantic),
		arrow.ListOf(semantic),
		arrow.LargeListOf(semantic),
	}
	byteWidth, widthErr := externalVectorByteWidth(fieldType, dim)
	if semantic.ID() != arrow.UINT8 {
		if widthErr == nil && byteWidth <= int64(^uint32(0)>>1) {
			candidates = append(candidates,
				arrow.FixedSizeListOf(int32(byteWidth), arrow.PrimitiveTypes.Uint8),
				arrow.ListOf(arrow.PrimitiveTypes.Uint8),
				arrow.LargeListOf(arrow.PrimitiveTypes.Uint8),
			)
		}
	}
	if widthErr == nil && byteWidth <= int64(^uint32(0)>>1) {
		candidates = append(candidates, &arrow.FixedSizeBinaryType{ByteWidth: int(byteWidth)})
	}
	candidates = append(candidates,
		arrow.BinaryTypes.Binary,
		arrow.BinaryTypes.LargeBinary,
		arrow.BinaryTypes.BinaryView,
	)
	return candidates
}

func markLanceSchemaCandidateError(err error) error {
	message := strings.ToLower(err.Error())
	for _, marker := range []string{"schema", "type", "field", "cast", "column"} {
		if strings.Contains(message, marker) {
			return &lanceSchemaCandidateError{err: err}
		}
	}
	return err
}

func shouldTryNextLanceVectorType(err error) bool {
	var candidateError *lanceSchemaCandidateError
	return errors.As(err, &candidateError)
}

func newExternalVectorNullResults(ranges []externalVectorSegmentRange) []*ExternalVectorNullRange {
	results := make([]*ExternalVectorNullRange, len(ranges))
	for i, sourceRange := range ranges {
		results[i] = &ExternalVectorNullRange{
			SegmentID:  sourceRange.SegmentID,
			ObjectKey:  redactExternalVectorObjectKey(sourceRange.ObjectKey),
			StartIndex: sourceRange.StartIndex,
			EndIndex:   sourceRange.EndIndex,
		}
	}
	return results
}
