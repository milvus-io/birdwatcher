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
				result := inspectLanceVectorNullObject(
					ctx, job, externalField, fieldType, dim, batchSize, location, spec)
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
			select {
			case jobs <- externalVectorObjectJob{ObjectKey: objectKey, Ranges: objectRanges[objectKey]}:
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

func inspectLanceVectorNullObject(
	ctx context.Context,
	job externalVectorObjectJob,
	externalField string,
	fieldType schemapb.DataType,
	dim int64,
	batchSize int64,
	location externalSourceLocation,
	spec externalSourceSpec,
) externalVectorObjectResult {
	var lastErr error
	for _, dataType := range lanceVectorCandidateTypes(fieldType, dim) {
		result, err := inspectLanceVectorNullObjectWithType(
			ctx, job, externalField, fieldType, dim, batchSize, location, spec, dataType)
		if err == nil {
			return result
		}
		lastErr = err
		if !isLanceSchemaMismatch(err) {
			break
		}
	}
	if lastErr == nil {
		lastErr = errors.Newf("no Arrow schema candidate for vector type %s", fieldType)
	}
	result := externalVectorObjectResult{Ranges: newExternalVectorNullResults(job.Ranges)}
	message := sanitizeExternalVectorInspectionError(lastErr, job.ObjectKey)
	for _, item := range result.Ranges {
		item.InspectionError = message
	}
	return result
}

func inspectLanceVectorNullObjectWithType(
	ctx context.Context,
	job externalVectorObjectJob,
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
		return result, errors.New("Lance scan job does not contain ranges")
	}
	intervals, err := buildExternalVectorScanIntervals(job.Ranges)
	if err != nil {
		return result, err
	}

	schema := arrow.NewSchema([]arrow.Field{{Name: externalField, Type: dataType, Nullable: true}}, nil)
	for _, item := range result.Ranges {
		item.ArrowType = dataType.String()
	}

	for _, interval := range intervals {
		err := func() error {
			stream, closeStream, err := openLanceVectorStream(
				job.ObjectKey, interval.StartIndex, interval.EndIndex,
				externalField, schema, batchSize, location, spec)
			if err != nil {
				return err
			}
			defer closeStream()

			streamOffset := interval.StartIndex
			for {
				if err := ctx.Err(); err != nil {
					return err
				}
				var cArray cdata.CArrowArray
				if errno := C.bw_lance_stream_next(
					(*C.struct_ArrowArrayStream)(unsafe.Pointer(stream)),
					(*C.struct_ArrowArray)(unsafe.Pointer(&cArray)),
				); errno != 0 {
					return errors.Newf(
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
					return errors.Wrap(err, "import Lance record batch")
				}
				if record.NumCols() != 1 {
					record.Release()
					return errors.Newf(
						"projected Lance field %s returned %d Arrow columns", externalField, record.NumCols())
				}
				batchEnd := streamOffset + record.NumRows()
				for _, rangeIndex := range interval.RangeIndexes {
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
						record.Release()
						return classifyErr
					}
					addExternalVectorNullCounts(result.Ranges[rangeIndex], counts)
				}
				record.Release()
				streamOffset = batchEnd
			}
			if streamOffset != interval.EndIndex {
				return errors.Newf(
					"Lance row count mismatch: expected stream [%d, %d), got end %d",
					interval.StartIndex, interval.EndIndex, streamOffset)
			}
			return nil
		}()
		if err != nil {
			return result, err
		}
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

func openLanceVectorStream(
	objectPath string,
	start, end int64,
	externalField string,
	schema *arrow.Schema,
	batchSize int64,
	location externalSourceLocation,
	spec externalSourceSpec,
) (*cdata.CArrowArrayStream, func(), error) {
	properties, err := newLanceProperties(location, spec, batchSize)
	if err != nil {
		return nil, nil, err
	}
	defer C.loon_properties_free(properties)

	columns, freeColumns := newCStringArray([]string{externalField})
	defer freeColumns()
	paths, freePaths := newCStringArray([]string{objectPath})
	defer freePaths()
	format := C.CString("lance-table")
	defer C.free(unsafe.Pointer(format))
	cStart := C.int64_t(start)
	cEnd := C.int64_t(end)
	var columnGroups *C.LoonColumnGroups
	ffiResult := C.loon_column_groups_create(
		(**C.char)(unsafe.Pointer(columns)),
		1,
		format,
		(**C.char)(unsafe.Pointer(paths)),
		&cStart,
		&cEnd,
		1,
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
		return nil, nil, errors.Wrap(err, "open Lance reader")
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
	if semantic.ID() != arrow.UINT8 {
		byteWidth, widthErr := externalVectorByteWidth(fieldType, dim)
		if widthErr == nil && byteWidth <= int64(^uint32(0)>>1) {
			candidates = append(candidates,
				arrow.FixedSizeListOf(int32(byteWidth), arrow.PrimitiveTypes.Uint8),
				arrow.ListOf(arrow.PrimitiveTypes.Uint8),
				arrow.LargeListOf(arrow.PrimitiveTypes.Uint8),
			)
		}
	}
	candidates = append(candidates, arrow.BinaryTypes.Binary)
	return candidates
}

func isLanceSchemaMismatch(err error) bool {
	message := strings.ToLower(err.Error())
	for _, marker := range []string{"schema", "type", "field", "cast", "column"} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
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
