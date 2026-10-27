package tasks

import (
	"fmt"
	"math"
	"strings"
	"sync"

	"go.uber.org/atomic"

	"github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// CountDistinctTask counts exact non-null field values across all workers and
// segments. The set lives for one command and uses O(distinct values) memory.
type CountDistinctTask struct {
	baseScanTask
	fieldID   int64
	fieldName string
	dataType  schemapb.DataType
	key       func(any) (any, bool)
	seen      sync.Map
	matched   atomic.Int64
	nulls     atomic.Int64
}

func NewCountDistinctTask(field *schemapb.FieldSchema) (*CountDistinctTask, error) {
	if field == nil {
		return nil, fmt.Errorf("count-distinct requires a field schema")
	}
	t := &CountDistinctTask{fieldID: field.GetFieldID(), fieldName: field.GetName(), dataType: field.GetDataType()}
	switch field.GetDataType() {
	case schemapb.DataType_Bool:
		t.key = distinctScalarKey[bool]
	case schemapb.DataType_Int8:
		t.key = distinctScalarKey[int8]
	case schemapb.DataType_Int16:
		t.key = distinctScalarKey[int16]
	case schemapb.DataType_Int32:
		t.key = distinctScalarKey[int32]
	case schemapb.DataType_Int64:
		t.key = distinctScalarKey[int64]
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		t.key = distinctScalarKey[string]
	case schemapb.DataType_Float:
		t.key = func(value any) (any, bool) {
			v, ok := value.(float32)
			if !ok {
				return nil, false
			}
			if v == 0 {
				return uint32(0), true
			}
			if math.IsNaN(float64(v)) {
				return uint32(0x7fc00000), true
			}
			return math.Float32bits(v), true
		}
	case schemapb.DataType_Double:
		t.key = func(value any) (any, bool) {
			v, ok := value.(float64)
			if !ok {
				return nil, false
			}
			if v == 0 {
				return uint64(0), true
			}
			if math.IsNaN(v) {
				return uint64(0x7ff8000000000000), true
			}
			return math.Float64bits(v), true
		}
	default:
		return nil, fmt.Errorf("count-distinct does not support field %q (id %d, type %s); supported types: Bool, Int8/16/32/64, Float, Double, String, VarChar", t.fieldName, t.fieldID, t.dataType)
	}
	return t, nil
}

func distinctScalarKey[T comparable](value any) (any, bool) {
	v, ok := value.(T)
	return v, ok
}

func (t *CountDistinctTask) Scan(_ common.PrimaryKey, _ *common.BatchInfo, _ int, values map[int64]any) error {
	value, present := values[t.fieldID]
	if !present {
		return fmt.Errorf("count-distinct field %q (id %d) is missing from the scanned row", t.fieldName, t.fieldID)
	}
	if value == nil {
		t.matched.Inc()
		t.nulls.Inc()
		return nil
	}
	key, ok := t.key(value)
	if !ok {
		return fmt.Errorf("count-distinct field %q (id %d) expects %s, got %T", t.fieldName, t.fieldID, t.dataType, value)
	}
	if _, exists := t.seen.Load(key); !exists {
		// Arrow strings can refer to a much larger batch buffer. Keep only the
		// unique value's bytes, without copying strings already in the set.
		if s, ok := key.(string); ok {
			key = strings.Clone(s)
		}
		if _, loaded := t.seen.LoadOrStore(key, struct{}{}); !loaded {
			t.counter.Inc()
		}
	}
	t.matched.Inc()
	return nil
}

func (t *CountDistinctTask) Summary() {
	fmt.Printf("Total %d distinct non-null values found for field %q (id %d)\n", t.counter.Load(), t.fieldName, t.fieldID)
	fmt.Printf("Matched rows: %d, null rows skipped: %d\n", t.matched.Load(), t.nulls.Load())
}
