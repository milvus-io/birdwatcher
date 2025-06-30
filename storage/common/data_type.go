package common

import (
	"encoding/binary"
	"math"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type SerdeEntry struct {
	// ArrowType returns the arrow type for the given dimension
	ArrowType func(int) arrow.DataType
	// Deserialize deserializes the i-th element in the array, returns the value and ok.
	//	null is deserialized to nil without checking the type nullability.
	Deserialize func(arrow.Array, int) (any, bool)
	// Serialize serializes the value to the builder, returns ok.
	// 	nil is serialized to null without checking the type nullability.
	Serialize func(array.Builder, any) bool
}

var SerdeMap = func() map[schemapb.DataType]SerdeEntry {
	m := make(map[schemapb.DataType]SerdeEntry)
	m[schemapb.DataType_Bool] = SerdeEntry{
		func(i int) arrow.DataType {
			return arrow.FixedWidthTypes.Boolean
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Boolean); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.BooleanBuilder); ok {
				if v, ok := v.(bool); ok {
					builder.Append(v)
					return true
				}
			}
			return false
		},
	}
	m[schemapb.DataType_Int8] = SerdeEntry{
		func(i int) arrow.DataType {
			return arrow.PrimitiveTypes.Int8
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Int8); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.Int8Builder); ok {
				if v, ok := v.(int8); ok {
					builder.Append(v)
					return true
				}
			}
			return false
		},
	}
	m[schemapb.DataType_Int16] = SerdeEntry{
		func(i int) arrow.DataType {
			return arrow.PrimitiveTypes.Int16
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Int16); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.Int16Builder); ok {
				if v, ok := v.(int16); ok {
					builder.Append(v)
					return true
				}
			}
			return false
		},
	}
	m[schemapb.DataType_Int32] = SerdeEntry{
		func(i int) arrow.DataType {
			return arrow.PrimitiveTypes.Int32
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Int32); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.Int32Builder); ok {
				if v, ok := v.(int32); ok {
					builder.Append(v)
					return true
				}
			}
			return false
		},
	}
	m[schemapb.DataType_Int64] = SerdeEntry{
		func(i int) arrow.DataType {
			return arrow.PrimitiveTypes.Int64
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Int64); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.Int64Builder); ok {
				if v, ok := v.(int64); ok {
					builder.Append(v)
					return true
				}
			}
			return false
		},
	}
	m[schemapb.DataType_Float] = SerdeEntry{
		func(i int) arrow.DataType {
			return arrow.PrimitiveTypes.Float32
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Float32); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.Float32Builder); ok {
				if v, ok := v.(float32); ok {
					builder.Append(v)
					return true
				}
			}
			return false
		},
	}
	m[schemapb.DataType_Double] = SerdeEntry{
		func(i int) arrow.DataType {
			return arrow.PrimitiveTypes.Float64
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Float64); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.Float64Builder); ok {
				if v, ok := v.(float64); ok {
					builder.Append(v)
					return true
				}
			}
			return false
		},
	}
	stringEntry := SerdeEntry{
		func(i int) arrow.DataType {
			return arrow.BinaryTypes.String
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.String); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.StringBuilder); ok {
				if v, ok := v.(string); ok {
					builder.Append(v)
					return true
				}
			}
			return false
		},
	}

	m[schemapb.DataType_VarChar] = stringEntry
	m[schemapb.DataType_String] = stringEntry
	// m[schemapb.DataType_Text] = stringEntry

	// We're not using the deserialized data in go, so we can skip the heavy pb serde.
	// If there is need in the future, just assign it to m[schemapb.DataType_Array]
	eagerArrayEntry := SerdeEntry{
		func(i int) arrow.DataType {
			return arrow.BinaryTypes.Binary
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Binary); ok && i < arr.Len() {
				v := &schemapb.ScalarField{}
				if err := proto.Unmarshal(arr.Value(i), v); err == nil {
					return v, true
				}
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.BinaryBuilder); ok {
				if vv, ok := v.(*schemapb.ScalarField); ok {
					if bytes, err := proto.Marshal(vv); err == nil {
						builder.Append(bytes)
						return true
					}
				}
			}
			return false
		},
	}
	_ = eagerArrayEntry

	byteEntry := SerdeEntry{
		func(i int) arrow.DataType {
			return arrow.BinaryTypes.Binary
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Binary); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.BinaryBuilder); ok {
				if vv, ok := v.([]byte); ok {
					builder.Append(vv)
					return true
				}
				if vv, ok := v.(*schemapb.ScalarField); ok {
					if bytes, err := proto.Marshal(vv); err == nil {
						builder.Append(bytes)
						return true
					}
				}
			}
			return false
		},
	}

	m[schemapb.DataType_Array] = eagerArrayEntry
	m[schemapb.DataType_JSON] = byteEntry

	fixedSizeDeserializer := func(a arrow.Array, i int) (any, bool) {
		if a.IsNull(i) {
			return nil, true
		}
		if arr, ok := a.(*array.FixedSizeBinary); ok && i < arr.Len() {
			return arr.Value(i), true
		}
		return nil, false
	}
	fixedSizeSerializer := func(b array.Builder, v any) bool {
		if v == nil {
			b.AppendNull()
			return true
		}
		if builder, ok := b.(*array.FixedSizeBinaryBuilder); ok {
			if v, ok := v.([]byte); ok {
				builder.Append(v)
				return true
			}
		}
		return false
	}

	m[schemapb.DataType_BinaryVector] = SerdeEntry{
		func(i int) arrow.DataType {
			return &arrow.FixedSizeBinaryType{ByteWidth: (i + 7) / 8}
		},
		fixedSizeDeserializer,
		fixedSizeSerializer,
	}
	m[schemapb.DataType_Float16Vector] = SerdeEntry{
		func(i int) arrow.DataType {
			return &arrow.FixedSizeBinaryType{ByteWidth: i * 2}
		},
		fixedSizeDeserializer,
		fixedSizeSerializer,
	}
	m[schemapb.DataType_BFloat16Vector] = SerdeEntry{
		func(i int) arrow.DataType {
			return &arrow.FixedSizeBinaryType{ByteWidth: i * 2}
		},
		fixedSizeDeserializer,
		fixedSizeSerializer,
	}
	// m[schemapb.DataType_Int8Vector] = SerdeEntry{
	// 	func(i int) arrow.DataType {
	// 		return &arrow.FixedSizeBinaryType{ByteWidth: i}
	// 	},
	// 	func(a arrow.Array, i int) (any, bool) {
	// 		if a.IsNull(i) {
	// 			return nil, true
	// 		}
	// 		if arr, ok := a.(*array.FixedSizeBinary); ok && i < arr.Len() {
	// 			// convert to []int8
	// 			bytes := arr.Value(i)
	// 			int8s := make([]int8, len(bytes))
	// 			for i, b := range bytes {
	// 				int8s[i] = int8(b)
	// 			}
	// 			return int8s, true
	// 		}
	// 		return nil, false
	// 	},
	// 	func(b array.Builder, v any) bool {
	// 		if v == nil {
	// 			b.AppendNull()
	// 			return true
	// 		}
	// 		if builder, ok := b.(*array.FixedSizeBinaryBuilder); ok {
	// 			if vv, ok := v.([]byte); ok {
	// 				builder.Append(vv)
	// 				return true
	// 			} else if vv, ok := v.([]int8); ok {
	// 				builder.Append(arrow.Int8Traits.CastToBytes(vv))
	// 				return true
	// 			}
	// 		}
	// 		return false
	// 	},
	// }
	m[schemapb.DataType_FloatVector] = SerdeEntry{
		func(i int) arrow.DataType {
			return &arrow.FixedSizeBinaryType{ByteWidth: i * 4}
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.FixedSizeBinary); ok && i < arr.Len() {
				return arrow.Float32Traits.CastFromBytes(arr.Value(i)), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.FixedSizeBinaryBuilder); ok {
				if vv, ok := v.([]float32); ok {
					dim := len(vv)
					byteLength := dim * 4
					bytesData := make([]byte, byteLength)
					for i, vec := range vv {
						bytes := math.Float32bits(vec)
						binary.LittleEndian.PutUint32(bytesData[i*4:], bytes)
					}
					builder.Append(bytesData)
					return true
				}
			}
			return false
		},
	}
	m[schemapb.DataType_SparseFloatVector] = byteEntry
	return m
}()
