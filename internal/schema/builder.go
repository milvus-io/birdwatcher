package schema

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/client/v2/entity"
)

type BuilderOptions struct {
	Name         string
	PKType       string
	AutoID       bool
	PKMaxLength  int64
	VectorField  string
	VectorType   string
	Dim          int64
	DynamicField bool
	ScalarFields []string
}

func Build(opts BuilderOptions) (*entity.Schema, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("collection name is required")
	}
	if opts.Dim <= 0 {
		return nil, fmt.Errorf("vector dim must be positive")
	}

	pkName := "id"
	pkField := entity.NewField().WithName(pkName).WithIsPrimaryKey(true).WithIsAutoID(opts.AutoID)
	switch strings.ToLower(opts.PKType) {
	case "", "int64":
		pkField.WithDataType(entity.FieldTypeInt64)
	case "varchar":
		maxLen := opts.PKMaxLength
		if maxLen == 0 {
			maxLen = 256
		}
		pkField.WithDataType(entity.FieldTypeVarChar).WithMaxLength(maxLen)
	default:
		return nil, fmt.Errorf("unsupported pk type %q (use int64 or varchar)", opts.PKType)
	}

	vecName := opts.VectorField
	if vecName == "" {
		vecName = "vector"
	}
	vecType, err := parseVectorType(opts.VectorType)
	if err != nil {
		return nil, err
	}
	vecField := entity.NewField().WithName(vecName).WithDataType(vecType).WithDim(opts.Dim)

	s := entity.NewSchema().WithName(opts.Name).WithDynamicFieldEnabled(opts.DynamicField).
		WithField(pkField).WithField(vecField)

	for _, raw := range opts.ScalarFields {
		field, err := parseScalarField(raw)
		if err != nil {
			return nil, fmt.Errorf("scalar field %q: %w", raw, err)
		}
		s.WithField(field)
	}

	return s, nil
}

func parseVectorType(v string) (entity.FieldType, error) {
	switch strings.ToLower(v) {
	case "", "float_vector", "float":
		return entity.FieldTypeFloatVector, nil
	case "binary_vector", "binary":
		return entity.FieldTypeBinaryVector, nil
	case "float16_vector", "float16", "fp16":
		return entity.FieldTypeFloat16Vector, nil
	case "bfloat16_vector", "bfloat16", "bf16":
		return entity.FieldTypeBFloat16Vector, nil
	case "int8_vector", "int8":
		return entity.FieldTypeInt8Vector, nil
	default:
		return 0, fmt.Errorf("unsupported vector type %q", v)
	}
}

// parseScalarField expects "name:type[:maxLen]" format. type is one of:
// int8/int16/int32/int64/float/double/bool/varchar/json.
func parseScalarField(raw string) (*entity.Field, error) {
	parts := strings.Split(raw, ":")
	if len(parts) < 2 {
		return nil, fmt.Errorf("expected name:type[:maxLen]")
	}
	name := parts[0]
	if name == "" {
		return nil, fmt.Errorf("empty field name")
	}
	f := entity.NewField().WithName(name)
	switch strings.ToLower(parts[1]) {
	case "int8":
		f.WithDataType(entity.FieldTypeInt8)
	case "int16":
		f.WithDataType(entity.FieldTypeInt16)
	case "int32":
		f.WithDataType(entity.FieldTypeInt32)
	case "int64":
		f.WithDataType(entity.FieldTypeInt64)
	case "float":
		f.WithDataType(entity.FieldTypeFloat)
	case "double":
		f.WithDataType(entity.FieldTypeDouble)
	case "bool":
		f.WithDataType(entity.FieldTypeBool)
	case "json":
		f.WithDataType(entity.FieldTypeJSON)
	case "varchar":
		maxLen := int64(256)
		if len(parts) >= 3 {
			n, err := strconv.ParseInt(parts[2], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid maxLen %q", parts[2])
			}
			maxLen = n
		}
		f.WithDataType(entity.FieldTypeVarChar).WithMaxLength(maxLen)
	default:
		return nil, fmt.Errorf("unsupported type %q", parts[1])
	}
	return f, nil
}
