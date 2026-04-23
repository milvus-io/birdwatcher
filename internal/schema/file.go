package schema

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/milvus-io/milvus/client/v2/entity"
)

type FieldSpec struct {
	Name           string `yaml:"name"`
	Type           string `yaml:"type"`
	Dim            int64  `yaml:"dim,omitempty"`
	MaxLength      int64  `yaml:"max_length,omitempty"`
	IsPrimaryKey   bool   `yaml:"is_primary_key,omitempty"`
	AutoID         bool   `yaml:"auto_id,omitempty"`
	Nullable       bool   `yaml:"nullable,omitempty"`
	IsPartitionKey bool   `yaml:"is_partition_key,omitempty"`
	Description    string `yaml:"description,omitempty"`
}

type SchemaSpec struct {
	Name               string      `yaml:"name"`
	Description        string      `yaml:"description,omitempty"`
	EnableDynamicField bool        `yaml:"enable_dynamic_field,omitempty"`
	AutoID             bool        `yaml:"auto_id,omitempty"`
	Fields             []FieldSpec `yaml:"fields"`
}

// BuildSchema turns a SchemaSpec into a Milvus entity.Schema.
func BuildSchema(s SchemaSpec) (*entity.Schema, error) {
	if s.Name == "" {
		return nil, fmt.Errorf("schema missing `name`")
	}
	if len(s.Fields) == 0 {
		return nil, fmt.Errorf("schema has no fields")
	}
	sch := entity.NewSchema().WithName(s.Name).
		WithDescription(s.Description).
		WithAutoID(s.AutoID).
		WithDynamicFieldEnabled(s.EnableDynamicField)
	for _, f := range s.Fields {
		field, err := BuildField(f)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", f.Name, err)
		}
		sch.WithField(field)
	}
	return sch, nil
}

func LoadFile(path string) (*entity.Schema, error) {
	bs, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read schema file: %w", err)
	}
	var fs SchemaSpec
	if err := yaml.Unmarshal(bs, &fs); err != nil {
		return nil, fmt.Errorf("parse schema yaml: %w", err)
	}
	return BuildSchema(fs)
}

func BuildField(f FieldSpec) (*entity.Field, error) {
	if f.Name == "" {
		return nil, fmt.Errorf("missing name")
	}
	ft, err := ParseFieldType(f.Type)
	if err != nil {
		return nil, err
	}
	field := entity.NewField().
		WithName(f.Name).
		WithDataType(ft).
		WithDescription(f.Description).
		WithIsPrimaryKey(f.IsPrimaryKey).
		WithIsAutoID(f.AutoID).
		WithNullable(f.Nullable).
		WithIsPartitionKey(f.IsPartitionKey)

	switch ft {
	case entity.FieldTypeFloatVector, entity.FieldTypeBinaryVector,
		entity.FieldTypeFloat16Vector, entity.FieldTypeBFloat16Vector,
		entity.FieldTypeInt8Vector:
		if f.Dim <= 0 {
			return nil, fmt.Errorf("vector field must set dim")
		}
		field.WithDim(f.Dim)
	case entity.FieldTypeVarChar:
		maxLen := f.MaxLength
		if maxLen <= 0 {
			maxLen = 256
		}
		field.WithMaxLength(maxLen)
	}
	return field, nil
}

func ParseFieldType(t string) (entity.FieldType, error) {
	switch strings.ToLower(t) {
	case "bool":
		return entity.FieldTypeBool, nil
	case "int8":
		return entity.FieldTypeInt8, nil
	case "int16":
		return entity.FieldTypeInt16, nil
	case "int32":
		return entity.FieldTypeInt32, nil
	case "int64":
		return entity.FieldTypeInt64, nil
	case "float":
		return entity.FieldTypeFloat, nil
	case "double":
		return entity.FieldTypeDouble, nil
	case "varchar":
		return entity.FieldTypeVarChar, nil
	case "json":
		return entity.FieldTypeJSON, nil
	case "array":
		return entity.FieldTypeArray, nil
	case "floatvector", "float_vector":
		return entity.FieldTypeFloatVector, nil
	case "binaryvector", "binary_vector":
		return entity.FieldTypeBinaryVector, nil
	case "float16vector", "float16_vector":
		return entity.FieldTypeFloat16Vector, nil
	case "bfloat16vector", "bfloat16_vector":
		return entity.FieldTypeBFloat16Vector, nil
	case "int8vector", "int8_vector":
		return entity.FieldTypeInt8Vector, nil
	default:
		return 0, fmt.Errorf("unsupported type %q", t)
	}
}
