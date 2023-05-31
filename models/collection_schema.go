package models

import (
	"errors"
	"strconv"
)

type CollectionSchema struct {
	Name                string
	Fields              []FieldSchema
	EnableDynamicSchema bool
}

type FieldSchema struct {
	FieldID        int64
	Name           string
	IsPrimaryKey   bool
	AutoID         bool
	DataType       DataType
	Description    string
	Properties     map[string]string
	IsDynamic      bool
	IsPartitionKey bool
}

func (fs *FieldSchema) GetDim() (int64, error) {
	if fs.DataType != DataTypeFloatVector && fs.DataType != DataTypeBinaryVector {
		return -1, errors.New("field is not vector")
	}
	raw, ok := fs.Properties["dim"]
	if !ok {
		return -1, errors.New("field not has dim property")
	}

	dim, err := strconv.ParseInt(raw, 10, 64)
	return dim, err
}

func newSchemaFromBase[schemaBase interface {
	GetName() string
}](schema schemaBase) CollectionSchema {
	return CollectionSchema{
		Name: schema.GetName(),
	}
}

func NewFieldSchemaFromBase[fieldSchemaBase interface {
	GetFieldID() int64
	GetName() string
	GetAutoID() bool
	GetIsPrimaryKey() bool
	GetDescription() string
	GetDataType() dt
}, dt ~int32](schema fieldSchemaBase) FieldSchema {
	return FieldSchema{
		FieldID:      schema.GetFieldID(),
		Name:         schema.GetName(),
		DataType:     DataType(schema.GetDataType()),
		IsPrimaryKey: schema.GetIsPrimaryKey(),
		AutoID:       schema.GetAutoID(),
		Description:  schema.GetDescription(),
	}
}
