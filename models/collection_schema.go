package models

import (
	"errors"
	"strconv"

	"github.com/samber/lo"
)

type CollectionSchema struct {
	Name                string
	Fields              []FieldSchema
	EnableDynamicSchema bool
}

func (cs *CollectionSchema) GetPKField() (FieldSchema, bool) {
	return lo.Find(cs.Fields, func(fs FieldSchema) bool {
		return fs.IsPrimaryKey
	})
}

type FieldSchema struct {
	FieldID         int64
	Name            string
	IsPrimaryKey    bool
	AutoID          bool
	DataType        DataType
	Description     string
	Properties      map[string]string
	IsDynamic       bool
	IsPartitionKey  bool
	IsClusteringKey bool
	ElementType     DataType
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
