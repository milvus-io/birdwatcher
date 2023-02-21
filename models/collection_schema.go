package models

type CollectionSchema struct {
	Name   string
	Fields []FieldSchema
}

type FieldSchema struct {
	FieldID      int64
	Name         string
	IsPrimaryKey bool
	AutoID       bool
	DataType     DataType
	Description  string
	Properties   map[string]string
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
