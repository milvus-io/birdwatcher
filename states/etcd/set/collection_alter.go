package set

import (
	"context"
	"fmt"
	"strconv"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type FieldAttrParam struct {
	framework.ParamBase `use:"set field-attr" desc:"set field attribute for collection"`
	CollectionID        int64 `name:"collectionID" default:"0" desc:"collection id to update"`
	FieldID             int64 `name:"fieldID" default:"0" desc:"field id to update"`

	// target attributes
	IsClusteringKey bool   `name:"clusterKey" default:"false" desc:"flags indicating whether to enable clusterKey"`
	Nullable        bool   `name:"nullable" default:"false" desc:"flags indicating whether to enable nullable"`
	DefaultValue    string `name:"defaultValue" default:"" desc:"default value for nullable field"`

	Run bool `name:"run" default:"false"`
}

func (c *ComponentSet) FieldAttrCommand(ctx context.Context, p *FieldAttrParam) error {
	if p.CollectionID <= 0 {
		return fmt.Errorf("invalid collection id(%d)", p.CollectionID)
	}
	if p.FieldID < 0 {
		return fmt.Errorf("invalid field id(%d)", p.FieldID)
	}

	alterField := func(field *schemapb.FieldSchema) error {
		if field.FieldID != p.FieldID {
			return nil
		}
		field.IsClusteringKey = p.IsClusteringKey
		field.Nullable = p.Nullable
		if p.DefaultValue != "" {
			defaultValue, err := GetDefaultValueForDataType(field.DataType, p.DefaultValue)
			if err != nil {
				return fmt.Errorf("failed to parse default value (%s)", err.Error())
			}
			field.DefaultValue = defaultValue
		}
		return nil
	}
	err := common.UpdateField(ctx, c.client, c.basePath, p.CollectionID, p.FieldID, alterField, !p.Run)
	if err != nil {
		return fmt.Errorf("failed to alter field (%s)", err.Error())
	}

	return nil
}

func GetDefaultValueForDataType(dataType schemapb.DataType, defaultValue string) (*schemapb.ValueField, error) {
	switch dataType {
	case schemapb.DataType_Bool:
		v, err := strconv.ParseBool(defaultValue)
		if err != nil {
			return nil, err
		}
		return &schemapb.ValueField{
			Data: &schemapb.ValueField_BoolData{
				BoolData: v,
			},
		}, nil
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		return &schemapb.ValueField{
			Data: &schemapb.ValueField_StringData{
				StringData: defaultValue,
			},
		}, nil
	case schemapb.DataType_Int64:
		v, err := strconv.ParseInt(defaultValue, 10, 64)
		if err != nil {
			return nil, err
		}
		return &schemapb.ValueField{
			Data: &schemapb.ValueField_LongData{
				LongData: v,
			},
		}, nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		v, err := strconv.ParseInt(defaultValue, 10, 32)
		if err != nil {
			return nil, err
		}
		return &schemapb.ValueField{
			Data: &schemapb.ValueField_IntData{
				IntData: int32(v),
			},
		}, nil
	case schemapb.DataType_Float:
		v, err := strconv.ParseFloat(defaultValue, 32)
		if err != nil {
			return nil, err
		}
		return &schemapb.ValueField{
			Data: &schemapb.ValueField_FloatData{
				FloatData: float32(v),
			},
		}, nil
	case schemapb.DataType_Double:
		v, err := strconv.ParseFloat(defaultValue, 64)
		if err != nil {
			return nil, err
		}
		return &schemapb.ValueField{
			Data: &schemapb.ValueField_DoubleData{
				DoubleData: v,
			},
		}, nil
	case schemapb.DataType_JSON:
		return &schemapb.ValueField{
			Data: &schemapb.ValueField_BytesData{
				BytesData: []byte(defaultValue),
			},
		}, nil
	default:
		return nil, fmt.Errorf("setting default value for data type %s is not supported yet", dataType.String())
	}
}
