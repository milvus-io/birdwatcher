package set

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
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

	// type params modification
	TypeParams       []string `name:"typeParams" default:"" desc:"type params to set, format: key=value (e.g., max_length=256,dim=128)"`
	DeleteTypeParams []string `name:"deleteTypeParams" default:"" desc:"type param keys to delete"`

	Run bool `name:"run" default:"false"`
}

func (c *ComponentSet) FieldAttrCommand(ctx context.Context, p *FieldAttrParam) error {
	if p.CollectionID <= 0 {
		return fmt.Errorf("invalid collection id(%d)", p.CollectionID)
	}
	if p.FieldID < 0 {
		return fmt.Errorf("invalid field id(%d)", p.FieldID)
	}

	// parse type params to set
	typeParamsToSet, err := parseTypeParams(p.TypeParams)
	if err != nil {
		return fmt.Errorf("failed to parse type params: %s", err.Error())
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

		// handle type params deletion
		if len(p.DeleteTypeParams) > 0 {
			deleteSet := make(map[string]struct{})
			for _, key := range p.DeleteTypeParams {
				deleteSet[key] = struct{}{}
			}
			newTypeParams := make([]*commonpb.KeyValuePair, 0, len(field.TypeParams))
			for _, kv := range field.TypeParams {
				if _, shouldDelete := deleteSet[kv.Key]; !shouldDelete {
					newTypeParams = append(newTypeParams, kv)
				} else {
					fmt.Printf("Deleting type param: %s=%s\n", kv.Key, kv.Value)
				}
			}
			field.TypeParams = newTypeParams
		}

		// handle type params update/add
		if len(typeParamsToSet) > 0 {
			// create a map from existing type params for easy lookup
			existingParams := make(map[string]*commonpb.KeyValuePair)
			for _, kv := range field.TypeParams {
				existingParams[kv.Key] = kv
			}

			for key, value := range typeParamsToSet {
				if existing, ok := existingParams[key]; ok {
					fmt.Printf("Updating type param: %s=%s -> %s=%s\n", key, existing.Value, key, value)
					existing.Value = value
				} else {
					fmt.Printf("Adding type param: %s=%s\n", key, value)
					field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
						Key:   key,
						Value: value,
					})
				}
			}
		}

		return nil
	}
	err = common.UpdateField(ctx, c.client, c.basePath, p.CollectionID, p.FieldID, alterField, !p.Run)
	if err != nil {
		return fmt.Errorf("failed to alter field (%s)", err.Error())
	}

	return nil
}

// parseTypeParams parses type params from string slice in format "key=value"
func parseTypeParams(params []string) (map[string]string, error) {
	result := make(map[string]string)
	for _, param := range params {
		if param == "" {
			continue
		}
		parts := strings.SplitN(param, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid type param format: %s, expected key=value", param)
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if key == "" {
			return nil, fmt.Errorf("type param key cannot be empty")
		}
		result[key] = value
	}
	return result, nil
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
