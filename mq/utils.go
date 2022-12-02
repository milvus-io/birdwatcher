package mq

import (
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/birdwatcher/mq/ifc"
	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/schemapb"
)

var (
	messageSerdeFactory = ProtoUDFactory{}
	MessageUnmarshal    = messageSerdeFactory.NewUnmarshalDispatcher()
)

func GetMsgPosition(msg ifc.Message) (*MsgPosition, error) {
	header := commonpb.MsgHeader{}
	if msg.Payload() == nil {
		return nil, fmt.Errorf("failed to unmarshal message header, payload is empty")
	}
	err := proto.Unmarshal(msg.Payload(), &header)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal message header, err %s", err.Error())
	}
	if header.Base == nil {
		return nil, fmt.Errorf("failed to unmarshal message, header is uncomplete")
	}
	tsMsg, err := MessageUnmarshal.Unmarshal(msg.Payload(), header.Base.MsgType)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal tsMsg, err %s", err.Error())
	}

	return &MsgPosition{
		ChannelName: filepath.Base(msg.Topic()),
		MsgID:       msg.ID().Serialize(),
		Timestamp:   tsMsg.BeginTs(),
	}, nil
}

func GetSizeOfIDs(data *schemapb.IDs) int {
	result := 0
	if data.IdField == nil {
		return result
	}

	switch data.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		result = len(data.GetIntId().GetData())
	case *schemapb.IDs_StrId:
		result = len(data.GetStrId().GetData())
	default:
		//TODO::
	}

	return result
}

func getNumRowsOfScalarField(datas interface{}) uint64 {
	realTypeDatas := reflect.ValueOf(datas)
	return uint64(realTypeDatas.Len())
}

func getNumRowsOfFloatVectorField(fDatas []float32, dim int64) (uint64, error) {
	if dim <= 0 {
		return 0, fmt.Errorf("dim(%d) should be greater than 0", dim)
	}
	l := len(fDatas)
	if int64(l)%dim != 0 {
		return 0, fmt.Errorf("the length(%d) of float data should divide the dim(%d)", l, dim)
	}
	return uint64(int64(l) / dim), nil
}

func getNumRowsOfBinaryVectorField(bDatas []byte, dim int64) (uint64, error) {
	if dim <= 0 {
		return 0, fmt.Errorf("dim(%d) should be greater than 0", dim)
	}
	if dim%8 != 0 {
		return 0, fmt.Errorf("dim(%d) should divide 8", dim)
	}
	l := len(bDatas)
	if (8*int64(l))%dim != 0 {
		return 0, fmt.Errorf("the num(%d) of all bits should divide the dim(%d)", 8*l, dim)
	}
	return uint64((8 * int64(l)) / dim), nil
}

const MsgIDNeedFill int64 = 0

type MsgBaseOptions func(*commonpb.MsgBase)

func WithMsgType(msgType commonpb.MsgType) MsgBaseOptions {
	return func(msgBase *commonpb.MsgBase) {
		msgBase.MsgType = msgType
	}
}

func WithMsgID(msgID int64) MsgBaseOptions {
	return func(msgBase *commonpb.MsgBase) {
		msgBase.MsgID = msgID
	}
}

func WithTimeStamp(ts uint64) MsgBaseOptions {
	return func(msgBase *commonpb.MsgBase) {
		msgBase.Timestamp = ts
	}
}

func WithSourceID(sourceID int64) MsgBaseOptions {
	return func(msgBase *commonpb.MsgBase) {
		msgBase.SourceID = sourceID
	}
}

func newMsgBaseDefault() *commonpb.MsgBase {
	return &commonpb.MsgBase{
		MsgType: commonpb.MsgType_Undefined,
		MsgID:   MsgIDNeedFill,
	}
}

func NewMsgBase(options ...MsgBaseOptions) *commonpb.MsgBase {
	msgBase := newMsgBaseDefault()
	for _, op := range options {
		op(msgBase)
	}
	return msgBase
}

// AppendFieldData appends fields data of specified index from src to dst
func AppendFieldData(dst []*schemapb.FieldData, src []*schemapb.FieldData, idx int64) {
	for i, fieldData := range src {
		switch fieldType := fieldData.Field.(type) {
		case *schemapb.FieldData_Scalars:
			if dst[i] == nil || dst[i].GetScalars() == nil {
				dst[i] = &schemapb.FieldData{
					Type:      fieldData.Type,
					FieldName: fieldData.FieldName,
					FieldId:   fieldData.FieldId,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{},
					},
				}
			}
			dstScalar := dst[i].GetScalars()
			switch srcScalar := fieldType.Scalars.Data.(type) {
			case *schemapb.ScalarField_BoolData:
				if dstScalar.GetBoolData() == nil {
					dstScalar.Data = &schemapb.ScalarField_BoolData{
						BoolData: &schemapb.BoolArray{
							Data: []bool{srcScalar.BoolData.Data[idx]},
						},
					}
				} else {
					dstScalar.GetBoolData().Data = append(dstScalar.GetBoolData().Data, srcScalar.BoolData.Data[idx])
				}
			case *schemapb.ScalarField_IntData:
				if dstScalar.GetIntData() == nil {
					dstScalar.Data = &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{srcScalar.IntData.Data[idx]},
						},
					}
				} else {
					dstScalar.GetIntData().Data = append(dstScalar.GetIntData().Data, srcScalar.IntData.Data[idx])
				}
			case *schemapb.ScalarField_LongData:
				if dstScalar.GetLongData() == nil {
					dstScalar.Data = &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{srcScalar.LongData.Data[idx]},
						},
					}
				} else {
					dstScalar.GetLongData().Data = append(dstScalar.GetLongData().Data, srcScalar.LongData.Data[idx])
				}
			case *schemapb.ScalarField_FloatData:
				if dstScalar.GetFloatData() == nil {
					dstScalar.Data = &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{
							Data: []float32{srcScalar.FloatData.Data[idx]},
						},
					}
				} else {
					dstScalar.GetFloatData().Data = append(dstScalar.GetFloatData().Data, srcScalar.FloatData.Data[idx])
				}
			case *schemapb.ScalarField_DoubleData:
				if dstScalar.GetDoubleData() == nil {
					dstScalar.Data = &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: []float64{srcScalar.DoubleData.Data[idx]},
						},
					}
				} else {
					dstScalar.GetDoubleData().Data = append(dstScalar.GetDoubleData().Data, srcScalar.DoubleData.Data[idx])
				}
			case *schemapb.ScalarField_StringData:
				if dstScalar.GetStringData() == nil {
					dstScalar.Data = &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{srcScalar.StringData.Data[idx]},
						},
					}
				} else {
					dstScalar.GetStringData().Data = append(dstScalar.GetStringData().Data, srcScalar.StringData.Data[idx])
				}
			default:
				panic("Not supported field type" + fieldData.Type.String())
			}
		case *schemapb.FieldData_Vectors:
			dim := fieldType.Vectors.Dim
			if dst[i] == nil || dst[i].GetVectors() == nil {
				dst[i] = &schemapb.FieldData{
					Type:      fieldData.Type,
					FieldName: fieldData.FieldName,
					FieldId:   fieldData.FieldId,
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim: dim,
						},
					},
				}
			}
			dstVector := dst[i].GetVectors()
			switch srcVector := fieldType.Vectors.Data.(type) {
			case *schemapb.VectorField_BinaryVector:
				if dstVector.GetBinaryVector() == nil {
					srcToCopy := srcVector.BinaryVector[idx*(dim/8) : (idx+1)*(dim/8)]
					dstVector.Data = &schemapb.VectorField_BinaryVector{
						BinaryVector: make([]byte, len(srcToCopy)),
					}
					copy(dstVector.Data.(*schemapb.VectorField_BinaryVector).BinaryVector, srcToCopy)
				} else {
					dstBinaryVector := dstVector.Data.(*schemapb.VectorField_BinaryVector)
					dstBinaryVector.BinaryVector = append(dstBinaryVector.BinaryVector, srcVector.BinaryVector[idx*(dim/8):(idx+1)*(dim/8)]...)
				}
			case *schemapb.VectorField_FloatVector:
				if dstVector.GetFloatVector() == nil {
					srcToCopy := srcVector.FloatVector.Data[idx*dim : (idx+1)*dim]
					dstVector.Data = &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: make([]float32, len(srcToCopy)),
						},
					}
					copy(dstVector.Data.(*schemapb.VectorField_FloatVector).FloatVector.Data, srcToCopy)
				} else {
					dstVector.GetFloatVector().Data = append(dstVector.GetFloatVector().Data, srcVector.FloatVector.Data[idx*dim:(idx+1)*dim]...)
				}
			default:
				panic("Not supported field type" + fieldData.Type.String())
			}
		}
	}
}
