package models

type DataType int32

const (
	DataTypeNone         DataType = 0
	DataTypeBool         DataType = 1
	DataTypeInt8         DataType = 2
	DataTypeInt16        DataType = 3
	DataTypeInt32        DataType = 4
	DataTypeInt64        DataType = 5
	DataTypeFloat        DataType = 10
	DataTypeDouble       DataType = 11
	DataTypeString       DataType = 20
	DataTypeVarChar      DataType = 21
	DataTypeBinaryVector DataType = 100
	DataTypeFloatVector  DataType = 101
)

var DataTypename = map[int32]string{
	0:   "None",
	1:   "Bool",
	2:   "Int8",
	3:   "Int16",
	4:   "Int32",
	5:   "Int64",
	10:  "Float",
	11:  "Double",
	20:  "String",
	21:  "VarChar",
	100: "BinaryVector",
	101: "FloatVector",
}

var DataTypevalue = map[string]int32{
	"None":         0,
	"Bool":         1,
	"Int8":         2,
	"Int16":        3,
	"Int32":        4,
	"Int64":        5,
	"Float":        10,
	"Double":       11,
	"String":       20,
	"VarChar":      21,
	"BinaryVector": 100,
	"FloatVector":  101,
}

func (x DataType) String() string {
	return EnumName(DataTypename, int32(x))
}
