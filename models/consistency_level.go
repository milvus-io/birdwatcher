package models

type ConsistencyLevel int32

const (
	ConsistencyLevelStrong     ConsistencyLevel = 0
	ConsistencyLevelSession    ConsistencyLevel = 1
	ConsistencyLevelBounded    ConsistencyLevel = 2
	ConsistencyLevelEventually ConsistencyLevel = 3
	ConsistencyLevelCustomized ConsistencyLevel = 4
)

var ConsistencyLevelname = map[int32]string{
	0: "Strong",
	1: "Session",
	2: "Bounded",
	3: "Eventually",
	4: "Customized",
}

var ConsistencyLevelvalue = map[string]int32{
	"Strong":     0,
	"Session":    1,
	"Bounded":    2,
	"Eventually": 3,
	"Customized": 4,
}

func (x ConsistencyLevel) String() string {
	return EnumName(ConsistencyLevelname, int32(x))
}
