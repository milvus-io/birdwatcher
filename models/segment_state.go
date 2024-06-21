package models

type SegmentState int32

const (
	SegmentStateSegmentStateNone SegmentState = 0
	SegmentStateNotExist         SegmentState = 1
	SegmentStateGrowing          SegmentState = 2
	SegmentStateSealed           SegmentState = 3
	SegmentStateFlushed          SegmentState = 4
	SegmentStateFlushing         SegmentState = 5
	SegmentStateDropped          SegmentState = 6
	SegmentStateImporting        SegmentState = 7
)

var SegmentStatename = map[int32]string{
	0: "SegmentStateNone",
	1: "NotExist",
	2: "Growing",
	3: "Sealed",
	4: "Flushed",
	5: "Flushing",
	6: "Dropped",
	7: "Importing",
}

var SegmentStatevalue = map[string]int32{
	"SegmentStateNone": 0,
	"NotExist":         1,
	"Growing":          2,
	"Sealed":           3,
	"Flushed":          4,
	"Flushing":         5,
	"Dropped":          6,
	"Importing":        7,
}

func (x SegmentState) String() string {
	return EnumName(SegmentStatename, int32(x))
}

type SegmentLevel int32

const (
	SegmentLevelLegacy SegmentLevel = 0
	SegmentLevelL0     SegmentLevel = 1
	SegmentLevelL1     SegmentLevel = 2
	SegmentLevelL2     SegmentLevel = 3
)

var SegmentLevelName = map[int32]string{
	0: "Legacy",
	1: "L0",
	2: "L1",
	3: "L2",
}

var SegmentLevelValue = map[string]int32{
	"Legacy": 0,
	"L0":     1,
	"L1":     2,
	"L2":     3,
}

func (x SegmentLevel) String() string {
	return EnumName(SegmentLevelName, int32(x))
}
