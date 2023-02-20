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
