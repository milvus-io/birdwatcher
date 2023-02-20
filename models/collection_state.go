package models

type CollectionState int32

const (
	CollectionStateCollectionCreated  CollectionState = 0
	CollectionStateCollectionCreating CollectionState = 1
	CollectionStateCollectionDropping CollectionState = 2
	CollectionStateCollectionDropped  CollectionState = 3
)

var CollectionStatename = map[int32]string{
	0: "CollectionCreated",
	1: "CollectionCreating",
	2: "CollectionDropping",
	3: "CollectionDropped",
}

var CollectionStatevalue = map[string]int32{
	"CollectionCreated":  0,
	"CollectionCreating": 1,
	"CollectionDropping": 2,
	"CollectionDropped":  3,
}

func (x CollectionState) String() string {
	return EnumName(CollectionStatename, int32(x))
}
