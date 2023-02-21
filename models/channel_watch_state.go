package models

type ChannelWatchState int32

const (
	ChannelWatchStateUncomplete     ChannelWatchState = 0
	ChannelWatchStateComplete       ChannelWatchState = 1
	ChannelWatchStateToWatch        ChannelWatchState = 2
	ChannelWatchStateWatchSuccess   ChannelWatchState = 3
	ChannelWatchStateWatchFailure   ChannelWatchState = 4
	ChannelWatchStateToRelease      ChannelWatchState = 5
	ChannelWatchStateReleaseSuccess ChannelWatchState = 6
	ChannelWatchStateReleaseFailure ChannelWatchState = 7
)

var ChannelWatchStatename = map[int32]string{
	0: "Uncomplete",
	1: "Complete",
	2: "ToWatch",
	3: "WatchSuccess",
	4: "WatchFailure",
	5: "ToRelease",
	6: "ReleaseSuccess",
	7: "ReleaseFailure",
}

var ChannelWatchStatevalue = map[string]int32{
	"Uncomplete":     0,
	"Complete":       1,
	"ToWatch":        2,
	"WatchSuccess":   3,
	"WatchFailure":   4,
	"ToRelease":      5,
	"ReleaseSuccess": 6,
	"ReleaseFailure": 7,
}

func (x ChannelWatchState) String() string {
	return EnumName(ChannelWatchStatename, int32(x))
}
