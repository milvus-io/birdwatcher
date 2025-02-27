package common

const (
	RCPrefix = `root-coord`

	DBPrefix             = `database`
	DBInfoPrefix         = `db-info`
	CollectionPrefix     = `collection`
	CollectionInfoPrefix = `collection-info`
	PartitionPrefix      = `partitions`
	FieldPrefix          = `fields`

	SnapshotPrefix = "snapshots"
)

const (
	DCPrefix = `datacoord-meta`

	ChannelCheckpointPrefix = `channel-cp`
	ChannelWatchPrefix      = `channelwatch`
	ChannelRemovalPrefix    = `channel-removal`

	SegmentMetaPrefix      = "s"
	SegmentStatsMetaPrefix = "datacoord-meta/statslog"

	CompactionTaskPrefix = `compaction-task`
)

const (
	ReplicaPrefix         = `querycoord-replica`
	PartitionLoadedPrefix = "querycoord-partition-loadinfo"
	ResourceGroupPrefix   = `queryCoord-ResourceGroup/`
)

const (
	IndexPrefix        = `indexes`
	SegmentIndexPrefix = `segment-index`
)
