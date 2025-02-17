package models

// RateMetricLabel defines the metric label collected from nodes.
type RateMetricLabel = string

const (
	ReadResultThroughput    RateMetricLabel = "ReadResultThroughput"
	InsertConsumeThroughput RateMetricLabel = "InsertConsumeThroughput"
	DeleteConsumeThroughput RateMetricLabel = "DeleteConsumeThroughput"
)

const (
	UnsolvedQueueType string = "Unsolved"
	ReadyQueueType    string = "Ready"
	ReceiveQueueType  string = "Receive"
	ExecuteQueueType  string = "Execute"
)

// RateMetric contains a RateMetricLabel and a float rate.
type RateMetric struct {
	Label RateMetricLabel
	Rate  float64
}

// FlowGraphMetric contains a minimal timestamp of flow graph and the number of flow graphs.
type FlowGraphMetric struct {
	MinFlowGraphChannel string
	MinFlowGraphTt      uint64
	NumFlowGraph        int
}

// NodeEffect contains the a node and its effected collection info.
type NodeEffect struct {
	NodeID        int64
	CollectionIDs []int64
}

// QueryNodeQuotaMetrics are metrics of QueryNode.
type QueryNodeQuotaMetrics struct {
	Hms                 HardwareMetrics
	Rms                 []RateMetric
	Fgm                 FlowGraphMetric
	GrowingSegmentsSize int64
	Effect              NodeEffect
	DeleteBufferInfo    DeleteBufferInfo
}

type DeleteBufferInfo struct {
	CollectionDeleteBufferNum  map[int64]int64
	CollectionDeleteBufferSize map[int64]int64
}

type DataCoordQuotaMetrics struct {
	TotalBinlogSize      int64
	CollectionBinlogSize map[int64]int64
	PartitionsBinlogSize map[int64]map[int64]int64
	// l0 segments
	CollectionL0RowCount map[int64]int64
}

// DataNodeQuotaMetrics are metrics of DataNode.
type DataNodeQuotaMetrics struct {
	Hms    HardwareMetrics
	Rms    []RateMetric
	Fgm    FlowGraphMetric
	Effect NodeEffect
}

// ProxyQuotaMetrics are metrics of Proxy.
type ProxyQuotaMetrics struct {
	Hms HardwareMetrics
	Rms []RateMetric
}
