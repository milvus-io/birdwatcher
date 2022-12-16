package show

import (
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	"github.com/milvus-io/birdwatcher/utils"
)

const (
	triggerTaskPrefix     = "queryCoord-triggerTask"
	activeTaskPrefix      = "queryCoord-activeTask"
	taskInfoPrefix        = "queryCoord-taskInfo"
	loadBalanceInfoPrefix = "queryCoord-loadBalanceInfo"
)

var (
	ErrCollectionLoaded       = errors.New("CollectionLoaded")
	ErrLoadParametersMismatch = errors.New("LoadParametersMismatch")
)

type taskState int

const (
	taskUndo    taskState = 0
	taskDoing   taskState = 1
	taskDone    taskState = 3
	taskExpired taskState = 4
	taskFailed  taskState = 5
)

var taskStateNames = map[taskState]string{
	0: "taskUndo",
	1: "taskDoing",
	3: "taskDone",
	4: "taskExpired",
	5: "taskFailed",
}

func (x taskState) String() string {
	ret, ok := taskStateNames[x]
	if !ok {
		return "None"
	}
	return ret
}

type Timestamp = uint64

// UniqueID is an alias of int64
type UniqueID = int64

type queryCoordTask interface {
	getTaskID() UniqueID       // return ReqId
	getCollectionID() UniqueID // return ReqId
	setTaskID(id UniqueID)
	msgType() commonpb.MsgType
	msgBase() *commonpb.MsgBase
	timestamp() Timestamp
	marshal() ([]byte, error)
	getState() taskState
	setState(state taskState)
	String() string
	getTriggerCondition() querypb.TriggerCondition
	setTriggerCondition(trigger querypb.TriggerCondition)
	getType() string
	setType(taskType string)
}

type baseQueryCoordTask struct {
	taskID           UniqueID
	triggerCondition querypb.TriggerCondition
	parentTask       queryCoordTask
	childTasks       []queryCoordTask
	state            taskState
	taskType         string
}

// getTaskID function returns the unique taskID of the trigger task
func (bt *baseQueryCoordTask) getTaskID() UniqueID {
	return bt.taskID
}

func (bt *baseQueryCoordTask) getType() string {
	return bt.taskType
}

func (bt *baseQueryCoordTask) setType(taskType string) {
	bt.taskType = taskType
}

func (bt *baseQueryCoordTask) String() string {
	state := bt.getState()
	taskID := bt.getTaskID()
	condition := bt.getTriggerCondition()
	ret := fmt.Sprintf("taskID:%d \t type:%s, taskState:%s, condition:%s", taskID, bt.getType(), state.String(), condition.String())
	return ret
}

// setTaskID function sets the trigger task with a unique id, which is allocated by tso
func (bt *baseQueryCoordTask) setTaskID(id UniqueID) {
	bt.taskID = id
}

func (bt *baseQueryCoordTask) getTriggerCondition() querypb.TriggerCondition {
	return bt.triggerCondition
}

func (bt *baseQueryCoordTask) setTriggerCondition(trigger querypb.TriggerCondition) {
	bt.triggerCondition = trigger
}

// State returns the state of task, such as taskUndo, taskDoing, taskDone, taskExpired, taskFailed
func (bt *baseQueryCoordTask) getState() taskState {
	return bt.state
}

func (bt *baseQueryCoordTask) setState(state taskState) {
	bt.state = state
}

func newBaseQueryCoordTask(triggerType querypb.TriggerCondition) *baseQueryCoordTask {
	baseTask := &baseQueryCoordTask{
		triggerCondition: triggerType,
		childTasks:       []queryCoordTask{},
	}

	return baseTask
}

func unmarshalQueryTask(taskID UniqueID, t string) (queryCoordTask, error) {
	header := commonpb.MsgHeader{}
	err := proto.Unmarshal([]byte(t), &header)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal message header, err %s ", err.Error())
	}
	var newTask queryCoordTask
	baseTask := newBaseQueryCoordTask(querypb.TriggerCondition_GrpcRequest)
	switch header.Base.MsgType {
	case commonpb.MsgType_LoadCollection:
		loadReq := querypb.LoadCollectionRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		loadCollectionTask := &loadCollectionTask{
			baseQueryCoordTask:    baseTask,
			LoadCollectionRequest: &loadReq,
		}
		newTask = loadCollectionTask
	case commonpb.MsgType_LoadPartitions:
		loadReq := querypb.LoadPartitionsRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		loadPartitionTask := &loadPartitionTask{
			baseQueryCoordTask:    baseTask,
			LoadPartitionsRequest: &loadReq,
		}
		newTask = loadPartitionTask
	case commonpb.MsgType_ReleaseCollection:
		loadReq := querypb.ReleaseCollectionRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		releaseCollectionTask := &releaseCollectionTask{
			baseQueryCoordTask:       baseTask,
			ReleaseCollectionRequest: &loadReq,
		}
		newTask = releaseCollectionTask
	case commonpb.MsgType_ReleasePartitions:
		loadReq := querypb.ReleasePartitionsRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		releasePartitionTask := &releasePartitionTask{
			baseQueryCoordTask:       baseTask,
			ReleasePartitionsRequest: &loadReq,
		}
		newTask = releasePartitionTask
	case commonpb.MsgType_LoadSegments:
		//TODO::trigger condition may be different
		loadReq := querypb.LoadSegmentsRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		loadSegmentTask := &loadSegmentTask{
			baseQueryCoordTask:  baseTask,
			LoadSegmentsRequest: &loadReq,
			excludeNodeIDs:      []int64{},
		}
		newTask = loadSegmentTask
	case commonpb.MsgType_ReleaseSegments:
		//TODO::trigger condition may be different
		loadReq := querypb.ReleaseSegmentsRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		releaseSegmentTask := &releaseSegmentTask{
			baseQueryCoordTask:     baseTask,
			ReleaseSegmentsRequest: &loadReq,
		}
		newTask = releaseSegmentTask
	case commonpb.MsgType_WatchDmChannels:
		//TODO::trigger condition may be different
		req := querypb.WatchDmChannelsRequest{}
		err = proto.Unmarshal([]byte(t), &req)
		if err != nil {
			return nil, err
		}
		watchDmChannelTask := &watchDmChannelTask{
			baseQueryCoordTask:     baseTask,
			WatchDmChannelsRequest: &req,
			excludeNodeIDs:         []int64{},
		}
		newTask = watchDmChannelTask
	case commonpb.MsgType_WatchDeltaChannels:
		fmt.Println("legacy WatchDeltaChannels type found, ignore")
	case commonpb.MsgType_WatchQueryChannels:
		//Deprecated WatchQueryChannel
		fmt.Println("legacy WatchQueryChannels type found, ignore")
	case commonpb.MsgType_LoadBalanceSegments:
		//TODO::trigger condition may be different
		loadReq := querypb.LoadBalanceRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		// if triggerCondition == nodeDown, and the queryNode resources are insufficient,
		// queryCoord will waits until queryNode can load the data, ensuring that the data is not lost
		baseTask = newBaseQueryCoordTask(loadReq.BalanceReason)
		loadBalanceTask := &loadBalanceTask{
			baseQueryCoordTask: baseTask,
			LoadBalanceRequest: &loadReq,
		}
		newTask = loadBalanceTask
	case commonpb.MsgType_HandoffSegments:
		handoffReq := querypb.HandoffSegmentsRequest{}
		err = proto.Unmarshal([]byte(t), &handoffReq)
		if err != nil {
			return nil, err
		}
		handoffTask := &handoffTask{
			baseQueryCoordTask:     baseTask,
			HandoffSegmentsRequest: &handoffReq,
		}
		newTask = handoffTask
	default:
		err = errors.New("inValid msg type when unMarshal task")
		return nil, err
	}

	newTask.setTaskID(taskID)
	return newTask, nil
}

type loadCollectionTask struct {
	*baseQueryCoordTask
	*querypb.LoadCollectionRequest
}

func (lct *loadCollectionTask) String() string {
	baseStr := lct.baseQueryCoordTask.String()
	typeStr := fmt.Sprintf("type:%s", lct.msgType().String())
	ts := lct.timestamp()
	time, logic := utils.ParseTS(ts)
	timeStr := fmt.Sprintf("time:%s, logicTs:%d", time, logic)
	ret := fmt.Sprintf("%s\t%s\t%s\ninfo:%s", baseStr, typeStr, timeStr, lct.LoadCollectionRequest.String())
	return ret
}

func (lct *loadCollectionTask) getCollectionID() UniqueID {
	return lct.GetCollectionID()
}

func (lct *loadCollectionTask) msgBase() *commonpb.MsgBase {
	return lct.Base
}

func (lct *loadCollectionTask) marshal() ([]byte, error) {
	return proto.Marshal(lct.LoadCollectionRequest)
}

func (lct *loadCollectionTask) msgType() commonpb.MsgType {
	return lct.Base.MsgType
}

func (lct *loadCollectionTask) timestamp() Timestamp {
	ret := lct.Base.Timestamp
	if ret == 0 {
		ret = uint64(lct.getTaskID())
	}
	return ret
}

// releaseCollectionTask will release all the data of this collection on query nodes
type releaseCollectionTask struct {
	*baseQueryCoordTask
	*querypb.ReleaseCollectionRequest
}

func (rct *releaseCollectionTask) String() string {
	baseStr := rct.baseQueryCoordTask.String()
	typeStr := fmt.Sprintf("type:%s", rct.msgType().String())
	ts := rct.timestamp()
	time, logic := utils.ParseTS(ts)
	timeStr := fmt.Sprintf("time:%s, logicTs:%d", time, logic)
	ret := fmt.Sprintf("%s\t%s\t%s\ninfo:%s", baseStr, typeStr, timeStr, rct.ReleaseCollectionRequest.String())
	return ret
}

func (rct *releaseCollectionTask) getCollectionID() UniqueID {
	return rct.GetCollectionID()
}

func (rct *releaseCollectionTask) msgBase() *commonpb.MsgBase {
	return rct.Base
}

func (rct *releaseCollectionTask) marshal() ([]byte, error) {
	return proto.Marshal(rct.ReleaseCollectionRequest)
}

func (rct *releaseCollectionTask) msgType() commonpb.MsgType {
	return rct.Base.MsgType
}

func (rct *releaseCollectionTask) timestamp() Timestamp {
	ret := rct.Base.Timestamp
	if ret == 0 {
		ret = uint64(rct.getTaskID())
	}
	return ret
}

// loadPartitionTask will load all the data of this partition to query nodes
type loadPartitionTask struct {
	*baseQueryCoordTask
	*querypb.LoadPartitionsRequest
	addCol bool
}

func (lpt *loadPartitionTask) String() string {
	baseStr := lpt.baseQueryCoordTask.String()
	typeStr := fmt.Sprintf("type:%s", lpt.msgType().String())
	ts := lpt.timestamp()
	time, logic := utils.ParseTS(ts)
	timeStr := fmt.Sprintf("time:%s, logicTs:%d", time, logic)
	ret := fmt.Sprintf("%s\t%s\t%s\ninfo:%s", baseStr, typeStr, timeStr, lpt.LoadPartitionsRequest.String())
	return ret
}

func (lpt *loadPartitionTask) getCollectionID() UniqueID {
	return lpt.GetCollectionID()
}

func (lpt *loadPartitionTask) msgBase() *commonpb.MsgBase {
	return lpt.Base
}

func (lpt *loadPartitionTask) marshal() ([]byte, error) {
	return proto.Marshal(lpt.LoadPartitionsRequest)
}

func (lpt *loadPartitionTask) msgType() commonpb.MsgType {
	return lpt.Base.MsgType
}

func (lpt *loadPartitionTask) timestamp() Timestamp {
	ret := lpt.Base.Timestamp
	if ret == 0 {
		ret = uint64(lpt.getTaskID())
	}
	return ret
}

// releasePartitionTask will release all the data of this partition on query nodes
type releasePartitionTask struct {
	*baseQueryCoordTask
	*querypb.ReleasePartitionsRequest
}

func (rpt *releasePartitionTask) String() string {
	baseStr := rpt.baseQueryCoordTask.String()
	typeStr := fmt.Sprintf("type:%s", rpt.msgType().String())
	ts := rpt.timestamp()
	time, logic := utils.ParseTS(ts)
	timeStr := fmt.Sprintf("time:%s, logicTs:%d", time, logic)
	ret := fmt.Sprintf("%s\t%s\t%s\ninfo:%s", baseStr, typeStr, timeStr, rpt.ReleasePartitionsRequest.String())
	return ret
}

func (rpt *releasePartitionTask) getCollectionID() UniqueID {
	return rpt.GetCollectionID()
}

func (rpt *releasePartitionTask) msgBase() *commonpb.MsgBase {
	return rpt.Base
}

func (rpt *releasePartitionTask) marshal() ([]byte, error) {
	return proto.Marshal(rpt.ReleasePartitionsRequest)
}

func (rpt *releasePartitionTask) msgType() commonpb.MsgType {
	return rpt.Base.MsgType
}

func (rpt *releasePartitionTask) timestamp() Timestamp {
	ret := rpt.Base.Timestamp
	if ret == 0 {
		ret = uint64(rpt.getTaskID())
	}
	return ret
}

type loadSegmentTask struct {
	*baseQueryCoordTask
	*querypb.LoadSegmentsRequest
	excludeNodeIDs []int64
}

func (lst *loadSegmentTask) String() string {
	baseStr := lst.baseQueryCoordTask.String()
	typeStr := fmt.Sprintf("type:%s", lst.msgType().String())
	ts := lst.timestamp()
	time, logic := utils.ParseTS(ts)
	timeStr := fmt.Sprintf("time:%s, logicTs:%d", time, logic)
	ret := fmt.Sprintf("%s\t%s\t%s\ninfo:%s", baseStr, typeStr, timeStr, lst.LoadSegmentsRequest.String())
	return ret
}

func (lst *loadSegmentTask) getCollectionID() UniqueID {
	return lst.GetCollectionID()
}

func (lst *loadSegmentTask) msgBase() *commonpb.MsgBase {
	return lst.Base
}

func (lst *loadSegmentTask) marshal() ([]byte, error) {
	return proto.Marshal(lst.LoadSegmentsRequest)
}

func (lst *loadSegmentTask) msgType() commonpb.MsgType {
	return lst.Base.MsgType
}

func (lst *loadSegmentTask) timestamp() Timestamp {
	ret := lst.Base.Timestamp
	if ret == 0 {
		ret = uint64(lst.getTaskID())
	}
	return ret
}

type releaseSegmentTask struct {
	*baseQueryCoordTask
	*querypb.ReleaseSegmentsRequest
}

func (rst *releaseSegmentTask) String() string {
	baseStr := rst.baseQueryCoordTask.String()
	typeStr := fmt.Sprintf("type:%s", rst.msgType().String())
	ts := rst.timestamp()
	time, logic := utils.ParseTS(ts)
	timeStr := fmt.Sprintf("time:%s, logicTs:%d", time, logic)
	ret := fmt.Sprintf("%s\t%s\t%s\ninfo:%s", baseStr, typeStr, timeStr, rst.ReleaseSegmentsRequest.String())
	return ret
}

func (rst *releaseSegmentTask) getCollectionID() UniqueID {
	return rst.GetCollectionID()
}

func (rst *releaseSegmentTask) msgBase() *commonpb.MsgBase {
	return rst.Base
}

func (rst *releaseSegmentTask) marshal() ([]byte, error) {
	return proto.Marshal(rst.ReleaseSegmentsRequest)
}

func (rst *releaseSegmentTask) msgType() commonpb.MsgType {
	return rst.Base.MsgType
}

func (rst *releaseSegmentTask) timestamp() Timestamp {
	ret := rst.Base.Timestamp
	if ret == 0 {
		ret = uint64(rst.getTaskID())
	}
	return ret
}

type watchDmChannelTask struct {
	*baseQueryCoordTask
	*querypb.WatchDmChannelsRequest
	excludeNodeIDs []int64
}

func (wdt *watchDmChannelTask) String() string {
	baseStr := wdt.baseQueryCoordTask.String()
	typeStr := fmt.Sprintf("type:%s", wdt.msgType().String())
	ts := wdt.timestamp()
	time, logic := utils.ParseTS(ts)
	timeStr := fmt.Sprintf("time:%s, logicTs:%d", time, logic)
	ret := fmt.Sprintf("%s\t%s\t%s\ninfo:%s", baseStr, typeStr, timeStr, wdt.WatchDmChannelsRequest.String())
	return ret
}

func (wdt *watchDmChannelTask) getCollectionID() UniqueID {
	return wdt.GetCollectionID()
}

func (wdt *watchDmChannelTask) msgBase() *commonpb.MsgBase {
	return wdt.Base
}

func (wdt *watchDmChannelTask) marshal() ([]byte, error) {
	return nil, nil
	//return proto.Marshal(thinWatchDmChannelsRequest(wdt.WatchDmChannelsRequest))
}

func (wdt *watchDmChannelTask) msgType() commonpb.MsgType {
	return wdt.Base.MsgType
}

func (wdt *watchDmChannelTask) timestamp() Timestamp {
	ret := wdt.Base.Timestamp
	if ret == 0 {
		ret = uint64(wdt.getTaskID())
	}
	return ret
}

// handoffTask handoff definition
type handoffTask struct {
	*baseQueryCoordTask
	*querypb.HandoffSegmentsRequest
}

func (ht *handoffTask) String() string {
	baseStr := ht.baseQueryCoordTask.String()
	typeStr := fmt.Sprintf("type:%s", ht.msgType().String())
	ts := ht.timestamp()
	time, logic := utils.ParseTS(ts)
	timeStr := fmt.Sprintf("time:%s, logicTs:%d", time, logic)
	ret := fmt.Sprintf("%s\t%s\t%s\ninfo:%s", baseStr, typeStr, timeStr, ht.HandoffSegmentsRequest.String())
	return ret
}

func (ht *handoffTask) getCollectionID() UniqueID {
	return 0
}

func (ht *handoffTask) msgBase() *commonpb.MsgBase {
	return ht.Base
}

func (ht *handoffTask) marshal() ([]byte, error) {
	return proto.Marshal(ht.HandoffSegmentsRequest)
}

func (ht *handoffTask) msgType() commonpb.MsgType {
	return ht.Base.MsgType
}

func (ht *handoffTask) timestamp() Timestamp {
	ret := ht.Base.Timestamp
	if ret == 0 {
		ret = uint64(ht.getTaskID())
	}
	return ret
}

type loadBalanceTask struct {
	*baseQueryCoordTask
	*querypb.LoadBalanceRequest
}

func (lbt *loadBalanceTask) String() string {
	baseStr := lbt.baseQueryCoordTask.String()
	typeStr := fmt.Sprintf("type:%s", lbt.msgType().String())
	ts := lbt.timestamp()
	time, logic := utils.ParseTS(ts)
	timeStr := fmt.Sprintf("time:%s, logicTs:%d", time, logic)
	ret := fmt.Sprintf("%s\t%s\t%s", baseStr, typeStr, timeStr) //fmt.Sprintf("%s\t%s\t%s\ninfo:%s", baseStr, typeStr, timeStr, lbt.LoadBalanceRequest.String())
	return ret
}

func (lbt *loadBalanceTask) getCollectionID() UniqueID {
	return lbt.GetCollectionID()
}

func (lbt *loadBalanceTask) msgBase() *commonpb.MsgBase {
	return lbt.Base
}

func (lbt *loadBalanceTask) marshal() ([]byte, error) {
	return proto.Marshal(lbt.LoadBalanceRequest)
}

func (lbt *loadBalanceTask) msgType() commonpb.MsgType {
	return lbt.Base.MsgType
}

func (lbt *loadBalanceTask) timestamp() Timestamp {
	ret := lbt.Base.Timestamp
	if ret == 0 {
		ret = uint64(lbt.getTaskID())
	}
	return ret
}
