package states

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

type FlushParam struct {
	framework.ExecutionParam `use:"flush" desc:"manual flush collection, supports both legacy and streaming (2.6+) mode"`
	CollectionID             int64  `name:"collection" default:"0" desc:"collection id to flush"`
	CollectionName           string `name:"collectionName" default:"" desc:"collection name to flush"`
}

func (s *InstanceState) FlushCommand(ctx context.Context, p *FlushParam) error {
	// resolve collection
	collection, err := s.resolveCollection(ctx, p.CollectionID, p.CollectionName)
	if err != nil {
		return err
	}

	collID := collection.GetProto().GetID()
	collName := collection.GetProto().GetSchema().GetName()
	vchannels := collection.GetProto().GetVirtualChannelNames()

	fmt.Printf("Collection: %s (ID: %d)\n", collName, collID)
	fmt.Printf("VChannels: %v\n", vchannels)

	// check if streaming mode is enabled by looking for WAL distribution metadata
	isStreaming := s.isStreamingMode(ctx, vchannels)
	if isStreaming {
		fmt.Println("Mode: streaming (2.6+)")
	} else {
		fmt.Println("Mode: legacy")
	}

	if !p.Run {
		fmt.Println("Dry run, use --run to actually flush")
		return nil
	}

	// for streaming mode, send ManualFlush message to WAL via StreamingNode
	if isStreaming {
		if err := s.streamingFlush(ctx, collID, vchannels); err != nil {
			return err
		}
	}

	// call DataCoord Flush RPC
	return s.dataCoordFlush(ctx, collID)
}

// resolveCollection resolves collection by ID or name.
func (s *InstanceState) resolveCollection(ctx context.Context, collID int64, collName string) (*models.Collection, error) {
	if collID <= 0 && collName == "" {
		return nil, fmt.Errorf("either --collection or --collectionName must be provided")
	}

	collections, err := common.ListCollections(ctx, s.client, s.basePath)
	if err != nil {
		return nil, err
	}

	for _, col := range collections {
		if collID > 0 && col.GetProto().GetID() == collID {
			return col, nil
		}
		if collName != "" && col.GetProto().GetSchema().GetName() == collName {
			return col, nil
		}
	}

	if collID > 0 {
		return nil, fmt.Errorf("collection with ID %d not found", collID)
	}
	return nil, fmt.Errorf("collection with name %q not found", collName)
}

// isStreamingMode checks whether the cluster uses streaming mode by looking for WAL distribution metadata.
func (s *InstanceState) isStreamingMode(ctx context.Context, vchannels []string) bool {
	if len(vchannels) == 0 {
		return false
	}
	pchannel := funcutil.ToPhysicalChannel(vchannels[0])
	metas, err := common.ListWALDistribution(ctx, s.client, s.basePath, pchannel)
	if err != nil || len(metas) == 0 {
		return false
	}
	return true
}

// streamingFlush sends ManualFlush messages to StreamingNode for each vchannel.
func (s *InstanceState) streamingFlush(ctx context.Context, collID int64, vchannels []string) error {
	// group vchannels by pchannel
	pchannelMap := make(map[string][]string) // pchannel -> vchannels
	for _, vchannel := range vchannels {
		pchannel := funcutil.ToPhysicalChannel(vchannel)
		pchannelMap[pchannel] = append(pchannelMap[pchannel], vchannel)
	}

	// allocate a flush timestamp using current time
	flushTs := tsoutil.GetCurrentTime()

	fmt.Printf("FlushTs: %d (time: %v)\n", flushTs, tsoutil.PhysicalTime(flushTs))

	for pchannel, vchs := range pchannelMap {
		// find which StreamingNode owns this pchannel
		metas, err := common.ListWALDistribution(ctx, s.client, s.basePath, pchannel)
		if err != nil {
			return errors.Wrapf(err, "failed to get WAL distribution for pchannel %s", pchannel)
		}
		if len(metas) == 0 {
			return fmt.Errorf("no WAL distribution found for pchannel %s", pchannel)
		}
		meta := metas[0]
		if meta.GetNode() == nil {
			return fmt.Errorf("pchannel %s has no assigned streaming node", pchannel)
		}

		nodeAddr := meta.GetNode().GetAddress()
		nodeID := meta.GetNode().GetServerId()
		term := meta.GetChannel().GetTerm()

		// if address is empty, look up from sessions
		if nodeAddr == "" {
			sessions, err := common.ListSessions(ctx, s.client, s.basePath)
			if err != nil {
				return errors.Wrap(err, "failed to list sessions")
			}
			for _, sess := range sessions {
				if sess.ServerID == nodeID {
					nodeAddr = sess.Address
					break
				}
			}
		}

		if nodeAddr == "" {
			return fmt.Errorf("cannot resolve address for streaming node %d", nodeID)
		}

		fmt.Printf("Connecting to StreamingNode %d (%s) for pchannel %s (term=%d)\n", nodeID, nodeAddr, pchannel, term)

		for _, vchannel := range vchs {
			segmentIDs, err := s.sendManualFlushToStreamingNode(ctx, nodeAddr, pchannel, term, collID, vchannel, flushTs)
			if err != nil {
				return errors.Wrapf(err, "failed to send manual flush for vchannel %s", vchannel)
			}
			fmt.Printf("  ManualFlush sent to vchannel %s, sealed segments: %v\n", vchannel, segmentIDs)
		}
	}

	return nil
}

// sendManualFlushToStreamingNode sends a ManualFlush message to a StreamingNode via the Produce gRPC stream.
func (s *InstanceState) sendManualFlushToStreamingNode(ctx context.Context, nodeAddr, pchannel string, term, collID int64, vchannel string, flushTs uint64) ([]int64, error) {
	// build the ManualFlush message
	flushMsg, err := message.NewManualFlushMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.ManualFlushMessageHeader{
			CollectionId: collID,
			FlushTs:      flushTs,
		}).
		WithBody(&message.ManualFlushMessageBody{}).
		BuildMutable()
	if err != nil {
		return nil, errors.Wrap(err, "failed to build manual flush message")
	}

	// set barrier time tick
	flushMsg = flushMsg.WithBarrierTimeTick(flushTs)

	// connect to StreamingNode
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}
	conn, err := grpc.DialContext(ctx, nodeAddr, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect to streaming node at %s", nodeAddr)
	}
	defer conn.Close()

	client := streamingpb.NewStreamingNodeHandlerServiceClient(conn)

	// attach CreateProducerRequest to context via gRPC metadata
	createReq := &streamingpb.CreateProducerRequest{
		Pchannel: &streamingpb.PChannelInfo{
			Name: pchannel,
			Term: term,
		},
	}
	reqBytes, err := proto.Marshal(createReq)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal create producer request")
	}
	produceCtx := metadata.AppendToOutgoingContext(ctx, "create-producer", base64.StdEncoding.EncodeToString(reqBytes))

	// open Produce stream
	stream, err := client.Produce(produceCtx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open produce stream")
	}

	// recv CreateProducerResponse
	resp, err := stream.Recv()
	if err != nil {
		return nil, errors.Wrap(err, "failed to receive create producer response")
	}
	createResp := resp.GetCreate()
	if createResp == nil {
		return nil, fmt.Errorf("unexpected first response type: %T", resp.GetResponse())
	}

	// send the ManualFlush message
	requestID := int64(1)
	if err := stream.Send(&streamingpb.ProduceRequest{
		Request: &streamingpb.ProduceRequest_Produce{
			Produce: &streamingpb.ProduceMessageRequest{
				RequestId: requestID,
				Message:   flushMsg.IntoMessageProto(),
			},
		},
	}); err != nil {
		return nil, errors.Wrap(err, "failed to send manual flush message")
	}

	// recv the produce response
	produceResp, err := stream.Recv()
	if err != nil {
		return nil, errors.Wrap(err, "failed to receive produce response")
	}

	msgResp := produceResp.GetProduce()
	if msgResp == nil {
		return nil, fmt.Errorf("unexpected response type: %T", produceResp.GetResponse())
	}
	if msgResp.GetRequestId() != requestID {
		return nil, fmt.Errorf("request id mismatch: expected %d, got %d", requestID, msgResp.GetRequestId())
	}

	result := msgResp.GetResult()
	if result == nil {
		if errResp := msgResp.GetError(); errResp != nil {
			return nil, fmt.Errorf("produce error: code=%d, cause=%s", errResp.GetCode(), errResp.GetCause())
		}
		return nil, fmt.Errorf("unexpected produce response without result")
	}

	// extract ManualFlushExtraResponse from extra field
	var segmentIDs []int64
	if result.GetExtra() != nil {
		var flushExtra messagespb.ManualFlushExtraResponse
		if err := anypb.UnmarshalTo(result.GetExtra(), &flushExtra, proto.UnmarshalOptions{
			DiscardUnknown: true,
			AllowPartial:   true,
		}); err != nil {
			fmt.Printf("  Warning: failed to unmarshal flush extra response: %v\n", err)
		} else {
			segmentIDs = flushExtra.GetSegmentIds()
		}
	}

	// send close and ignore errors
	_ = stream.Send(&streamingpb.ProduceRequest{
		Request: &streamingpb.ProduceRequest_Close{
			Close: &streamingpb.CloseProducerRequest{},
		},
	})
	_ = stream.CloseSend()

	return segmentIDs, nil
}

// dataCoordFlush connects to DataCoord and calls the Flush RPC.
func (s *InstanceState) dataCoordFlush(ctx context.Context, collID int64) error {
	sessions, err := common.ListSessions(ctx, s.client, s.basePath)
	if err != nil {
		return err
	}

	session := lo.FindOrElse(sessions, nil, func(session *models.Session) bool {
		return session.ServerName == "datacoord" || session.ServerName == "mixcoord"
	})

	if session == nil {
		return fmt.Errorf("datacoord/mixcoord session not found")
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}

	fmt.Printf("Connecting to DataCoord(%d) at %s\n", session.ServerID, session.Address)

	conn, err := grpc.DialContext(ctx, session.Address, opts...)
	if err != nil {
		return errors.Wrapf(err, "failed to connect to datacoord at %s", session.Address)
	}
	defer conn.Close()

	client := datapb.NewDataCoordClient(conn)
	resp, err := client.Flush(ctx, &datapb.FlushRequest{
		CollectionID: collID,
	})
	if err != nil {
		return errors.Wrapf(err, "datacoord flush RPC failed")
	}

	fmt.Printf("DataCoord Flush response:\n")
	fmt.Printf("  Status: %v\n", resp.GetStatus())
	fmt.Printf("  SegmentIDs (sealed): %v\n", resp.GetSegmentIDs())
	fmt.Printf("  FlushSegmentIDs (already flushed): %v\n", resp.GetFlushSegmentIDs())
	if resp.GetFlushTs() > 0 {
		fmt.Printf("  FlushTs: %d (time: %v)\n", resp.GetFlushTs(), tsoutil.PhysicalTime(resp.GetFlushTs()))
	}
	if len(resp.GetChannelCps()) > 0 {
		fmt.Println("  Channel checkpoints:")
		for ch, cp := range resp.GetChannelCps() {
			if cp != nil {
				fmt.Printf("    %s: ts=%d\n", ch, cp.GetTimestamp())
			}
		}
	}

	return nil
}
