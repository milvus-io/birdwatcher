#!/usr/bin/env bash
SCRIPTS_DIR=$(dirname "$0")

PROTO_DIR=$SCRIPTS_DIR MILVUS_PROTO_DIR=$SCRIPTS_DIR

GOOGLE_PROTO_DIR="/home/sunby/Documents/milvus/cmake_build/thirdparty/protobuf/protobuf-src/src"

PROGRAM=$(basename "$0")
GOPATH=$(go env GOPATH)


if [ -z $GOPATH ]; then
    printf "Error: GOPATH cannot be found, please set it before running this script"
    exit 1
fi

case ":$PATH:" in
    *":$GOPATH/bin:"*) ;;
    *) export PATH="$GOPATH/bin:$PATH";;
esac

echo "using protoc-gen-go: $(which protoc-gen-go)"

mkdir -p commonpb
mkdir -p milvuspb
mkdir -p schemapb
mkdir -p internalpb
mkdir -p etcdpb
mkdir -p datapb
mkdir -p indexpb
mkdir -p rootcoordpb
mkdir -p querypb
mkdir -p proxypb
mkdir -p msgpb
mkdir -p federpb

protoc=/home/sunby/Documents/milvus/cmake_build/bin/protoc
# milvus.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/milvus-io/birdwatcher/proto/v2.2/milvuspb" \
    --go_opt="Mcommon.proto=github.com/milvus-io/birdwatcher/proto/v2.2/commonpb" \
    --go_opt="Mschema.proto=github.com/milvus-io/birdwatcher/proto/v2.2/schemapb" \
    --go_opt="Mmsg.proto=github.com/milvus-io/birdwatcher/proto/v2.2/msgpb" \
    --go_opt="Mfeder.proto=github.com/milvus-io/birdwatcher/proto/v2.2/federpb" \
    --go_out=plugins=grpc,paths=source_relative:milvuspb milvus.proto
# feder.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/milvus-io/birdwatcher/proto/v2.2/milvuspb" \
    --go_opt="Mcommon.proto=github.com/milvus-io/birdwatcher/proto/v2.2/commonpb" \
    --go_out=plugins=grpc,paths=source_relative:federpb feder.proto
# schema.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/milvus-io/birdwatcher/proto/v2.2/milvuspb" \
    --go_opt="Mcommon.proto=github.com/milvus-io/birdwatcher/proto/v2.2/commonpb" \
    --go_opt="Mschema.proto=github.com/milvus-io/birdwatcher/proto/v2.2/schemapb" \
    --go_out=plugins=grpc,paths=source_relative:schemapb schema.proto
# common.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/milvus-io/birdwatcher/proto/v2.2/milvuspb" \
    --go_opt="Mcommon.proto=github.com/milvus-io/birdwatcher/proto/v2.2/commonpb" \
    --go_opt="Mschema.proto=github.com/milvus-io/birdwatcher/proto/v2.2/schemapb" \
    --go_out=plugins=grpc,paths=source_relative:commonpb common.proto
# msg.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mcommon.proto=github.com/milvus-io/birdwatcher/proto/v2.2/commonpb" \
    --go_opt="Mschema.proto=github.com/milvus-io/birdwatcher/proto/v2.2/schemapb" \
    --go_out=plugins=grpc,paths=source_relative:msgpb msg.proto
# internal.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/milvus-io/birdwatcher/proto/v2.2/milvuspb" \
    --go_opt="Mcommon.proto=github.com/milvus-io/birdwatcher/proto/v2.2/commonpb" \
    --go_opt="Mschema.proto=github.com/milvus-io/birdwatcher/proto/v2.2/schemapb" \
    --go_out=plugins=grpc,paths=source_relative:internalpb internal.proto
# etcd_meta.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/milvus-io/birdwatcher/proto/v2.2/milvuspb" \
    --go_opt="Mcommon.proto=github.com/milvus-io/birdwatcher/proto/v2.2/commonpb" \
    --go_opt="Mschema.proto=github.com/milvus-io/birdwatcher/proto/v2.2/schemapb" \
    --go_out=plugins=grpc,paths=source_relative:etcdpb etcd_meta.proto
# root_coord.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/milvus-io/birdwatcher/proto/v2.2/milvuspb" \
    --go_opt="Mcommon.proto=github.com/milvus-io/birdwatcher/proto/v2.2/commonpb" \
    --go_opt="Mschema.proto=github.com/milvus-io/birdwatcher/proto/v2.2/schemapb" \
    --go_opt="Minternal.proto=github.com/milvus-io/birdwatcher/proto/v2.2/internalpb" \
    --go_opt="Metcd_meta.proto=github.com/milvus-io/birdwatcher/proto/v2.2/etcdpb" \
    --go_opt="Mdata_coord.proto=github.com/milvus-io/birdwatcher/proto/v2.2/datapb" \
    --go_opt="Mindex_coord.proto=github.com/milvus-io/birdwatcher/proto/v2.2/indexpb" \
    --go_opt="Mproxy.proto=github.com/milvus-io/birdwatcher/proto/v2.2/proxypb" \
    --go_out=plugins=grpc,paths=source_relative:rootcoordpb root_coord.proto
# data_coord.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/milvus-io/birdwatcher/proto/v2.2/milvuspb" \
    --go_opt="Mcommon.proto=github.com/milvus-io/birdwatcher/proto/v2.2/commonpb" \
    --go_opt="Mschema.proto=github.com/milvus-io/birdwatcher/proto/v2.2/schemapb" \
    --go_opt="Minternal.proto=github.com/milvus-io/birdwatcher/proto/v2.2/internalpb" \
    --go_opt="Mmsg.proto=github.com/milvus-io/birdwatcher/proto/v2.2/msgpb" \
    --go_opt="Mindex_coord.proto=github.com/milvus-io/birdwatcher/proto/v2.2/indexpb" \
    --go_out=plugins=grpc,paths=source_relative:datapb data_coord.proto
# query_coord.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/milvus-io/birdwatcher/proto/v2.2/milvuspb" \
    --go_opt="Mcommon.proto=github.com/milvus-io/birdwatcher/proto/v2.2/commonpb" \
    --go_opt="Mschema.proto=github.com/milvus-io/birdwatcher/proto/v2.2/schemapb" \
    --go_opt="Minternal.proto=github.com/milvus-io/birdwatcher/proto/v2.2/internalpb" \
    --go_opt="Mdata_coord.proto=github.com/milvus-io/birdwatcher/proto/v2.2/datapb" \
    --go_opt="Mmsg.proto=github.com/milvus-io/birdwatcher/proto/v2.2/msgpb" \
    --go_opt="Mindex_coord.proto=github.com/milvus-io/birdwatcher/proto/v2.2/indexpb" \
    --go_out=plugins=grpc,paths=source_relative:querypb query_coord.proto
# index_coord.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/milvus-io/birdwatcher/proto/v2.2/milvuspb" \
    --go_opt="Mcommon.proto=github.com/milvus-io/birdwatcher/proto/v2.2/commonpb" \
    --go_opt="Mschema.proto=github.com/milvus-io/birdwatcher/proto/v2.2/schemapb" \
    --go_opt="Minternal.proto=github.com/milvus-io/birdwatcher/proto/v2.2/internalpb" \
    --go_out=plugins=grpc,paths=source_relative:indexpb index_coord.proto
# proxy.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/milvus-io/birdwatcher/proto/v2.2/milvuspb" \
    --go_opt="Mcommon.proto=github.com/milvus-io/birdwatcher/proto/v2.2/commonpb" \
    --go_opt="Mschema.proto=github.com/milvus-io/birdwatcher/proto/v2.2/schemapb" \
    --go_opt="Minternal.proto=github.com/milvus-io/birdwatcher/proto/v2.2/internalpb" \
    --go_out=plugins=grpc,paths=source_relative:proxypb proxy.proto

sed -i "s/\/milvus.protov2.data/\/milvus.proto.data/g" datapb/data_coord.pb.go
sed -i "s/\/milvus.protov2.milvus/\/milvus.proto.milvus/g" milvuspb/milvus.pb.go
sed -i "s/\/milvus.protov2.query/\/milvus.proto.query/g" querypb/query_coord.pb.go
sed -i "s/\/milvus.protov2.index/\/milvus.proto.index/g" indexpb/index_coord.pb.go
sed -i "s/\/milvus.protov2.rootcoord/\/milvus.proto.rootcoord/g" rootcoordpb/root_coord.pb.go

