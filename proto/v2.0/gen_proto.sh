#!/usr/bin/env bash
SCRIPTS_DIR=$(dirname "$0")

PROTO_DIR=$SCRIPTS_DIR
MILVUS_PROTO_DIR=$SCRIPTS_DIR

GOOGLE_PROTO_DIR="/home/silverxia/workspace/milvus/cmake_build/thirdparty/protobuf/protobuf-src/src"

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

# milvus.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/congqixia/birdwatcher/proto/v2.0/milvuspb" \
    --go_opt="Mcommon.proto=github.com/congqixia/birdwatcher/proto/v2.0/commonpb" \
    --go_opt="Mschema.proto=github.com/congqixia/birdwatcher/proto/v2.0/schemapb" \
    --go_out=plugins=grpc,paths=source_relative:milvuspb milvus.proto
# schema.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/congqixia/birdwatcher/proto/v2.0/milvuspb" \
    --go_opt="Mcommon.proto=github.com/congqixia/birdwatcher/proto/v2.0/commonpb" \
    --go_opt="Mschema.proto=github.com/congqixia/birdwatcher/proto/v2.0/schemapb" \
    --go_out=plugins=grpc,paths=source_relative:schemapb schema.proto
# common.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/congqixia/birdwatcher/proto/v2.0/milvuspb" \
    --go_opt="Mcommon.proto=github.com/congqixia/birdwatcher/proto/v2.0/commonpb" \
    --go_opt="Mschema.proto=github.com/congqixia/birdwatcher/proto/v2.0/schemapb" \
    --go_out=plugins=grpc,paths=source_relative:commonpb common.proto
# internal.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/congqixia/birdwatcher/proto/v2.0/milvuspb" \
    --go_opt="Mcommon.proto=github.com/congqixia/birdwatcher/proto/v2.0/commonpb" \
    --go_opt="Mschema.proto=github.com/congqixia/birdwatcher/proto/v2.0/schemapb" \
    --go_out=plugins=grpc,paths=source_relative:internalpb internal.proto
# etcd_meta.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/congqixia/birdwatcher/proto/v2.0/milvuspb" \
    --go_opt="Mcommon.proto=github.com/congqixia/birdwatcher/proto/v2.0/commonpb" \
    --go_opt="Mschema.proto=github.com/congqixia/birdwatcher/proto/v2.0/schemapb" \
    --go_out=plugins=grpc,paths=source_relative:etcdpb etcd_meta.proto
# root_coord.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/congqixia/birdwatcher/proto/v2.0/milvuspb" \
    --go_opt="Mcommon.proto=github.com/congqixia/birdwatcher/proto/v2.0/commonpb" \
    --go_opt="Mschema.proto=github.com/congqixia/birdwatcher/proto/v2.0/schemapb" \
    --go_opt="Minternal.proto=github.com/congqixia/birdwatcher/proto/v2.0/internalpb" \
    --go_out=plugins=grpc,paths=source_relative:rootcoordpb root_coord.proto
# data_coord.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/congqixia/birdwatcher/proto/v2.0/milvuspb" \
    --go_opt="Mcommon.proto=github.com/congqixia/birdwatcher/proto/v2.0/commonpb" \
    --go_opt="Mschema.proto=github.com/congqixia/birdwatcher/proto/v2.0/schemapb" \
    --go_opt="Minternal.proto=github.com/congqixia/birdwatcher/proto/v2.0/internalpb" \
    --go_out=plugins=grpc,paths=source_relative:datapb data_coord.proto
# query_coord.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/congqixia/birdwatcher/proto/v2.0/milvuspb" \
    --go_opt="Mcommon.proto=github.com/congqixia/birdwatcher/proto/v2.0/commonpb" \
    --go_opt="Mschema.proto=github.com/congqixia/birdwatcher/proto/v2.0/schemapb" \
    --go_opt="Minternal.proto=github.com/congqixia/birdwatcher/proto/v2.0/internalpb" \
    --go_out=plugins=grpc,paths=source_relative:querypb query_coord.proto
# index_coord.proto
${protoc} --proto_path=${MILVUS_PROTO_DIR} --proto_path=${GOOGLE_PROTO_DIR}\
    --go_opt="Mmilvus.proto=github.com/congqixia/birdwatcher/proto/v2.0/milvuspb" \
    --go_opt="Mcommon.proto=github.com/congqixia/birdwatcher/proto/v2.0/commonpb" \
    --go_opt="Mschema.proto=github.com/congqixia/birdwatcher/proto/v2.0/schemapb" \
    --go_opt="Minternal.proto=github.com/congqixia/birdwatcher/proto/v2.0/internalpb" \
    --go_out=plugins=grpc,paths=source_relative:indexpb index_coord.proto
