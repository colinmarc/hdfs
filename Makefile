HADOOP_COMMON_PROTOS = $(shell find protocol/hadoop_common -name '*.proto')
HADOOP_HDFS_PROTOS = $(shell find protocol/hadoop_hdfs -name '*.proto')
GENERATED_PROTOS = $(shell echo "$(HADOOP_HDFS_PROTOS) $(HADOOP_COMMON_PROTOS)" | sed 's/\.proto/\.pb\.go/g')
SOURCES = $(shell find . -name '*.go') $(GENERATED_PROTOS)

# Protobuf needs one of these for every 'import "foo.proto"' in .protoc files.
PROTO_MAPPING = MSecurity.proto=github.com/colinmarc/hdfs/protocol/hadoop_common

all: hdfs

%.pb.go: $(HADOOP_HDFS_PROTOS) $(HADOOP_COMMON_PROTOS)
	protoc --go_out='$(PROTO_MAPPING):protocol/hadoop_common' -Iprotocol/hadoop_common -Iprotocol/hadoop_hdfs $(HADOOP_COMMON_PROTOS)
	protoc --go_out='$(PROTO_MAPPING):protocol/hadoop_hdfs' -Iprotocol/hadoop_common -Iprotocol/hadoop_hdfs $(HADOOP_HDFS_PROTOS)

clean-protos:
	find . -name *.pb.go | xargs rm

hdfs: clean $(SOURCES)
	go build ./cmd/hdfs

install: get-deps
	go install ./...

test: hdfs
	go test -v -race $(shell go list ./... | grep -v vendor)
	bats ./cmd/hdfs/test/*.bats

clean:
	rm -f ./hdfs

.PHONY: clean clean-protos install test
