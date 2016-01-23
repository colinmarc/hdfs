THIS_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
PROJECT_NAME=github.com/colinmarc/hdfs
PROJECT_GOPATH_DIR=$(GOPATH)/src/$(PROJECT_NAME)

all: hdfs

hdfs: get-deps clean $(PROJECT_GOPATH_DIR)
	go build ./cmd/hdfs

$(PROJECT_GOPATH_DIR):
	# this symlink workaround is needed to support building of non-official forks of the project
	mkdir -p $(shell dirname $(PROJECT_GOPATH_DIR))
	ln -s $(THIS_DIR) $(PROJECT_GOPATH_DIR)

install: get-deps
	go install ./...

test: hdfs
	go test -v ./...
	bats ./cmd/hdfs/test/*.bats

clean:
	rm -f ./hdfs

get-deps:
	go get github.com/golang/protobuf/proto
	go get github.com/pborman/getopt
	go get github.com/stretchr/testify/assert
	go get github.com/stretchr/testify/require

.PHONY: install test get-deps
