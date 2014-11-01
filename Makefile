GOCMD ?= $(shell which go)

all: hdfs

hdfs: get-deps clean
	$(GOCMD) build ./...
	$(GOCMD) build ./cmd/hdfs

install: get-deps
	$(GOCMD) install ./...

test: hdfs
	$(GOCMD) test ./...
	bats ./cmd/hdfs/test/*.bats

clean:
	rm -f ./hdfs

get-deps:
	$(GOCMD) get code.google.com/p/goprotobuf/proto
	$(GOCMD) get code.google.com/p/getopt
	$(GOCMD) get github.com/stretchr/testify/assert
	$(GOCMD) get github.com/stretchr/testify/require

.PHONY: install test get-deps
