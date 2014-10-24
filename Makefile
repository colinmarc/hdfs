GOCMD ?= $(shell which go)

all: hdfs

hdfs: get-deps
	$(GOCMD) build ./...

install: get-deps
	$(GOCMD) install ./...

test: get-deps
	$(GOCMD) test ./...

get-deps:
	$(GOCMD) get code.google.com/p/goprotobuf/proto
	$(GOCMD) get code.google.com/p/getopt
	$(GOCMD) get github.com/stretchr/testify/assert
	$(GOCMD) get github.com/stretchr/testify/require

.PHONY: install test get-deps
