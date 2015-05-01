all: hdfs

hdfs: get-deps clean
	go build ./cmd/hdfs

install: get-deps
	go install ./...

test: hdfs
	go test ./...
	bats ./cmd/hdfs/test/*.bats

clean:
	rm -f ./hdfs

get-deps:
	go get github.com/golang/protobuf/proto
	go get code.google.com/p/getopt
	go get github.com/stretchr/testify/assert
	go get github.com/stretchr/testify/require

.PHONY: install test get-deps
