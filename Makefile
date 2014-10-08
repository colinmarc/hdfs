GOCMD ?= $(shell which go)

all: test

install: get-deps
	$(GOCMD) install

test: get-deps
	$(GOCMD) test

get-deps:
	$(GOCMD) get github.com/stretchr/testify/assert
	$(GOCMD) get code.google.com/p/goprotobuf/proto

.PHONY: install test get-deps
