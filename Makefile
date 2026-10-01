# The single entry point for every implementation. Each language directory
# has a Makefile with build, test and clean targets, and this file fans out
# to them in parallel.
MAKEFLAGS += -j$(shell nproc) --output-sync=target

ROOT := $(CURDIR)
BUILD := $(ROOT)/build
GEN := $(BUILD)/gen
IPCGEN := $(BUILD)/ipcgen
GO_STAMP := $(BUILD)/.go-toolchain.stamp
SCHEMA := $(ROOT)/spec/vectors/schema/example.ipc

export GOIPC_SPEC_DIR := $(ROOT)/spec
export GOIPC_LIBRARY := $(ROOT)/c/build/libgoipc.so
export GOIPC_PEER_C := $(ROOT)/c/build/goipc-peer
export GOIPC_PEER_CPP := $(ROOT)/cpp/build/goipc-peer
export GOIPC_PEER_PY := python3 -m goipc.peer
export PYTHONPATH := $(ROOT)/python:$(GEN)/py

SUBMAKE = $(MAKE) GEN_DIR=$(GEN) IPCGEN=$(IPCGEN)

.PHONY: all build test clean gen \
	build-c build-cpp \
	test-go test-c test-cpp test-py test-codegen test-interop

all: build

build: $(GO_STAMP) build-c build-cpp

test: test-go test-c test-cpp test-py test-codegen test-interop

# go-toolchain tests the root module and builds build/ipcgen in the same run.
$(GO_STAMP): $(shell find $(ROOT) -maxdepth 3 -name '*.go' -not -path '*/interop/*' -not -path '*/codegen/*') go.mod spec/wire.json
	go-toolchain --no-benchmark
	touch $@

test-go: $(GO_STAMP)

gen: $(GO_STAMP)
	mkdir -p $(GEN)/go/demo $(GEN)/c $(GEN)/cpp $(GEN)/py
	$(IPCGEN) --lang go --out $(GEN)/go/demo/demo.go $(SCHEMA)
	$(IPCGEN) --lang c --out $(GEN)/c/demo.h $(SCHEMA)
	$(IPCGEN) --lang cpp --out $(GEN)/cpp/demo.hpp $(SCHEMA)
	$(IPCGEN) --lang py --out $(GEN)/py/demo.py $(SCHEMA)

build-c:
	$(SUBMAKE) -C c build

build-cpp:
	$(SUBMAKE) -C cpp build

test-c:
	$(SUBMAKE) -C c test

test-cpp:
	$(SUBMAKE) -C cpp test

test-py: build-c
	$(SUBMAKE) -C python test

test-codegen: gen
	$(SUBMAKE) -C codegen test

test-interop: build-c build-cpp gen
	$(SUBMAKE) -C interop test

clean:
	$(MAKE) -C c clean
	$(MAKE) -C cpp clean
	$(MAKE) -C python clean
	$(MAKE) -C codegen clean
	$(MAKE) -C interop clean
	rm -rf $(BUILD)
