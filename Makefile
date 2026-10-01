# The single entry point for every implementation. Each language directory
# has a Makefile with build, test and clean targets, and this file fans out
# to them in parallel.
#
# go-toolchain at the root also runs every nested Go module, which includes
# interop/ and codegen/go/. Those need the C, C++ and Python peers and the
# generated code, so the root Go run comes last. ipcgen is its own module
# and builds first, because every language's generated code comes from it.
MAKEFLAGS += -j$(shell nproc) --output-sync=target

ROOT := $(CURDIR)
BUILD := $(ROOT)/build
GEN := $(BUILD)/gen
IPCGEN := $(ROOT)/ipcgen/build/ipcgen
SCHEMA := $(ROOT)/spec/vectors/schema/example.ipc

export GOIPC_SPEC_DIR := $(ROOT)/spec
export GOIPC_LIBRARY := $(ROOT)/c/build/libgoipc.so
export GOIPC_PEER_C := $(ROOT)/c/build/goipc-peer
export GOIPC_PEER_CPP := $(ROOT)/cpp/build/goipc-peer
export GOIPC_PEER_PY := python3 -m goipc.peer
export PYTHONPATH := $(ROOT)/python:$(GEN)/py

SUBMAKE = $(MAKE) GEN_DIR=$(GEN) IPCGEN=$(IPCGEN)

.PHONY: all build test native clean gen ipcgen prepare-go \
	build-c build-cpp \
	test-go test-c test-cpp test-py test-codegen

all: build

build: build-c build-cpp prepare-go

# native is everything except the root Go run. CI runs it, then hands the
# root run to the go-toolchain action.
native: build test-c test-cpp test-py test-codegen

test: native
	$(MAKE) test-go

# This run covers the root package, ipcgen, codegen/go and interop/.
test-go:
	go-toolchain --no-benchmark

ipcgen $(IPCGEN):
	cd ipcgen && go-toolchain --no-benchmark

gen: $(IPCGEN)
	mkdir -p $(GEN)/go/demo $(GEN)/c $(GEN)/cpp $(GEN)/py
	$(IPCGEN) --lang go --out $(GEN)/go/demo/demo.go $(SCHEMA)
	$(IPCGEN) --lang c --out $(GEN)/c/demo.h $(SCHEMA)
	$(IPCGEN) --lang cpp --out $(GEN)/cpp/demo.hpp $(SCHEMA)
	$(IPCGEN) --lang py --out $(GEN)/py/demo.py $(SCHEMA)

# The Go modules that use generated code take a copy, which git ignores.
prepare-go: gen
	+$(SUBMAKE) -C codegen prepare
	+$(SUBMAKE) -C interop prepare

build-c: gen
	+$(SUBMAKE) -C c build

build-cpp: gen
	+$(SUBMAKE) -C cpp build

# Sub-makes in one directory race on the same outputs, so each test runs
# after the build in its directory.
test-c: build-c
	+$(SUBMAKE) -C c test

test-cpp: build-cpp
	+$(SUBMAKE) -C cpp test

test-py: build-c gen
	+$(SUBMAKE) -C python test

test-codegen: gen
	+$(SUBMAKE) -C codegen test

clean:
	$(MAKE) -C c clean
	$(MAKE) -C cpp clean
	$(MAKE) -C python clean
	$(MAKE) -C codegen clean
	$(MAKE) -C interop clean
	rm -rf $(BUILD) $(ROOT)/ipcgen/build
