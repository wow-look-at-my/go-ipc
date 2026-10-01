# C++ implementation of go-ipc. Targets: build, test, clean.
SPEC_DIR := $(abspath ../spec)
B := build
GEN := $(B)/gen

CXXFLAGS_COMMON := -std=c++20 -Wall -Wextra -Werror -O2 -pthread -Iinclude
GTEST_CFLAGS := $(shell pkg-config --cflags gtest_main)
GTEST_LIBS := $(shell pkg-config --libs gtest_main)
TEST_FLAGS := $(GTEST_CFLAGS) -I$(GEN) -I.. -DGOIPC_SPEC_DIR='"$(SPEC_DIR)"' -DGOIPC_PEER_BIN='"$(abspath $(B)/goipc-peer)"'
ASAN_FLAGS := -g -fsanitize=address,undefined -fno-sanitize-recover=all -fno-omit-frame-pointer
TSAN_FLAGS := -g -fsanitize=thread

HEADERS := $(wildcard include/goipc/*.hpp)
TEST_SRCS := $(wildcard tests/*_test.cpp)
TEST_DEPS := $(HEADERS) tests/helpers.hpp $(GEN)/spec_wire.hpp $(GEN)/spec_vectors.hpp ../c/include/goipc.h
# The TSan build covers the threaded tests. TSan does not support fork from a
# threaded process, so the fork tests stay in the other builds.
TSAN_SRCS := tests/concurrency_test.cpp

.PHONY: all build test clean

all: build

build: $(B)/goipc-peer

$(B)/goipc-peer: peer/peer.cpp $(HEADERS)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS_COMMON) -o $@ $<

$(GEN)/spec_wire.hpp: $(SPEC_DIR)/wire.json tests/wire.jq
	@mkdir -p $(@D)
	jq -r -f tests/wire.jq $< > $@.tmp
	mv $@.tmp $@

$(GEN)/spec_vectors.hpp: $(SPEC_DIR)/vectors/ring/manifest.json tests/vectors.jq
	@mkdir -p $(@D)
	jq -r -f tests/vectors.jq $< > $@.tmp
	mv $@.tmp $@

$(B)/obj/unit/%.o: tests/%.cpp $(TEST_DEPS)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS_COMMON) $(TEST_FLAGS) -c -o $@ $<

$(B)/obj/asan/%.o: tests/%.cpp $(TEST_DEPS)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS_COMMON) $(ASAN_FLAGS) $(TEST_FLAGS) -c -o $@ $<

$(B)/obj/tsan/%.o: tests/%.cpp $(TEST_DEPS)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS_COMMON) $(TSAN_FLAGS) $(TEST_FLAGS) -c -o $@ $<

$(B)/unit_tests: $(TEST_SRCS:tests/%.cpp=$(B)/obj/unit/%.o)
	$(CXX) $(CXXFLAGS_COMMON) -o $@ $^ $(GTEST_LIBS)

$(B)/asan_tests: $(TEST_SRCS:tests/%.cpp=$(B)/obj/asan/%.o)
	$(CXX) $(CXXFLAGS_COMMON) $(ASAN_FLAGS) -o $@ $^ $(GTEST_LIBS)

$(B)/tsan_tests: $(TSAN_SRCS:tests/%.cpp=$(B)/obj/tsan/%.o)
	$(CXX) $(CXXFLAGS_COMMON) $(TSAN_FLAGS) -o $@ $^ $(GTEST_LIBS)

# The runs are sequential so that the CPU budget test measures an idle machine.
test: $(B)/goipc-peer $(B)/unit_tests $(B)/asan_tests $(B)/tsan_tests
	GOIPC_SPEC_DIR=$(SPEC_DIR) $(B)/unit_tests
	GOIPC_SPEC_DIR=$(SPEC_DIR) ASAN_OPTIONS=detect_leaks=1 $(B)/asan_tests
	TSAN_OPTIONS=halt_on_error=1 $(B)/tsan_tests

clean:
	rm -rf $(B)
