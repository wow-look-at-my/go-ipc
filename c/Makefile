# libgoipc, goipc-peer and the C test suite. Targets: build, test, clean.
CC ?= cc
AR ?= ar
JQ ?= jq

ROOT := $(abspath .)
SPEC := $(abspath ../spec)
B := $(ROOT)/build
GEN := $(B)/gen

# The typed peer roles use the ipcgen output for spec/vectors/schema/example.ipc.
# The top-level Makefile passes both; these defaults are the paths it uses.
GEN_DIR ?= $(abspath ../build/gen)
IPCGEN ?= $(abspath ../ipcgen/build/ipcgen)
SCHEMA_DIR := $(SPEC)/vectors/schema
PEER_GEN := $(B)/peer

SRC := $(wildcard src/*.c)
TEST_SRC := $(wildcard tests/*.c)
HEADERS := $(wildcard include/*.h src/*.h tests/*.h) $(GEN)/spec_wire.h

WARN := -std=c11 -Wall -Wextra -Werror -pthread
CPPFLAGS := -Iinclude -Isrc -I$(GEN) -D_DEFAULT_SOURCE \
	-DGOIPC_SPEC_DIR='"$(SPEC)"' \
	-DGOIPC_VECTORS_FILE='"$(GEN)/vectors.txt"' \
	-DGOIPC_PEER_PATH='"$(B)/goipc-peer"' \
	-DGOIPC_SHARED_LIB='"$(B)/libgoipc.so"'
CFLAGS := $(WARN) -O2 -fPIC -fvisibility=hidden
ASAN_FLAGS := $(WARN) -O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined -fno-sanitize-recover=all
TSAN_FLAGS := $(WARN) -O1 -g -fsanitize=thread

LIB_OBJ := $(patsubst src/%.c,$(B)/obj/%.o,$(SRC))
TEST_OBJ := $(patsubst tests/%.c,$(B)/obj/tests/%.o,$(TEST_SRC))
ASAN_OBJ := $(patsubst %.c,$(B)/asan/%.o,$(SRC) $(TEST_SRC))
TSAN_OBJ := $(patsubst %.c,$(B)/tsan/%.o,$(SRC) $(TEST_SRC))

.PHONY: build test clean

build: $(B)/libgoipc.so $(B)/libgoipc.a $(B)/goipc-peer

$(GEN)/spec_wire.h: $(SPEC)/wire.json tests/wire.jq
	@mkdir -p $(@D)
	$(JQ) -r -f tests/wire.jq $< > $@.tmp && mv $@.tmp $@

$(GEN)/vectors.txt: $(SPEC)/vectors/ring/manifest.json tests/vectors.jq
	@mkdir -p $(@D)
	$(JQ) -r -f tests/vectors.jq $< > $@.tmp && mv $@.tmp $@

$(B)/obj/%.o: src/%.c $(HEADERS)
	@mkdir -p $(@D)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(B)/obj/tests/%.o: tests/%.c $(HEADERS)
	@mkdir -p $(@D)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(B)/asan/%.o: %.c $(HEADERS)
	@mkdir -p $(@D)
	$(CC) $(CPPFLAGS) $(ASAN_FLAGS) -c $< -o $@

$(B)/tsan/%.o: %.c $(HEADERS)
	@mkdir -p $(@D)
	$(CC) $(CPPFLAGS) $(TSAN_FLAGS) -c $< -o $@

$(B)/libgoipc.so: $(LIB_OBJ)
	$(CC) -shared -pthread -o $@ $^

$(B)/libgoipc.a: $(LIB_OBJ)
	rm -f $@
	$(AR) rcs $@ $^

# These two rules run only when the file is missing, and then they fail with the reason.
$(GEN_DIR)/c/demo.h:
	@echo "c: $@ is missing; GEN_DIR=$(GEN_DIR) must hold the ipcgen output (run make gen at the top level)" >&2; exit 1

$(IPCGEN):
	@echo "c: IPCGEN=$(IPCGEN) does not exist; it must name the ipcgen binary (run make ipcgen at the top level)" >&2; exit 1

$(PEER_GEN)/schema.json: $(SCHEMA_DIR)/example.ipc $(IPCGEN)
	@mkdir -p $(@D)
	$(IPCGEN) --lang json --out $@ $<

$(PEER_GEN)/fixture.h: $(PEER_GEN)/schema.json ../codegen/fixture.jq $(SCHEMA_DIR)/values.json $(SCHEMA_DIR)/invalid.json
	$(JQ) -r --arg lang c --slurpfile schema $< --slurpfile invalid $(SCHEMA_DIR)/invalid.json \
		-f ../codegen/fixture.jq $(SCHEMA_DIR)/values.json > $@.tmp && mv $@.tmp $@

$(PEER_GEN)/messages.h: $(PEER_GEN)/schema.json peer/messages.jq
	$(JQ) -r -f peer/messages.jq $< > $@.tmp && mv $@.tmp $@

$(B)/goipc-peer: peer/peer.c $(B)/libgoipc.a include/goipc.h $(GEN_DIR)/c/demo.h $(PEER_GEN)/fixture.h $(PEER_GEN)/messages.h
	$(CC) $(CPPFLAGS) -I$(GEN_DIR)/c -I$(PEER_GEN) $(CFLAGS) -o $@ peer/peer.c $(B)/libgoipc.a

$(B)/tests: $(TEST_OBJ) $(LIB_OBJ)
	$(CC) -pthread -o $@ $^ -ldl

$(B)/tests-asan: $(ASAN_OBJ)
	$(CC) -pthread -fsanitize=address,undefined -o $@ $^ -ldl

$(B)/tests-tsan: $(TSAN_OBJ)
	$(CC) -pthread -fsanitize=thread -o $@ $^ -ldl

# The ThreadSanitizer run covers the threaded tests; it cannot follow fork.
test: build $(B)/tests $(B)/tests-asan $(B)/tests-tsan $(GEN)/vectors.txt
	$(B)/tests
	ASAN_OPTIONS=detect_leaks=1:abort_on_error=1 UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1 $(B)/tests-asan
	TSAN_OPTIONS=halt_on_error=1:second_deadlock_stack=1 setarch $$(uname -m) -R $(B)/tests-tsan --threads-only

clean:
	rm -rf $(B)
