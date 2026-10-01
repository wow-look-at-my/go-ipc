# Tests the code that ipcgen generates from spec/vectors/schema/example.ipc.
# The top-level Makefile passes GEN_DIR (the generated code) and IPCGEN (the binary).
# `test` runs the C, C++ and Python tests. The Go test lives in go/, where go-toolchain
# generates go/demo/demo.go itself and the root run tests it.

ROOT := $(abspath $(CURDIR)/..)
VECTORS := $(ROOT)/spec/vectors/schema
SCHEMA := $(VECTORS)/example.ipc
OUT := $(CURDIR)/build
SANITIZE := -fsanitize=address,undefined -fno-sanitize-recover=all -fno-omit-frame-pointer -g
CC := cc
CXX := c++
include $(ROOT)/python/versions.mk
PY_TESTS := $(addprefix test-py-,$(PY_VERSIONS))

.PHONY: test test-c test-cpp test-py $(PY_TESTS) clean require FORCE

test: test-c test-cpp test-py

# The test targets fail here, with a reason, when the caller did not say where the generated code is.
require:
	@test -n "$(GEN_DIR)" || { echo "codegen: GEN_DIR is not set; pass GEN_DIR=<dir with c/ cpp/ py/ go/>" >&2; exit 1; }
	@test -n "$(IPCGEN)" || { echo "codegen: IPCGEN is not set; pass IPCGEN=<path to the ipcgen binary>" >&2; exit 1; }
	@test -x "$(IPCGEN)" || { echo "codegen: IPCGEN=$(IPCGEN) is not an executable file" >&2; exit 1; }
	@for f in c/demo.h cpp/demo.hpp py/demo.py go/demo/demo.go; do \
		test -f "$(GEN_DIR)/$$f" || { echo "codegen: $(GEN_DIR)/$$f is missing; run make gen first" >&2; exit 1; }; \
	done

$(OUT)/schema.json: require FORCE
	mkdir -p $(OUT)
	$(IPCGEN) --lang json --out $@ $(SCHEMA)

$(OUT)/c/fixture.h $(OUT)/cpp/fixture.h: $(OUT)/%/fixture.h: $(OUT)/schema.json fixture.jq FORCE
	mkdir -p $(dir $@)
	jq -r --arg lang $* --slurpfile schema $(OUT)/schema.json --slurpfile invalid $(VECTORS)/invalid.json \
		-f fixture.jq $(VECTORS)/values.json > $@.tmp
	mv $@.tmp $@

test-c: $(OUT)/c/fixture.h
	$(CC) -std=c99 -Wall -Wextra -Werror $(SANITIZE) -I$(GEN_DIR)/c -I$(OUT)/c -DVECTOR_DIR='"$(VECTORS)"' \
		c/test.c -o $(OUT)/c/test
	$(OUT)/c/test

test-cpp: $(OUT)/cpp/fixture.h
	$(CXX) -std=c++20 -Wall -Wextra -Werror $(SANITIZE) -I$(GEN_DIR)/cpp -I$(OUT)/cpp -DVECTOR_DIR='"$(VECTORS)"' \
		cpp/test.cpp -o $(OUT)/cpp/test
	$(OUT)/cpp/test

# The generated Python runs on every supported CPython, so it is tested on each.
test-py: $(PY_TESTS)

$(PY_TESTS): test-py-%: $(OUT)/schema.json
	@uv python find --no-project $* >/dev/null 2>&1 || { \
		echo "python $*: interpreter not installed; run 'make -C python interpreters'" >&2; \
		exit 1; }
	cd py && PYTHONPATH=$(GEN_DIR)/py IPCGEN_VECTORS=$(VECTORS) IPCGEN_SCHEMA_JSON=$(OUT)/schema.json \
		uv run --no-project --no-python-downloads --python $* python -W error -m unittest -v test_demo

clean:
	rm -rf $(OUT) go/demo/demo.go go/build py/__pycache__
