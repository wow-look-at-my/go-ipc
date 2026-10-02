# Runs the goipc suite under every supported CPython. Each version is its own
# target, so `make -j` runs them in parallel.
include $(dir $(lastword $(MAKEFILE_LIST)))versions.mk
# A narrower glob, such as test_wire.py, runs part of the suite.
PATTERN ?= test*.py
# Extra unittest arguments, such as -k QueueTest.
UNITTEST_ARGS ?=
# none streams the output of a single version as it runs.
OUTPUT_SYNC ?= target

ROOT := $(abspath $(CURDIR)/..)
# The typed peer roles import the ipcgen module demo from here.
GEN_DIR ?= $(ROOT)/build/gen
export PYTHONPATH := $(CURDIR):$(GEN_DIR)/py$(if $(PYTHONPATH),:$(PYTHONPATH))
export GOIPC_SPEC_DIR ?= $(ROOT)/spec

ifeq ($(findstring jobserver,$(MAKEFLAGS)),)
MAKEFLAGS += -j$(words $(PY_VERSIONS)) --output-sync=$(OUTPUT_SYNC)
endif

TEST_TARGETS := $(addprefix test-,$(PY_VERSIONS))

.PHONY: build test interpreters clean $(TEST_TARGETS)

build:

interpreters:
	uv python install $(PY_VERSIONS)

test: $(TEST_TARGETS)

$(TEST_TARGETS): test-%:
	@uv python find --no-project $* >/dev/null 2>&1 || { \
		echo "python $*: interpreter not installed; run 'make -C python interpreters'" >&2; \
		exit 1; }
	uv run --no-project --no-python-downloads --python $* \
		python -W error -m unittest discover -s tests -p '$(PATTERN)' -v $(UNITTEST_ARGS)

clean:
	find $(CURDIR) -name __pycache__ -type d -prune -exec rm -rf {} +
