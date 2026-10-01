# Runs the cross-language matrix. The top-level Makefile exports the peer
# commands (GOIPC_PEER_C, GOIPC_PEER_CPP, GOIPC_PEER_PY). GOIPC_INTEROP_LANGS
# limits the matrix to a comma list of go, c, cpp and py.
# `prepare` copies the generated Go code into internal/demo, which git ignores.
.PHONY: test prepare clean

test:
	go-toolchain --no-benchmark

prepare:
	@test -n "$(GEN_DIR)" || { echo "interop: GEN_DIR is not set; pass GEN_DIR=<dir with go/demo/demo.go>" >&2; exit 1; }
	@test -f "$(GEN_DIR)/go/demo/demo.go" || { echo "interop: $(GEN_DIR)/go/demo/demo.go is missing; run make gen first" >&2; exit 1; }
	mkdir -p internal/demo
	cp $(GEN_DIR)/go/demo/demo.go internal/demo/demo.go

clean:
	rm -rf build internal/demo/demo.go
