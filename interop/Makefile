# Runs the cross-language matrix. The top-level Makefile exports the peer
# commands (GOIPC_PEER_C, GOIPC_PEER_CPP, GOIPC_PEER_PY). GOIPC_INTEROP_LANGS
# limits the matrix to a comma list of go, c, cpp and py.
.PHONY: test clean prepare

test:
	go-toolchain --no-benchmark

# The Go peer's typed roles use the Go code that ipcgen generates.
prepare:
	@test -n "$(GEN_DIR)" || { echo "interop: GEN_DIR is not set; run make prepare-go from the repository root" >&2; exit 1; }
	mkdir -p internal/demo
	cp $(GEN_DIR)/go/demo/demo.go internal/demo/demo.go

clean:
	rm -rf build internal/demo/demo.go
