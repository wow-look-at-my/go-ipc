# Runs the cross-language matrix. The top-level Makefile exports the peer
# commands (GOIPC_PEER_C, GOIPC_PEER_CPP, GOIPC_PEER_PY). GOIPC_INTEROP_LANGS
# limits the matrix to a comma list of go, c, cpp and py.
.PHONY: test clean

test:
	go-toolchain --no-benchmark

clean:
	rm -rf build
