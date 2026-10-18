//go:build !js && !wasip1

package ipc

import (
	"go/build"
	"testing"

	"github.com/stretchr/testify/require"
)

// A build for WebAssembly must refuse these packages, because neither a second
// process nor shared memory nor a file lock exists there. A file without the
// constraint lets a package build.
func TestNoFileBuildsOnWasm(t *testing.T) {
	for _, dir := range []string{".", "filelock"} {
		for _, goos := range []string{"js", "wasip1"} {
			ctx := build.Default
			ctx.GOOS, ctx.GOARCH = goos, "wasm"
			_, err := ctx.ImportDir(dir, 0)
			var noGo *build.NoGoError
			require.ErrorAs(t, err, &noGo, "%s/wasm builds a file of %s", goos, dir)
		}
	}
}
