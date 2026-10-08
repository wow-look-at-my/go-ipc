package filelock

import (
	"go/build"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
)

// The Go toolchain's bootstrap build takes this lock and refuses net.
func TestImportsNoNet(t *testing.T) {
	allowed := set.Of[string]("context",
		"github.com/wow-look-at-my/go-containers/set",
		"errors",
		"os",
		"golang.org/x/sys/unix",
		"golang.org/x/sys/windows",
		"github.com/stretchr/testify/assert",
		"github.com/stretchr/testify/require",
		"go/build",
		"testing")

	for _, goos := range []string{"linux", "darwin", "windows", "freebsd"} {
		ctx := build.Default
		ctx.GOOS = goos
		pkg, err := ctx.ImportDir(".", 0)
		require.NoError(t, err, goos)
		for _, path := range append(pkg.Imports, pkg.TestImports...) {
			assert.True(t, allowed.Contains(path), "%s: filelock imports %s", goos, path)
		}
	}
}
