package filelock

import (
	"go/build"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// import here must stay net-free on every platform.
func TestImportsNoNet(t *testing.T) {
	allowed := map[string]bool{
		"context":                             true,
		"errors":                              true,
		"os":                                  true,
		"golang.org/x/sys/unix":               true,
		"golang.org/x/sys/windows":            true,
		"github.com/stretchr/testify/assert":  true,
		"github.com/stretchr/testify/require": true,
		"go/build":                            true,
		"testing":                             true,
	}
	for _, goos := range []string{"linux", "darwin", "windows", "freebsd"} {
		ctx := build.Default
		ctx.GOOS = goos
		pkg, err := ctx.ImportDir(".", 0)
		require.NoError(t, err, goos)
		for _, path := range append(pkg.Imports, pkg.TestImports...) {
			assert.True(t, allowed[path], "%s: filelock imports %s", goos, path)
		}
	}
}
