package ipc

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Only a toolchain that knows GOOS=cosmo reads _cosmo.go as a GOOS suffix.
// Every other toolchain builds the file everywhere, unless its tag says not.
func TestCosmoFilesCarryTheirTag(t *testing.T) {
	files, err := filepath.Glob("*_cosmo.go")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, file := range files {
		src, err := os.ReadFile(file)
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(string(src), "//go:build cosmo\n"), "%s has no //go:build cosmo line", file)
	}
}
