//go:build cosmo || darwin || linux || windows

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
		first, _, _ := strings.Cut(string(src), "\n")
		assert.Equal(t, "//go:build cosmo", strings.TrimSuffix(first, "\r"), "%s has no //go:build cosmo line", file)
	}
}

// supportedConstraint names the platforms this module builds for.
const supportedConstraint = "//go:build cosmo || darwin || linux || windows"

// Each file whose name names no platform carries supportedConstraint.
func TestFilesNameTheSupportedPlatforms(t *testing.T) {
	for _, pattern := range []string{"*.go", "filelock/*.go"} {
		files, err := filepath.Glob(pattern)
		require.NoError(t, err)
		require.NotEmpty(t, files)
		for _, file := range files {
			base := strings.TrimSuffix(strings.TrimSuffix(filepath.Base(file), ".go"), "_test")
			if strings.HasSuffix(base, "_cosmo") || strings.HasSuffix(base, "_darwin") || strings.HasSuffix(base, "_linux") || strings.HasSuffix(base, "_windows") {
				continue
			}
			src, err := os.ReadFile(file)
			require.NoError(t, err)
			first, _, _ := strings.Cut(string(src), "\n")
			first = strings.TrimSuffix(first, "\r")
			if strings.HasPrefix(first, "//go:build ") && first != supportedConstraint {
				// A file for some of the platforms, such as event_unix.go.
				continue
			}
			assert.Equal(t, supportedConstraint, first, "%s builds on platforms this module does not support", file)
		}
	}
}
