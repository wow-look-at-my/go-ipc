package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-ipc/ipcgen/internal/gen"
)

const example = "../spec/vectors/schema/example.ipc"

// runFresh runs the command with flags reset, since cobra keeps flag values between calls.
func runFresh(args ...string) error {
	rootFlags.lang, rootFlags.out = "", ""
	for _, name := range []string{"lang", "out"} {
		rootCmd.Flags().Lookup(name).Changed = false
	}
	return run(args)
}

func TestNormalizeArgs(t *testing.T) {
	in := []string{"-lang", "go", "-out=x", "-o", "y", "--lang", "c", "-x", "--", "-lang"}
	assert.Equal(t, []string{"--lang", "go", "--out=x", "-o", "y", "--lang", "c", "-x", "--", "-lang"}, normalizeArgs(in))
	assert.Equal(t, "-lang", in[0], "the input slice stays as it is")
}

func TestRunWritesEveryLanguage(t *testing.T) {
	dir := t.TempDir()
	for _, lang := range gen.Languages() {
		out := filepath.Join(dir, "out."+lang)
		require.NoError(t, runFresh("--lang", lang, "--out", out, example), lang)
		b, err := os.ReadFile(out)
		require.NoError(t, err)
		assert.Contains(t, string(b), gen.Header, lang)
	}
	out := filepath.Join(dir, "single-dash.go")
	require.NoError(t, runFresh("-lang", "go", "-o", out, example))
	assert.FileExists(t, out)
}

func TestRunErrors(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.ipc")
	require.NoError(t, os.WriteFile(bad, []byte("package d\nmessage A = 1 {\n\tx Nope\n}\n"), 0o644))
	out := filepath.Join(dir, "out")
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"--out", out, example}, `"lang" not set`},
		{[]string{"--lang", "go", example}, `"out" not set`},
		{[]string{"--lang", "go", "--out", out}, "accepts 1 arg"},
		{[]string{"--lang", "rust", "--out", out, example}, `unknown language "rust"`},
		{[]string{"--lang", "go", "--out", out, bad}, "bad.ipc:3:4: unknown type Nope"},
		{[]string{"--lang", "go", "--out", filepath.Join(dir, "missing", "x.go"), example}, "write output"},
	}
	for _, c := range cases {
		err := runFresh(c.args...)
		require.Error(t, err, strings.Join(c.args, " "))
		assert.Contains(t, err.Error(), c.want)
	}
	assert.NoFileExists(t, out)
}
