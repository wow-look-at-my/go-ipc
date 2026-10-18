//go:build !(js || wasip1)

package ipc

import (
	"errors"
	"io/fs"
	"os"
	"strings"
)

// nameGone reports whether the file of the name is gone.
func nameGone(name string) bool {
	_, err := os.Stat(namePath(name))
	return errors.Is(err, fs.ErrNotExist)
}

// nameList answers each name that has a name file in runtimeDir.
func nameList() []string {
	entries, err := os.ReadDir(runtimeDir())
	if err != nil {
		return nil
	}
	var list []string
	for _, entry := range entries {
		rest, ok := strings.CutPrefix(entry.Name(), "go-ipc-")
		if !ok {
			continue
		}
		if name, ok := strings.CutSuffix(rest, ".name"); ok {
			list = append(list, name)
		}
	}
	return list
}
