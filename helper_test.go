package ipc

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

var nameCounter atomic.Uint64

// uniqueName builds a shared-memory name that no other test or concurrent run
// can collide with. The names live in a system-wide namespace, so the process
// id has to be part of them.
func uniqueName(t *testing.T) string {
	t.Helper()
	clean := strings.Map(func(r rune) rune {
		if r == '/' || r == '\\' {
			return '-'
		}
		return r
	}, t.Name())
	return fmt.Sprintf("test-%s-%d-%d", clean, os.Getpid(), nameCounter.Add(1))
}
