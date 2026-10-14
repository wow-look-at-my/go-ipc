//go:build !unix && !windows

package filelock

import (
	"errors"
	"os"
)

// This platform has no file lock. Take fails, so a caller runs without it.

// Take fails with errors.ErrUnsupported.
func Take(*os.File, bool) error {
	return errors.ErrUnsupported
}

// Release does nothing: Take never takes a lock here.
func Release(*os.File) {}
