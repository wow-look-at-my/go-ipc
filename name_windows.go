package ipc

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sys/windows"
)

// nameOutlivesHolder is false: delete-on-close removes the name with its holder.
const nameOutlivesHolder = false

func namePath(name string) string {
	return filepath.Join(runtimeDir(), "go-ipc-"+name+".name")
}

// A nameLock holds a name. The creator opens the name file with no write
// sharing and with delete-on-close.
type nameLock struct {
	f *os.File
}

func lockName(name string) (*nameLock, error) {
	path := namePath(name)
	wide, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	h, err := windows.CreateFile(wide,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ,
		nil,
		windows.OPEN_ALWAYS,
		windows.FILE_ATTRIBUTE_TEMPORARY|windows.FILE_FLAG_DELETE_ON_CLOSE,
		0)
	// A sharing violation means a live holder. Access denied means the
	// last holder closed the file while a reader still has it open.
	if errors.Is(err, windows.ERROR_SHARING_VIOLATION) || errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		return nil, fmt.Errorf("ipc: %q: %w", name, ErrInUse)
	}
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return &nameLock{f: os.NewFile(uintptr(h), path)}, nil
}

// previous is always empty. The file dies with its holder, and so does every object of the instance it named.
func (l *nameLock) previous() string { return "" }

func (l *nameLock) publish(inc string) error {
	if _, err := l.f.WriteAt([]byte(inc), 0); err != nil {
		return err
	}
	return l.f.Truncate(int64(len(inc)))
}

// withdraw empties the name file. The file stays until its holder closes it,
// so the empty name is how a sender learns that the receiver unlinked.
func (l *nameLock) withdraw() error { return l.f.Truncate(0) }

func (l *nameLock) release() error { return l.f.Close() }

func readName(name string) (string, error) {
	path := namePath(name)
	wide, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return "", err
	}
	h, err := windows.CreateFile(wide,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0)
	if errors.Is(err, windows.ERROR_FILE_NOT_FOUND) || errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		return "", fmt.Errorf("ipc: no endpoint named %q: %w", name, fs.ErrNotExist)
	}
	if err != nil {
		return "", &os.PathError{Op: "open", Path: path, Err: err}
	}
	f := os.NewFile(uintptr(h), path)
	defer f.Close()
	content, err := io.ReadAll(io.LimitReader(f, incarnationLen+1))
	if err != nil {
		return "", err
	}
	inc, ok := parseIncarnation(content)
	if !ok {
		return "", fmt.Errorf("ipc: %q: %w", name, errNotReady)
	}
	return inc, nil
}

// unlinkName has nothing to do. The name file goes away with its holder.
func unlinkName(string, string) error { return nil }

var sweepOnce sync.Once

// sweepStale removes the life sockets of processes that have exited. Every
// other object dies with its last handle.
func sweepStale() {
	sweepOnce.Do(func() {
		entries, err := os.ReadDir(runtimeDir())
		if err != nil {
			return
		}
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "go-ipc-life-") && strings.HasSuffix(entry.Name(), ".sock") {
				sweepLife(filepath.Join(runtimeDir(), entry.Name()))
			}
		}
	})
}
