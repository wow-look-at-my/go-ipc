//go:build unix

package ipc

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

const (
	namePrefix = "go-ipc-"
	nameSuffix = ".name"
)

// nameOutlivesHolder is true: the name stays until a sweep or a new creator replaces it.
const nameOutlivesHolder = true

func namePath(name string) string {
	return filepath.Join(runtimeDir(), namePrefix+name+nameSuffix)
}

// incPath holds the incarnation. It is a separate file from the lock, because
// a flock on a Windows host is mandatory and would stop a reader.
func incPath(name string) string {
	return filepath.Join(runtimeDir(), namePrefix+name+".inc")
}

// A nameLock holds a name. The creator keeps it for the life of the queue,
// and the kernel drops the flock when the creator exits.
type nameLock struct {
	name string
	f    *os.File
}

// lockName takes the name, or fails with ErrInUse while a live process
// holds it.
func lockName(name string) (*nameLock, error) {
	path := namePath(name)
	for {
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			return nil, err
		}
		if err := lockFile(f, false); err != nil {
			f.Close()
			if errors.Is(err, unix.EWOULDBLOCK) {
				return nil, fmt.Errorf("ipc: %q: %w", name, ErrInUse)
			}
			return nil, err
		}
		// Another process can remove or replace the file between the open and the flock.
		held, herr := f.Stat()
		now, nerr := os.Stat(path)
		if herr == nil && nerr == nil && os.SameFile(held, now) {
			return &nameLock{name: name, f: f}, nil
		}
		f.Close()
	}
}

// previous returns the instance the name named before this lock took it.
func (l *nameLock) previous() string {
	content, _ := os.ReadFile(incPath(l.name))
	inc, _ := parseIncarnation(content)
	return inc
}

func (l *nameLock) publish(inc string) error {
	f, err := os.OpenFile(incPath(l.name), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteAt([]byte(inc), 0); err != nil {
		return err
	}
	return f.Truncate(int64(len(inc)))
}

func (l *nameLock) release() error { return l.f.Close() }

// readName returns the instance a name points at.
func readName(name string) (string, error) {
	if _, err := os.Stat(namePath(name)); errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("ipc: no endpoint named %q: %w", name, fs.ErrNotExist)
	}
	content, err := os.ReadFile(incPath(name))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	inc, ok := parseIncarnation(content)
	if !ok {
		return "", fmt.Errorf("ipc: %q: %w", name, errNotReady)
	}
	return inc, nil
}

// unlinkName removes the name file while it still points at inc. A newer
// instance under the same name keeps its file.
func unlinkName(name, inc string) error {
	content, err := os.ReadFile(incPath(name))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if current, _ := parseIncarnation(content); current != inc {
		return nil
	}
	return removeName(name)
}

func removeName(name string) error {
	for _, path := range []string{incPath(name), namePath(name)} {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}

var sweepOnce sync.Once

// sweepStale removes every name that no live process holds, with the instance
// it points at.
func sweepStale() {
	sweepOnce.Do(func() { sweepDir(runtimeDir()) })
}

func sweepDir(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "go-ipc-life-") && strings.HasSuffix(entry.Name(), ".sock") {
			sweepLife(filepath.Join(dir, entry.Name()))
			continue
		}
		name, ok := strings.CutPrefix(entry.Name(), namePrefix)
		if !ok {
			continue
		}
		base, ok := strings.CutSuffix(name, nameSuffix)
		if !ok {
			base, ok = strings.CutSuffix(name, ".inc")
		}
		if name = base; !ok || validateName(name) != nil {
			continue
		}
		sweepName(name)
	}
}

// sweepLife removes the life socket of a process that has exited. Nothing
// listens there, so a dial is refused.
func sweepLife(path string) {
	conn, err := net.Dial("unix", path)
	if err == nil {
		conn.Close()
		return
	}
	if isRefused(err) {
		os.Remove(path)
	}
}

// sweepName removes a name that no live process holds. The cleanup is on
// behalf of processes that are gone. A failure costs this caller nothing, and
// the next sweep tries again.
func sweepName(name string) {
	lock, err := lockName(name)
	if err != nil {
		return
	}
	defer lock.release()
	if inc := lock.previous(); inc != "" {
		if removeInstance(name, inc) != nil {
			return
		}
	}
	removeName(name)
}
