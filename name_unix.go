//go:build unix

// name_unix.go holds nameOutlivesHolder.

package ipc

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/wow-look-at-my/go-ipc/filelock"
	"golang.org/x/sys/unix"
)

const (
	namePrefix = "go-ipc-"
	nameSuffix = ".name"
)

// nameOutlivesHolder is true: the name stays until a sweep or a new creator replaces it.
const nameOutlivesHolder = true

// nameGone reports whether err, from a stat of a name file, means the name is gone.
func nameGone(err error) bool { return errors.Is(err, fs.ErrNotExist) }

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
	// released is set once release let the lock go.
	released atomic.Bool
}

// held reports a lock that is taken and not yet released.
func (l *nameLock) held() bool { return l != nil && !l.released.Load() }

// lockName takes the name, or fails with ErrInUse while a live process holds it.
func lockName(name string) (*nameLock, error) { return lockNameFile(name, os.O_CREATE) }

// lockExistingName is lockName for a name that has its file.
func lockExistingName(name string) (*nameLock, error) { return lockNameFile(name, 0) }

func lockNameFile(name string, create int) (*nameLock, error) {
	path := namePath(name)
	for {
		f, err := os.OpenFile(path, os.O_RDWR|create, 0o600)
		if err != nil {
			return nil, err
		}
		if err := filelock.Take(f, false); err != nil {
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

func (l *nameLock) release() error {
	l.released.Store(true)
	return l.f.Close()
}

// unlink has nothing to do. unlinkName removes the name file by its path.
func (l *nameLock) unlink() error { return nil }

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

// unlinkName removes the name file while it still points at inc. A newer instance under the same name keeps its file.
//
// own is the lock of a caller that created the name, and nil for one that opened it. A creator that let its lock go takes the name first: whoever holds it now is a successor. This may not have published its instance yet. The file still points at inc then, and removing it takes the name from under the successor. An opener removes the name of a live creator, as before.
func unlinkName(name, inc string, own *nameLock) error {
	if own != nil && !own.held() {
		lock, err := lockExistingName(name)
		if errors.Is(err, ErrInUse) || errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		defer lock.release()
	}
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
	var names []string
	built := builtInstances{}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "go-ipc-life-") && strings.HasSuffix(entry.Name(), ".sock") {
			sweepLife(filepath.Join(dir, entry.Name()))
			continue
		}
		if seg, ok := strings.CutPrefix(entry.Name(), segmentPrefix); ok {
			built.add(seg, sawSegment)
			continue
		}
		name, ok := strings.CutPrefix(entry.Name(), namePrefix)
		if !ok {
			continue
		}
		if built.note(name) {
			continue
		}
		base, ok := strings.CutSuffix(name, nameSuffix)
		if !ok {
			base, ok = strings.CutSuffix(name, ".inc")
		}
		if name = base; !ok || validateName(name) != nil {
			continue
		}
		names = append(names, name)
	}
	for _, name := range names {
		sweepName(name, built.of(name, true)...)
		// A name with a file is settled here, whether the lock was free.
		delete(built, name)
	}
	for name := range built {
		sweepUnnamed(name, built.of(name, false))
	}
}

// segmentPrefix starts the file name of a segment, where a host keeps segments as files.
const segmentPrefix = "go-shm-"

// builtInstances maps a name to the instances whose files are in the directory.
type builtInstances map[string]map[string]int

const (
	sawNotEmpty = 1 << iota
	sawNotFull
	sawSegment
)

// note records entry when it is the event of an instance. entry is a file
// name without the prefix that every file of this package has.
func (b builtInstances) note(entry string) bool {
	if base, ok := strings.CutSuffix(entry, ".ne.event"); ok {
		return b.add(base, sawNotEmpty)
	}
	if base, ok := strings.CutSuffix(entry, ".nf.event"); ok {
		return b.add(base, sawNotFull)
	}
	return false
}

// add records base when it reads as a name, a dot and an instance id.
func (b builtInstances) add(base string, saw int) bool {
	cut := len(base) - incarnationLen
	if cut < 2 || base[cut-1] != '.' {
		return false
	}
	inc, ok := parseIncarnation([]byte(base[cut:]))
	name := base[:cut-1]
	if !ok || validateName(name) != nil {
		return false
	}
	if b[name] == nil {
		b[name] = map[string]int{}
	}
	b[name][inc] |= saw
	return true
}

// of returns the instances of name that have both events. One event alone can
// be an event a caller made under a name of that shape. With segments, an
// instance that shows only its segment counts too: the holder of a name owns
// the segments under it.
func (b builtInstances) of(name string, segments bool) []string {
	var incs []string
	for inc, saw := range b[name] {
		if saw&(sawNotEmpty|sawNotFull) == sawNotEmpty|sawNotFull || (segments && saw&sawSegment != 0) {
			incs = append(incs, inc)
		}
	}
	return incs
}

// sweepName removes a name that no live process holds, with the instance it
// points at and every instance in built. The cleanup is on behalf of
// processes that are gone. A failure costs this caller nothing, and the next
// sweep tries again.
func sweepName(name string, built ...string) {
	lock, err := lockExistingName(name)
	if err != nil {
		return
	}
	defer lock.release()
	if inc := lock.previous(); inc != "" {
		built = append(built, inc)
	}
	for _, inc := range built {
		if removeInstance(name, inc) != nil {
			return
		}
	}
	removeName(name)
}

// sweepUnnamed removes instances whose name has no file.
func sweepUnnamed(name string, incs []string) {
	if _, err := os.Stat(namePath(name)); !nameGone(err) {
		return
	}
	for _, inc := range incs {
		removeInstance(name, inc)
	}
}
