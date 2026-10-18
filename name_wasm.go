//go:build js || wasip1

package ipc

import (
	"fmt"
	"io/fs"
	"sync"
)

// nameOutlivesHolder is false: release removes the name, as delete-on-close
// does on Windows.
const nameOutlivesHolder = false

// WebAssembly runs one process, so a name is an entry in a registry of this
// process. Its holder is the lock that took it.
type nameEntry struct {
	holder *nameLock
	inc    string
}

var names struct {
	mu     sync.Mutex
	byName map[string]*nameEntry
}

// A nameLock holds a name until release.
type nameLock struct {
	name string
}

func lockName(name string) (*nameLock, error) {
	names.mu.Lock()
	defer names.mu.Unlock()
	if names.byName[name] != nil {
		return nil, fmt.Errorf("ipc: %q: %w", name, ErrInUse)
	}
	if names.byName == nil {
		names.byName = make(map[string]*nameEntry)
	}
	lock := &nameLock{name: name}
	names.byName[name] = &nameEntry{holder: lock}
	return lock, nil
}

// previous is always empty. A name goes with its holder, and so does every
// object of the instance it named.
func (l *nameLock) previous() string { return "" }

func (l *nameLock) publish(inc string) error {
	names.mu.Lock()
	defer names.mu.Unlock()
	entry := names.byName[l.name]
	if entry == nil || entry.holder != l {
		return fmt.Errorf("ipc: %q: the name was released", l.name)
	}
	entry.inc = inc
	return nil
}

func (l *nameLock) release() error {
	names.mu.Lock()
	defer names.mu.Unlock()
	if entry := names.byName[l.name]; entry != nil && entry.holder == l {
		delete(names.byName, l.name)
	}
	return nil
}

func readName(name string) (string, error) {
	names.mu.Lock()
	entry := names.byName[name]
	names.mu.Unlock()
	if entry == nil {
		return "", fmt.Errorf("ipc: no endpoint named %q: %w", name, fs.ErrNotExist)
	}
	if entry.inc == "" {
		return "", fmt.Errorf("ipc: %q: %w", name, errNotReady)
	}
	return entry.inc, nil
}

// nameGone reports whether no lock holds the name.
func nameGone(name string) bool {
	names.mu.Lock()
	defer names.mu.Unlock()
	return names.byName[name] == nil
}

// nameList answers each name a lock holds.
func nameList() []string {
	names.mu.Lock()
	defer names.mu.Unlock()
	list := make([]string, 0, len(names.byName))
	for name := range names.byName {
		list = append(list, name)
	}
	return list
}

// unlinkName has nothing to do. The name goes with its holder.
func unlinkName(string, string) error { return nil }

// sweepStale has nothing to do. No holder outlives this process.
func sweepStale() {}
