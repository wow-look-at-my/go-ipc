package ipc

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/wow-look-at-my/go-shm"
)

// A queue name points at an instance: a segment and a pair of events whose names carry an incarnation.

const incarnationLen = 16

func newIncarnation() (string, error) {
	var raw [incarnationLen / 2]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

// parseIncarnation checks the content of a name file.
func parseIncarnation(content []byte) (string, bool) {
	if len(content) != incarnationLen {
		return "", false
	}
	if _, err := hex.DecodeString(string(content)); err != nil {
		return "", false
	}
	return string(content), true
}

func instanceName(name, inc string) string { return name + "." + inc }

// errNotReady reports a name file that exists but names no instance yet.
var errNotReady = errors.New("ipc: endpoint is not ready")

// removeInstance deletes what an instance left in the namespace. An instance
// that is already gone is not an error.
func removeInstance(name, inc string) error {
	inst := instanceName(name, inc)
	var err error
	if seg, openErr := shm.Open(inst); openErr == nil {
		err = seg.Unlink()
		seg.Close()
	}
	if uerr := unlinkEventImpl(inst + ".ne"); err == nil {
		err = uerr
	}
	if uerr := unlinkEventImpl(inst + ".nf"); err == nil {
		err = uerr
	}
	if err != nil {
		return fmt.Errorf("ipc: remove %q: %w", inst, err)
	}
	return nil
}
