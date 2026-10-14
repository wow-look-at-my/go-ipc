//go:build !unix && !windows

package ipc

import (
	"context"
	"errors"
)

// This platform has no FIFO and no semaphore, so it has no named event.

type eventImpl struct{}

func createEventImpl(string) (*eventImpl, error) { return nil, errors.ErrUnsupported }

func openEventImpl(string) (*eventImpl, error) { return nil, errors.ErrUnsupported }

func unlinkEventImpl(string) error { return errors.ErrUnsupported }

func (*eventImpl) signal(int) error { return errors.ErrUnsupported }

func (*eventImpl) wait(context.Context) error { return errors.ErrUnsupported }

func (*eventImpl) close() error { return nil }
