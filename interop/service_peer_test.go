package interop

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"

	ipc "github.com/wow-look-at-my/go-ipc"
)

// The request and reply types of the service roles in spec/peer.md.
const (
	serviceEcho   = uint32(1)
	serviceEchoed = uint32(2)
	serviceFail   = uint32(3)
	serviceWho    = uint32(4)
	serviceWhoAmI = uint32(5)
)

// countingHandler answers the service roles' requests and counts the clients that go.
type countingHandler struct {
	gone chan struct{}
}

func (h *countingHandler) Call(s *ipc.Session, typ uint32, payload []byte) (uint32, []byte, error) {
	switch typ {
	case serviceEcho:
		return serviceEchoed, payload, nil
	case serviceFail:
		return 0, nil, errors.New(string(payload))
	case serviceWho:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(s.Ordinal()))
		return serviceWhoAmI, b[:], nil
	}
	return 0, nil, fmt.Errorf("type %d is not a request", typ)
}

func (h *countingHandler) Gone(*ipc.Session) { h.gone <- struct{}{} }

func peerServiceServe(ctx context.Context, name string, capacity, clients int) error {
	h := &countingHandler{gone: make(chan struct{}, clients)}
	svc, err := ipc.Serve(name, h, ipc.WithCapacity(capacity))
	if err != nil {
		return err
	}
	defer svc.Close()
	if err := ready(); err != nil {
		return err
	}
	for i := 0; i < clients; i++ {
		select {
		case <-h.gone:
		case <-ctx.Done():
			return fmt.Errorf("client %d of %d never went: %w", i, clients, ctx.Err())
		}
	}
	_, err = fmt.Fprintf(os.Stdout, "ok %d\n", clients)
	return err
}

func peerServiceCall(ctx context.Context, name string, count int, mode string) error {
	if err := checkMode(mode); err != nil {
		return err
	}
	c, err := ipc.Connect(ctx, name)
	if err != nil {
		return err
	}
	for i := 0; i < count; i++ {
		want := strconv.Itoa(i)
		typ, reply, err := c.Call(ctx, serviceEcho, []byte(want))
		if err != nil {
			return fmt.Errorf("call %d: %w", i, err)
		}
		if typ != serviceEchoed || string(reply) != want {
			return fmt.Errorf("call %d: reply type %d payload %q, want type %d payload %q", i, typ, reply, serviceEchoed, want)
		}
		boom := "boom " + want
		_, _, err = c.Call(ctx, serviceFail, []byte(boom))
		var ce *ipc.CallError
		if !errors.As(err, &ce) {
			return fmt.Errorf("call %d: fail returned %v, want a call error", i, err)
		}
		if ce.Message != boom {
			return fmt.Errorf("call %d: error %q, want %q", i, ce.Message, boom)
		}
	}
	typ, reply, err := c.Call(ctx, serviceWho, nil)
	if err != nil {
		return fmt.Errorf("who: %w", err)
	}
	if typ != serviceWhoAmI || len(reply) != 8 || binary.LittleEndian.Uint64(reply) != uint64(c.Ordinal()) {
		return fmt.Errorf("who: reply type %d payload %x, want ordinal %d", typ, reply, c.Ordinal())
	}
	if _, err := fmt.Fprintf(os.Stdout, "ok %d\n", count); err != nil {
		return err
	}
	if mode == "exit" {
		die()
	}
	return c.Close()
}
