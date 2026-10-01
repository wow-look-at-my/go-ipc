package interop

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	ipc "github.com/wow-look-at-my/go-ipc"
)

// die ends the process with no cleanup, as a crash does. Deferred calls do
// not run, so no claim commits and no handle closes.
func die() {
	os.Exit(0)
}

func checkMode(mode string) error {
	if mode != "close" && mode != "exit" {
		return fmt.Errorf("mode %q is neither close nor exit", mode)
	}
	return nil
}

func peerClaimAndDie(ctx context.Context, name string, length int) error {
	q, err := ipc.OpenQueue(name)
	if err != nil {
		return err
	}
	c, err := q.Claim(ctx, 1, length)
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	copy(c.Bytes, bytes.Repeat([]byte{0xAB}, length))
	die()
	return nil
}

func peerSendUntilGone(ctx context.Context, name, sender string) error {
	q, err := ipc.OpenQueue(name)
	if err != nil {
		return err
	}
	defer q.Close()
	for i := 0; ; i++ {
		err := q.SendTyped(ctx, uint32(i), []byte(sender+":"+strconv.Itoa(i)))
		if errors.Is(err, ipc.ErrPeerGone) {
			_, err = fmt.Fprintf(os.Stdout, "gone %d\n", i)
			return err
		}
		if err != nil {
			return fmt.Errorf("send %d: %w", i, err)
		}
	}
}

func peerRecvThenStop(ctx context.Context, name string, count, capacity int, mode string) error {
	if err := checkMode(mode); err != nil {
		return err
	}
	q, err := ipc.CreateQueue(name, ipc.WithCapacity(capacity))
	if err != nil {
		return err
	}
	if err := ready(); err != nil {
		return err
	}
	if err := receiveChecked(ctx, q, count); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(os.Stdout, "ok %d\n", count); err != nil {
		return err
	}
	if mode == "exit" {
		if err := q.Unlink(); err != nil {
			return err
		}
		die()
	}
	if err := q.Close(); err != nil {
		return err
	}
	return q.Unlink()
}

func peerChanRecvUntilGone(ctx context.Context, name string, capacity, count int) error {
	ch, err := ipc.CreateChannel(name, ipc.WithCapacity(capacity))
	if err != nil {
		return err
	}
	defer ch.Unlink()
	defer ch.Close()
	if err := ready(); err != nil {
		return err
	}

	got := 0
	for {
		typ, msg, err := ch.Recv(ctx)
		if errors.Is(err, ipc.ErrPeerGone) {
			break
		}
		if err != nil {
			return fmt.Errorf("message %d: %w", got, err)
		}
		if want := "0:" + strconv.Itoa(got); typ != uint32(got) || string(msg) != want {
			return fmt.Errorf("message %d: type %d, payload %q, want type %d, payload %q", got, typ, msg, got, want)
		}
		got++
	}
	if got != count {
		return fmt.Errorf("received %d messages before the peer was gone, want %d", got, count)
	}
	if err := ch.Send(ctx, []byte("late")); !errors.Is(err, ipc.ErrPeerGone) {
		return fmt.Errorf("send after the peer was gone: got %v, want peer-gone", err)
	}
	_, err = fmt.Fprintf(os.Stdout, "ok %d\n", count)
	return err
}

func peerChanSendThenStop(ctx context.Context, name string, count int, mode string) error {
	if err := checkMode(mode); err != nil {
		return err
	}
	ch, err := ipc.OpenChannel(name)
	if err != nil {
		return err
	}
	for i := 0; i < count; i++ {
		if err := ch.SendTyped(ctx, uint32(i), []byte("0:"+strconv.Itoa(i))); err != nil {
			return fmt.Errorf("send %d: %w", i, err)
		}
	}
	if mode == "exit" {
		die()
	}
	return ch.Close()
}
