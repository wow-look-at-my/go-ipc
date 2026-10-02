package interop

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	ipc "github.com/wow-look-at-my/go-ipc"
)

// goPeerEnv turns the test binary into the Go peer of spec/peer.md.
const goPeerEnv = "GOIPC_INTEROP_GO_PEER"

// peerTimeout is how long a Go peer role runs before it gives up and fails.
const peerTimeout = 60 * time.Second

// runPeer runs one role of spec/peer.md and returns the reason it failed.
func runPeer(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: peer <role> <args...>")
	}
	ctx, cancel := context.WithTimeout(context.Background(), peerTimeout)
	defer cancel()

	role, rest := args[0], args[1:]
	switch role {
	case "recv":
		if len(rest) != 3 {
			return errors.New("usage: recv <name> <total> <capacity>")
		}
		total, err := strconv.Atoi(rest[1])
		if err != nil {
			return fmt.Errorf("total: %w", err)
		}
		capacity, err := strconv.Atoi(rest[2])
		if err != nil {
			return fmt.Errorf("capacity: %w", err)
		}
		return peerRecv(ctx, rest[0], total, capacity)
	case "send":
		if len(rest) != 3 {
			return errors.New("usage: send <name> <sender> <count>")
		}
		count, err := strconv.Atoi(rest[2])
		if err != nil {
			return fmt.Errorf("count: %w", err)
		}
		return peerSend(ctx, rest[0], rest[1], count)
	case "listen-echo":
		if len(rest) != 2 {
			return errors.New("usage: listen-echo <name> <capacity>")
		}
		capacity, err := strconv.Atoi(rest[1])
		if err != nil {
			return fmt.Errorf("capacity: %w", err)
		}
		return peerListenEcho(rest[0], capacity)
	case "dial-check":
		if len(rest) != 2 {
			return errors.New("usage: dial-check <name> <bytes>")
		}
		n, err := strconv.Atoi(rest[1])
		if err != nil {
			return fmt.Errorf("bytes: %w", err)
		}
		return peerDialCheck(rest[0], n)
	case "typed-send":
		if len(rest) != 1 {
			return errors.New("usage: typed-send <name>")
		}
		return peerTypedSend(ctx, rest[0])
	case "typed-recv":
		if len(rest) != 2 {
			return errors.New("usage: typed-recv <name> <capacity>")
		}
		capacity, err := strconv.Atoi(rest[1])
		if err != nil {
			return fmt.Errorf("capacity: %w", err)
		}
		return peerTypedRecv(ctx, rest[0], capacity)
	case "claim-and-die":
		if len(rest) != 2 {
			return errors.New("usage: claim-and-die <name> <length>")
		}
		length, err := strconv.Atoi(rest[1])
		if err != nil {
			return fmt.Errorf("length: %w", err)
		}
		return peerClaimAndDie(ctx, rest[0], length)
	case "send-until-gone":
		if len(rest) != 2 {
			return errors.New("usage: send-until-gone <name> <sender>")
		}
		return peerSendUntilGone(ctx, rest[0], rest[1])
	case "recv-then-stop":
		if len(rest) != 4 {
			return errors.New("usage: recv-then-stop <name> <count> <capacity> <close|exit>")
		}
		count, err := strconv.Atoi(rest[1])
		if err != nil {
			return fmt.Errorf("count: %w", err)
		}
		capacity, err := strconv.Atoi(rest[2])
		if err != nil {
			return fmt.Errorf("capacity: %w", err)
		}
		return peerRecvThenStop(ctx, rest[0], count, capacity, rest[3])
	case "chan-recv-until-gone":
		if len(rest) != 3 {
			return errors.New("usage: chan-recv-until-gone <name> <capacity> <count>")
		}
		capacity, err := strconv.Atoi(rest[1])
		if err != nil {
			return fmt.Errorf("capacity: %w", err)
		}
		count, err := strconv.Atoi(rest[2])
		if err != nil {
			return fmt.Errorf("count: %w", err)
		}
		return peerChanRecvUntilGone(ctx, rest[0], capacity, count)
	case "chan-send-then-stop":
		if len(rest) != 3 {
			return errors.New("usage: chan-send-then-stop <name> <count> <close|exit>")
		}
		count, err := strconv.Atoi(rest[1])
		if err != nil {
			return fmt.Errorf("count: %w", err)
		}
		return peerChanSendThenStop(ctx, rest[0], count, rest[2])
	case "service-serve":
		if len(rest) != 3 {
			return errors.New("usage: service-serve <name> <capacity> <clients>")
		}
		capacity, err := strconv.Atoi(rest[1])
		if err != nil {
			return fmt.Errorf("capacity: %w", err)
		}
		clients, err := strconv.Atoi(rest[2])
		if err != nil {
			return fmt.Errorf("clients: %w", err)
		}
		return peerServiceServe(ctx, rest[0], capacity, clients)
	case "service-call":
		if len(rest) != 3 {
			return errors.New("usage: service-call <name> <count> <close|exit>")
		}
		count, err := strconv.Atoi(rest[1])
		if err != nil {
			return fmt.Errorf("count: %w", err)
		}
		return peerServiceCall(ctx, rest[0], count, rest[2])
	default:
		return fmt.Errorf("unknown role %q", role)
	}
}

// ready tells the driver that the endpoint exists. os.Stdout has no buffer, so
// the line leaves the process at once.
func ready() error {
	_, err := os.Stdout.WriteString("ready\n")
	return err
}

func peerRecv(ctx context.Context, name string, total, capacity int) error {
	q, err := ipc.CreateQueue(name, ipc.WithCapacity(capacity))
	if err != nil {
		return err
	}
	defer q.Unlink()
	defer q.Close()
	if err := ready(); err != nil {
		return err
	}
	if err := receiveChecked(ctx, q, total); err != nil {
		return err
	}
	_, err = fmt.Fprintf(os.Stdout, "ok %d\n", total)
	return err
}

// receiveChecked receives total messages of the form the send role writes, and
// checks the order of each sender.
func receiveChecked(ctx context.Context, q *ipc.Queue, total int) error {
	next := map[string]uint64{}
	buf := make([]byte, q.MaxMessageSize())
	for i := 0; i < total; i++ {
		typ, msg, err := q.RecvInto(ctx, buf)
		if err != nil {
			return fmt.Errorf("message %d of %d: %w", i, total, err)
		}
		sender, seqText, ok := strings.Cut(string(msg), ":")
		if !ok {
			return fmt.Errorf("message %d: payload %q has no ':'", i, msg)
		}
		seq, err := strconv.ParseUint(seqText, 10, 32)
		if err != nil {
			return fmt.Errorf("message %d: payload %q: %w", i, msg, err)
		}
		if uint64(typ) != seq {
			return fmt.Errorf("message %d: type %d, payload %q", i, typ, msg)
		}
		if seq != next[sender] {
			return fmt.Errorf("message %d: sender %q sent seq %d, want %d", i, sender, seq, next[sender])
		}
		next[sender] = seq + 1
	}
	return nil
}

func peerSend(ctx context.Context, name, sender string, count int) error {
	q, err := ipc.OpenQueue(name)
	if err != nil {
		return err
	}
	defer q.Close()
	for i := 0; i < count; i++ {
		payload := []byte(sender + ":" + strconv.Itoa(i))
		if err := q.SendTyped(ctx, uint32(i), payload); err != nil {
			return fmt.Errorf("send %d: %w", i, err)
		}
	}
	return nil
}

func peerListenEcho(name string, capacity int) error {
	conn, err := ipc.Listen(name, ipc.WithCapacity(capacity))
	if err != nil {
		return err
	}
	defer conn.Unlink()
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(peerTimeout)); err != nil {
		return err
	}
	if err := ready(); err != nil {
		return err
	}
	_, err = io.Copy(conn, conn)
	return err
}

func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + 7)
	}
	return b
}

func peerDialCheck(name string, n int) error {
	conn, err := ipc.Dial(name)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(peerTimeout)); err != nil {
		return err
	}

	// The ring holds less than the stream, so the write runs beside the read.
	want := pattern(n)
	written := make(chan error, 1)
	go func() {
		if _, werr := conn.Write(want); werr != nil {
			written <- fmt.Errorf("write: %w", werr)
			return
		}
		if werr := conn.CloseWrite(); werr != nil {
			written <- fmt.Errorf("close write: %w", werr)
			return
		}
		written <- nil
	}()
	got, err := io.ReadAll(conn)
	if err != nil {
		return fmt.Errorf("read echo: %w", err)
	}
	if err := <-written; err != nil {
		return err
	}
	if i := firstDiff(want, got); i >= 0 {
		return fmt.Errorf("echo of %d bytes differs from the %d sent, at byte %d", len(got), len(want), i)
	}
	return nil
}

func firstDiff(a, b []byte) int {
	if bytes.Equal(a, b) {
		return -1
	}
	for i := range a {
		if i >= len(b) || a[i] != b[i] {
			return i
		}
	}
	return len(a)
}
