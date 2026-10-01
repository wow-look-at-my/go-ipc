package interop

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
)

const (
	langsEnv     = "GOIPC_INTEROP_LANGS"
	capacity     = 4096
	queueCount   = 3000
	mpscCount    = 2000
	streamBytes  = 300000
	readyTimeout = 30 * time.Second
	cellTimeout  = 120 * time.Second
)

var allLangs = []string{"go", "c", "cpp", "py"}

// peerEnvs names the variable that holds each peer command. Go has none: its
// peer is this test binary.
var peerEnvs = map[string]string{
	"c":   "GOIPC_PEER_C",
	"cpp": "GOIPC_PEER_CPP",
	"py":  "GOIPC_PEER_PY",
}

func TestMain(m *testing.M) {
	if os.Getenv(goPeerEnv) != "" {
		if err := runPeer(os.Args[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "go peer: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// langs returns the languages in the matrix. The default is every language.
func langs(t *testing.T) []string {
	t.Helper()
	v := strings.TrimSpace(os.Getenv(langsEnv))
	if v == "" {
		return allLangs
	}
	var out []string
	seen := set.New[string]()
	for _, l := range strings.Split(v, ",") {
		l = strings.TrimSpace(l)
		require.Contains(t, allLangs, l, "%s names an unknown language", langsEnv)
		if !seen.Contains(l) {
			seen.Add(l)
			out = append(out, l)
		}
	}
	return out
}

// peerCommand returns the argv and the extra environment that start the peer
// of lang. A missing or non-executable peer fails the test.
func peerCommand(t *testing.T, lang string) ([]string, []string) {
	t.Helper()
	if lang == "go" {
		return []string{os.Args[0]}, []string{goPeerEnv + "=1"}
	}
	env := peerEnvs[lang]
	argv := strings.Fields(os.Getenv(env))
	require.NotEmpty(t, argv, "%s is not set; it must name the %s peer", env, lang)
	if strings.ContainsRune(argv[0], '/') {
		info, err := os.Stat(argv[0])
		require.NoError(t, err, "%s=%q: the %s peer does not exist", env, os.Getenv(env), lang)
		require.True(t, info.Mode().IsRegular() && info.Mode().Perm()&0o111 != 0,
			"%s=%q: the %s peer is not an executable file", env, os.Getenv(env), lang)
	} else {
		_, err := exec.LookPath(argv[0])
		require.NoError(t, err, "%s=%q: %s is not on PATH", env, os.Getenv(env), argv[0])
	}
	return argv, nil
}

var nameCounter atomic.Uint64

// endpointName returns a name no other cell or concurrent run uses. The
// t.Cleanup it registers removes every file of the name from /dev/shm.
func endpointName(t *testing.T, kind string) string {
	t.Helper()
	name := fmt.Sprintf("interop-%s-%d-%d", kind, os.Getpid(), nameCounter.Add(1))
	t.Cleanup(func() {
		for _, f := range shmFiles(t, name) {
			os.Remove(f)
		}
	})
	return name
}

// shmFiles lists the files of a queue or a conn called name. The "." in each
// pattern keeps name-1 from matching name-10.
func shmFiles(t *testing.T, name string) []string {
	t.Helper()
	var out []string
	for _, p := range []string{"go-shm-" + name, "go-shm-" + name + ".*", "go-ipc-" + name + ".*"} {
		m, err := filepath.Glob(filepath.Join("/dev/shm", p))
		require.NoError(t, err)
		out = append(out, m...)
	}
	return out
}

// A proc is one running peer.
type proc struct {
	label  string
	cmd    *exec.Cmd
	stderr bytes.Buffer
	ready  chan struct{}
	// stdoutDone closes when stdout reaches end-of-file.
	stdoutDone chan struct{}
	lines      []string
	waitOnce   sync.Once
	waitErr    error
}

func startPeer(t *testing.T, ctx context.Context, lang string, args ...string) *proc {
	t.Helper()
	argv, env := peerCommand(t, lang)
	p := &proc{
		label:      lang + " " + strings.Join(args, " "),
		ready:      make(chan struct{}),
		stdoutDone: make(chan struct{}),
	}
	p.cmd = exec.CommandContext(ctx, argv[0], append(argv[1:], args...)...)
	p.cmd.Env = append(os.Environ(), env...)
	p.cmd.Stderr = &p.stderr
	stdout, err := p.cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, p.cmd.Start(), "start %s peer %q", lang, argv)

	go func() {
		defer close(p.stdoutDone)
		sc := bufio.NewScanner(stdout)
		sentReady := false
		for sc.Scan() {
			line := sc.Text()
			p.lines = append(p.lines, line)
			if line == "ready" && !sentReady {
				sentReady = true
				close(p.ready)
			}
		}
	}()
	t.Cleanup(func() {
		p.cmd.Process.Kill()
		p.wait()
	})
	return p
}

// wait reaps the process. It reads stdout to the end before cmd.Wait, as
// StdoutPipe requires.
func (p *proc) wait() error {
	p.waitOnce.Do(func() {
		<-p.stdoutDone
		p.waitErr = p.cmd.Wait()
	})
	return p.waitErr
}

func (p *proc) report() string {
	return fmt.Sprintf("--- %s\nstdout: %q\nstderr:\n%s", p.label, p.lines, p.stderr.String())
}

// waitReady blocks until p prints its ready line. A timeout or an early exit
// kills p and fails the test.
func (p *proc) waitReady(t *testing.T) {
	t.Helper()
	timer := time.NewTimer(readyTimeout)
	defer timer.Stop()
	select {
	case <-p.ready:
		return
	case <-p.stdoutDone:
		err := p.wait()
		require.FailNow(t, "peer exited before it printed ready", "exit: %v\n%s", err, p.report())
	case <-timer.C:
		p.cmd.Process.Kill()
		p.wait()
		require.FailNow(t, "peer did not print ready", "waited %s\n%s", readyTimeout, p.report())
	}
}

// runCell waits for the client peers, then for the server peer. A failed
// client kills the server, so the server does not wait for its own timeout.
// The test fails with every peer's output when any peer fails.
func runCell(t *testing.T, server *proc, clients []*proc, wantLine string) {
	t.Helper()
	failed := false
	for _, c := range clients {
		if c.wait() != nil {
			failed = true
		}
	}
	if failed {
		server.cmd.Process.Kill()
	}
	serverErr := server.wait()
	failed = failed || serverErr != nil

	var out strings.Builder
	for _, p := range append([]*proc{server}, clients...) {
		fmt.Fprintf(&out, "%s\nexit: %v\n", p.report(), p.wait())
	}
	require.False(t, failed, "a peer failed\n%s", out.String())
	if wantLine != "" {
		require.Equal(t, []string{"ready", wantLine}, server.lines, "server stdout\n%s", out.String())
	}
}

func requireNoLeftovers(t *testing.T, name string) {
	t.Helper()
	assert.Empty(t, shmFiles(t, name), "the creating peer must unlink every file of %q", name)
}

func cellContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), cellTimeout)
	t.Cleanup(cancel)
	return ctx
}

func TestInterop(t *testing.T) {
	ls := langs(t)

	t.Run("queue", func(t *testing.T) {
		for _, rl := range ls {
			for _, sl := range ls {
				t.Run(sl+"->"+rl, func(t *testing.T) {
					t.Parallel()
					ctx := cellContext(t)
					name := endpointName(t, "queue")
					recv := startPeer(t, ctx, rl, "recv", name, strconv.Itoa(queueCount), strconv.Itoa(capacity))
					recv.waitReady(t)
					send := startPeer(t, ctx, sl, "send", name, "0", strconv.Itoa(queueCount))
					runCell(t, recv, []*proc{send}, fmt.Sprintf("ok %d", queueCount))
					requireNoLeftovers(t, name)
				})
			}
		}
	})

	t.Run("mpsc", func(t *testing.T) {
		for _, rl := range ls {
			t.Run("all->"+rl, func(t *testing.T) {
				t.Parallel()
				ctx := cellContext(t)
				name := endpointName(t, "mpsc")
				total := mpscCount * len(ls)
				recv := startPeer(t, ctx, rl, "recv", name, strconv.Itoa(total), strconv.Itoa(capacity))
				recv.waitReady(t)
				var senders []*proc
				for i, sl := range ls {
					senders = append(senders, startPeer(t, ctx, sl, "send", name, strconv.Itoa(i), strconv.Itoa(mpscCount)))
				}
				runCell(t, recv, senders, fmt.Sprintf("ok %d", total))
				requireNoLeftovers(t, name)
			})
		}
	})

	t.Run("conn", func(t *testing.T) {
		for _, ll := range ls {
			for _, dl := range ls {
				t.Run(dl+"->"+ll, func(t *testing.T) {
					t.Parallel()
					ctx := cellContext(t)
					name := endpointName(t, "conn")
					listen := startPeer(t, ctx, ll, "listen-echo", name, strconv.Itoa(capacity))
					listen.waitReady(t)
					dial := startPeer(t, ctx, dl, "dial-check", name, strconv.Itoa(streamBytes))
					runCell(t, listen, []*proc{dial}, "")
					requireNoLeftovers(t, name)
				})
			}
		}
	})
}
