//go:build darwin

package ipc

import (
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func TestProbeDarwinSysctl(t *testing.T) {
	pid := os.Getpid()
	raw, err := unix.SysctlRaw("kern.proc.pid", pid)
	t.Logf("SysctlRaw err=%v len=%d", err, len(raw))
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	t.Logf("SysctlKinfoProc err=%v", err)
	if kp != nil {
		t.Logf("pid=%d stat=%d start=%v", kp.Proc.P_pid, kp.Proc.P_stat, kp.Proc.P_starttime)
	}
	start, err := startTime(pid)
	t.Logf("startTime=%d err=%v", start, err)
}
