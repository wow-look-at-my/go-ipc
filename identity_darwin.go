package ipc

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// sZOMB is the p_stat value of a process that exited and was not reaped.
const sZOMB = 5

// startTime reads the start time of pid from the kernel, in microseconds.
func startTime(pid int) (uint64, error) {
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if errors.Is(err, unix.ESRCH) || errors.Is(err, unix.EIO) {
		// The kernel answers an empty record for a pid with no process, which x/sys reports as EIO.
		return 0, errProcGone
	}
	if err != nil {
		return 0, err
	}
	if int(kp.Proc.P_pid) != pid || kp.Proc.P_stat == sZOMB {
		return 0, errProcGone
	}
	tv := kp.Proc.P_starttime
	return uint64(tv.Sec)*1_000_000 + uint64(tv.Usec), nil
}

// openExit returns a kqueue that holds a NOTE_EXIT filter for the process
// id names. The kqueue becomes readable when that process exits.
func openExit(id procID) (exitWaiter, error) {
	kq, err := unix.Kqueue()
	if err != nil {
		return nil, fmt.Errorf("ipc: kqueue: %w", err)
	}
	change := unix.Kevent_t{
		Ident:  uint64(id.pid()),
		Filter: unix.EVFILT_PROC,
		Flags:  unix.EV_ADD | unix.EV_ONESHOT,
		Fflags: unix.NOTE_EXIT,
	}
	if _, err := unix.Kevent(kq, []unix.Kevent_t{change}, nil, nil); err != nil {
		unix.Close(kq)
		if errors.Is(err, unix.ESRCH) {
			return nil, errProcGone
		}
		return nil, fmt.Errorf("ipc: kevent: %w", err)
	}
	// The filter follows whatever process holds the pid now.
	start, err := startTime(id.pid())
	if err == nil && !id.matches(start) {
		err = errProcGone
	}
	if err != nil {
		unix.Close(kq)
		return nil, err
	}
	return newPollWaiter(kq, "kqueue", kqueueExited)
}

func kqueueExited(fd int) (bool, error) {
	var events [1]unix.Kevent_t
	n, err := unix.Kevent(fd, nil, events[:], &unix.Timespec{})
	if err != nil {
		if errors.Is(err, unix.EINTR) {
			return false, nil
		}
		return false, err
	}
	return n > 0, nil
}
