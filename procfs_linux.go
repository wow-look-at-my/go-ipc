package ipc

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"

	"golang.org/x/sys/unix"
)

// A zombie counts as gone: it has exited, and only its parent's wait
// keeps the entry.
func startTime(pid int) (uint64, error) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, unix.ESRCH) {
		return 0, errProcGone
	}
	if err != nil {
		return 0, err
	}
	// The command name can hold spaces and parentheses, so the fields count from the last parenthesis.
	end := bytes.LastIndexByte(data, ')')
	fields := bytes.Fields(data[end+1:])
	if end < 0 || len(fields) < 20 {
		return 0, fmt.Errorf("ipc: cannot parse /proc/%d/stat", pid)
	}
	if state := string(fields[0]); state == "Z" || state == "X" {
		return 0, errProcGone
	}
	return strconv.ParseUint(string(fields[19]), 10, 64)
}

// procNS returns the inode of this process's pid namespace.
func procNS() (uint64, error) {
	var st unix.Stat_t
	if err := unix.Stat("/proc/self/ns/pid", &st); err != nil {
		return 0, fmt.Errorf("ipc: pid namespace: %w", err)
	}
	return st.Ino, nil
}

// openExit returns a pidfd for the process id names. The kernel makes a
// pidfd readable when its process exits.
func openExit(id procID) (exitWaiter, error) {
	fd, err := unix.PidfdOpen(id.pid(), 0)
	if errors.Is(err, unix.ESRCH) {
		return nil, errProcGone
	}
	if err != nil {
		return nil, fmt.Errorf("ipc: pidfd_open: %w", err)
	}
	// The pidfd pins whatever process holds the pid now.
	start, err := startTime(id.pid())
	if err == nil && !id.matches(start) {
		err = errProcGone
	}
	if err != nil {
		unix.Close(fd)
		return nil, err
	}
	return newPollWaiter(fd, "pidfd", pidfdExited)
}

func pidfdExited(fd int) (bool, error) {
	fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	n, err := unix.Poll(fds, 0)
	if err != nil {
		if errors.Is(err, unix.EINTR) {
			return false, nil
		}
		return false, err
	}
	return n > 0 && fds[0].Revents&unix.POLLIN != 0, nil
}
