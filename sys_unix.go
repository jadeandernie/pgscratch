//go:build unix

package pgscratch

import (
	"errors"
	"os"
	"syscall"
)

const platformSupported = true

// detached puts a child in its own process group, so a terminal's Ctrl-C
// reaches the test process but not the server or the watchdog.
func detached() *syscall.SysProcAttr { return &syscall.SysProcAttr{Setpgid: true} }

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func signalProcess(pid int, sig syscall.Signal) { _ = syscall.Kill(pid, sig) }

// lockFile takes a flock. Locks belong to the open file description and are
// released when every descriptor for it is closed, including by process
// death, so a held lock is proof that its holder is alive.
func lockFile(f *os.File, exclusive, wait bool) (bool, error) {
	how := syscall.LOCK_SH
	if exclusive {
		how = syscall.LOCK_EX
	}
	if !wait {
		how |= syscall.LOCK_NB
	}
	for {
		err := syscall.Flock(int(f.Fd()), how)
		switch {
		case err == nil:
			return true, nil
		case errors.Is(err, syscall.EINTR):
			continue
		case errors.Is(err, syscall.EWOULDBLOCK):
			return false, nil
		default:
			return false, err
		}
	}
}

func unlockFile(f *os.File) { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }

func setCloseOnExec(fd uintptr) { syscall.CloseOnExec(int(fd)) }
