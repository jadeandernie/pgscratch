//go:build !unix

package pgscratch

import (
	"errors"
	"os"
	"syscall"
)

const platformSupported = false

func detached() *syscall.SysProcAttr    { return nil }
func processAlive(int) bool             { return true }
func signalProcess(int, syscall.Signal) {}
func lockFile(*os.File, bool, bool) (bool, error) {
	return false, errors.New("pgscratch: unsupported platform")
}
func unlockFile(*os.File)    {}
func setCloseOnExec(uintptr) {}
