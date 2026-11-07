package pgscratch

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// envWatchdog carries a watchdog's configuration to the re-executed binary.
const envWatchdog = "PGSCRATCH_WATCHDOG"

// The watchdog is this same executable run again with envWatchdog set. The
// init hook below catches it before main (or the test runner) starts. It
// inherits:
//
//	fd 3  the read end of the lifeline pipe; only the owner holds the write end
//	fd 4  the cluster's lock (own mode) or the user lock (shared mode)
//	fd 5  a pipe it writes one byte to once it is ready
//
// When fd 3 reports EOF, the owner has called Stop or has exited, however
// it exited, and the watchdog releases the cluster.
type watchdogConfig struct {
	Mode   string // "own" or "shared"
	Dir    string
	BinDir string
}

func init() {
	raw, ok := os.LookupEnv(envWatchdog)
	if !ok {
		return
	}
	os.Exit(runWatchdog(raw))
}

func runWatchdog(raw string) int {
	var cfg watchdogConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return 2
	}
	lifeline := os.NewFile(3, "lifeline")
	held := os.NewFile(4, "lock")
	ready := os.NewFile(5, "ready")
	if lifeline == nil || held == nil || ready == nil {
		return 2
	}
	// Children (pg_ctl stop) must not inherit these.
	for _, fd := range []uintptr{3, 4, 5} {
		setCloseOnExec(fd)
	}
	ready.Write([]byte{1})
	ready.Close()
	io.Copy(io.Discard, lifeline)

	dataDir := filepath.Join(cfg.Dir, "data")
	switch cfg.Mode {
	case "own":
		stopServer(cfg.BinDir, dataDir, false)
		os.RemoveAll(cfg.Dir)
		held.Close()
	case "shared":
		lock, err := os.OpenFile(cfg.Dir+".lock", os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return 1
		}
		for {
			ok, err := lockFile(lock, true, false)
			if err != nil {
				return 1
			}
			if ok {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		held.Close()
		if usersGone(cfg.Dir + ".users") {
			stopServer(cfg.BinDir, dataDir, false)
			if _, err := os.Stat(filepath.Join(cfg.Dir, persistMarker)); err != nil {
				os.RemoveAll(cfg.Dir)
			}
		}
		unlockFile(lock)
		lock.Close()
	}
	return 0
}

// spawnWatchdog starts the watchdog for this cluster, handing it held, and
// keeps the lifeline's write end.
func (c *Cluster) spawnWatchdog(cfg watchdogConfig, held *os.File) error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("pgscratch: finding own executable for the watchdog: %w", err)
	}
	raw, _ := json.Marshal(cfg)
	lifeR, lifeW, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("pgscratch: %w", err)
	}
	readyR, readyW, err := os.Pipe()
	if err != nil {
		lifeR.Close()
		lifeW.Close()
		return fmt.Errorf("pgscratch: %w", err)
	}
	cmd := exec.Command(exe)
	cmd.Env = append(os.Environ(), envWatchdog+"="+string(raw))
	cmd.ExtraFiles = []*os.File{lifeR, held, readyW}
	cmd.SysProcAttr = detached()
	cmd.Dir = "/"
	err = cmd.Start()
	lifeR.Close()
	readyW.Close()
	if err != nil {
		lifeW.Close()
		readyR.Close()
		return fmt.Errorf("pgscratch: starting watchdog: %w", err)
	}
	readyR.SetReadDeadline(time.Now().Add(30 * time.Second))
	var b [1]byte
	_, err = io.ReadFull(readyR, b[:])
	readyR.Close()
	if err != nil {
		lifeW.Close()
		cmd.Process.Kill()
		cmd.Wait()
		return fmt.Errorf("pgscratch: watchdog did not start (%v); is the pgscratch init hook in this binary?", err)
	}
	c.lifeline = lifeW
	c.watchdog = cmd
	return nil
}
