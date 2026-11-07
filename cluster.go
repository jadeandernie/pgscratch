package pgscratch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Layout under BaseDir:
//
//	p-XXXX/            a per-process cluster
//	  lock             flock held by the owner and its watchdog while alive
//	  owned            written once the lock is held; marks the dir sweepable
//	  data/            PGDATA, also the unix socket directory
//	  server.log
//	  locks/           per-template build locks
//	shared-NAME/       a shared cluster (same inner layout, no lock/owned)
//	shared-NAME.lock   flock held around a shared cluster's start and refcount
//	shared-NAME.users  shared flock held by each attached process's watchdog
const (
	ownPrefix       = "p-"
	sharedPrefix    = "shared-"
	ownedMarker     = "owned"
	persistMarker   = "persist"
	maxSocketPath   = 100 // sun_path is 104 bytes on macOS, 108 on Linux
	unownedSweepAge = 10 * time.Minute
)

func (c *Cluster) startOwn() error {
	o := c.opts
	sweep(o.BaseDir)
	dir, err := os.MkdirTemp(o.BaseDir, ownPrefix)
	if err != nil {
		return fmt.Errorf("pgscratch: %w", err)
	}
	lock, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		os.RemoveAll(dir)
		return fmt.Errorf("pgscratch: %w", err)
	}
	if ok, err := lockFile(lock, true, false); !ok {
		lock.Close()
		os.RemoveAll(dir)
		return fmt.Errorf("pgscratch: locking new cluster dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ownedMarker), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		lock.Close()
		os.RemoveAll(dir)
		return fmt.Errorf("pgscratch: %w", err)
	}
	c.dir = dir
	c.dataDir = filepath.Join(dir, "data")
	// The watchdog starts before initdb so that every later failure, or the
	// process dying mid-start, still ends in cleanup.
	if err := c.spawnWatchdog(watchdogConfig{Mode: "own", Dir: dir, BinDir: c.binDir}, lock); err != nil {
		lock.Close()
		os.RemoveAll(dir)
		return err
	}
	// Keep our copy of the lock open too: the dir must not look abandoned
	// while this process lives, even if the watchdog is killed.
	c.ownLock = lock
	ctx, cancel := context.WithTimeout(context.Background(), o.StartTimeout)
	defer cancel()
	if err := c.initdb(ctx); err != nil {
		c.abort()
		return err
	}
	if err := c.startServer(ctx); err != nil {
		c.abort()
		return err
	}
	return nil
}

// abort releases a cluster whose start failed and waits for the cleanup.
func (c *Cluster) abort() {
	c.lifeline.Close()
	c.watchdog.Wait()
	if c.ownLock != nil {
		c.ownLock.Close()
	}
}

func (c *Cluster) attachShared() error {
	o := c.opts
	if !sharedNamePattern.MatchString(o.Shared) {
		return fmt.Errorf("pgscratch: shared cluster name %q must match %s", o.Shared, sharedNamePattern)
	}
	sweep(o.BaseDir)
	c.shared = true
	c.dir = filepath.Join(o.BaseDir, sharedPrefix+o.Shared)
	c.dataDir = filepath.Join(c.dir, "data")
	ctx, cancel := context.WithTimeout(context.Background(), o.StartTimeout)
	defer cancel()

	lock, err := os.OpenFile(c.dir+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("pgscratch: %w", err)
	}
	defer lock.Close()
	if err := lockWait(ctx, lock); err != nil {
		return fmt.Errorf("pgscratch: waiting for shared cluster %q: %w", o.Shared, err)
	}
	defer unlockFile(lock)

	users, err := os.OpenFile(c.dir+".users", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("pgscratch: %w", err)
	}
	attached := false
	defer func() {
		if !attached {
			users.Close()
		}
	}()
	// Nobody else can attach while we hold the main lock, so "no users"
	// stays true until we register.
	noUsers := usersGone(c.dir + ".users")

	if port, socketDir, ok := runningServer(ctx, c.dataDir, o.User); ok {
		major, _ := dataDirMajor(c.dataDir)
		if o.Major != 0 && major != o.Major {
			return fmt.Errorf("pgscratch: shared cluster %q runs PostgreSQL %d, want %d", o.Shared, major, o.Major)
		}
		c.port, c.socketDir, c.major = port, socketDir, major
		if noUsers {
			c.dropLeftovers(ctx)
		}
	} else {
		stopServer(c.binDir, c.dataDir, true)
		major, _ := dataDirMajor(c.dataDir)
		reuse := o.Persist && major == c.major
		if !reuse {
			if err := os.RemoveAll(c.dir); err != nil {
				return fmt.Errorf("pgscratch: %w", err)
			}
			if err := os.MkdirAll(c.dir, 0o700); err != nil {
				return fmt.Errorf("pgscratch: %w", err)
			}
			if err := c.initdb(ctx); err != nil {
				os.RemoveAll(c.dir)
				return err
			}
		}
		if o.Persist {
			os.WriteFile(filepath.Join(c.dir, persistMarker), nil, 0o600)
		} else {
			os.Remove(filepath.Join(c.dir, persistMarker))
		}
		if err := c.startServer(ctx); err != nil {
			return err
		}
		if reuse {
			c.dropLeftovers(ctx)
		}
	}
	if ok, err := lockFile(users, false, false); !ok {
		return fmt.Errorf("pgscratch: registering with shared cluster: %v", err)
	}
	if err := c.spawnWatchdog(watchdogConfig{Mode: "shared", Dir: c.dir, BinDir: c.binDir}, users); err != nil {
		return err
	}
	// The watchdog's inherited descriptor now carries our user lock; ours
	// must go, or Stop could never see this process leave.
	users.Close()
	attached = true
	return nil
}

func lockWait(ctx context.Context, f *os.File) error {
	for {
		ok, err := lockFile(f, true, false)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// usersGone reports whether no process holds a user lock on path.
func usersGone(path string) bool {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return false
	}
	defer f.Close()
	ok, _ := lockFile(f, true, false)
	return ok
}

func (c *Cluster) initdb(ctx context.Context) error {
	args := []string{"-D", c.dataDir, "-U", c.opts.User, "-A", "trust", "-E", "UTF8", "--locale=C", "--no-sync"}
	if c.major >= 14 {
		args = append(args, "--no-instructions")
	}
	if out, err := c.run(ctx, "initdb", args...); err != nil {
		return fmt.Errorf("pgscratch: initdb: %v: %s", err, lastLines(out, 20))
	}
	if err := os.WriteFile(filepath.Join(c.dir, "user"), []byte(c.opts.User), 0o600); err != nil {
		return fmt.Errorf("pgscratch: %w", err)
	}
	socketDir := c.dataDir
	if len(filepath.Join(socketDir, ".s.PGSQL.65535")) > maxSocketPath {
		socketDir = ""
	}
	var conf strings.Builder
	conf.WriteString("\n# pgscratch: throwaway cluster, durability off\n")
	fmt.Fprintf(&conf, "listen_addresses = '127.0.0.1'\n")
	fmt.Fprintf(&conf, "unix_socket_directories = %s\n", QuoteLiteral(socketDir))
	conf.WriteString("fsync = off\nsynchronous_commit = off\nfull_page_writes = off\n")
	fmt.Fprintf(&conf, "max_connections = %d\n", c.opts.MaxConnections)
	conf.WriteString("shared_buffers = 64MB\n")
	conf.WriteString("logging_collector = off\nlog_min_messages = warning\n")
	for _, line := range c.opts.Settings {
		conf.WriteString(line + "\n")
	}
	f, err := os.OpenFile(filepath.Join(c.dataDir, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("pgscratch: %w", err)
	}
	defer f.Close()
	if _, err := f.WriteString(conf.String()); err != nil {
		return fmt.Errorf("pgscratch: %w", err)
	}
	return nil
}

// startServer starts the server on a free loopback port, retrying with a new
// port when another process takes the one picked.
func (c *Cluster) startServer(ctx context.Context) error {
	logFile := filepath.Join(c.dir, "server.log")
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		port, err := freePort()
		if err != nil {
			return fmt.Errorf("pgscratch: picking a port: %w", err)
		}
		timeout := 60
		if deadline, ok := ctx.Deadline(); ok {
			timeout = max(1, int(time.Until(deadline).Seconds()))
		}
		out, err := c.run(ctx, "pg_ctl", "start", "-D", c.dataDir, "-l", logFile, "-w", "-t", strconv.Itoa(timeout), "-o", "-p "+strconv.Itoa(port))
		if err == nil {
			c.port = port
			_, c.socketDir, _ = readPostmasterPid(c.dataDir)
			return nil
		}
		logTail, _ := os.ReadFile(logFile)
		lastErr = fmt.Errorf("pgscratch: pg_ctl start: %v: %s\n%s", err, strings.TrimSpace(string(out)), lastLines(logTail, 20))
		stopServer(c.binDir, c.dataDir, false)
		if !bytes.Contains(logTail, []byte("could not bind")) && !bytes.Contains(logTail, []byte("could not create any TCP/IP sockets")) && !bytes.Contains(logTail, []byte("already in use")) {
			break
		}
		if ctx.Err() != nil {
			break
		}
	}
	return lastErr
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// run runs a server binary with no inherited stdio, in its own process
// group, and returns its combined output.
func (c *Cluster) run(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, filepath.Join(c.binDir, name), args...)
	cmd.SysProcAttr = detached()
	cmd.Env = append(os.Environ(), "LC_ALL=C", "LANG=C", "PGTZ=UTC")
	cmd.WaitDelay = 5 * time.Second
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	return out.Bytes(), err
}

// readPostmasterPid reads the port and socket directory from a running
// server's postmaster.pid.
func readPostmasterPid(dataDir string) (pid int, socketDir string, port int) {
	raw, err := os.ReadFile(filepath.Join(dataDir, "postmaster.pid"))
	if err != nil {
		return 0, "", 0
	}
	lines := strings.Split(string(raw), "\n")
	if len(lines) > 0 {
		pid, _ = strconv.Atoi(strings.TrimSpace(lines[0]))
	}
	if len(lines) > 3 {
		port, _ = strconv.Atoi(strings.TrimSpace(lines[3]))
	}
	if len(lines) > 4 {
		socketDir = strings.TrimSpace(lines[4])
	}
	return pid, socketDir, port
}

// runningServer reports whether dataDir has a server that accepts
// connections, and where.
func runningServer(ctx context.Context, dataDir, user string) (int, string, bool) {
	pid, socketDir, port := readPostmasterPid(dataDir)
	if pid == 0 || port == 0 || !processAlive(pid) {
		return 0, "", false
	}
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := dial(pingCtx, "tcp", fmt.Sprintf("127.0.0.1:%d", port), user, "postgres", "")
	if err != nil {
		return 0, "", false
	}
	defer conn.Close()
	if err := conn.Exec(pingCtx, "SELECT 1"); err != nil {
		return 0, "", false
	}
	return port, socketDir, true
}

// stopServer stops the server running in dataDir, if any, immediately.
// With verify set it first proves the postmaster.pid's server is the one
// using dataDir (a stale pid file can name a reused pid); the watchdog,
// which started the server itself, skips that.
func stopServer(binDir, dataDir string, verify bool) {
	pid, _, port := readPostmasterPid(dataDir)
	if pid == 0 || !processAlive(pid) {
		return
	}
	if verify && !serverUsesDataDir(port, dataDir) {
		return
	}
	if binDir != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, filepath.Join(binDir, "pg_ctl"), "stop", "-D", dataDir, "-m", "immediate", "-w")
		cmd.SysProcAttr = detached()
		cmd.WaitDelay = 5 * time.Second
		if cmd.Run() == nil {
			return
		}
	}
	signalProcess(pid, syscall.SIGQUIT)
	for i := 0; i < 100 && processAlive(pid); i++ {
		time.Sleep(50 * time.Millisecond)
	}
}

func serverUsesDataDir(port int, dataDir string) bool {
	if port == 0 {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	user := defaultUser
	if raw, err := os.ReadFile(filepath.Join(filepath.Dir(dataDir), "user")); err == nil && len(raw) > 0 {
		user = string(raw)
	}
	conn, err := dial(ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", port), user, "postgres", "")
	if err != nil {
		return false
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, "SHOW data_directory")
	if err != nil || len(rows) != 1 {
		return false
	}
	return samePath(rows[0][0], dataDir)
}

func samePath(a, b string) bool {
	ra, errA := filepath.EvalSymlinks(a)
	rb, errB := filepath.EvalSymlinks(b)
	if errA != nil || errB != nil {
		return filepath.Clean(a) == filepath.Clean(b)
	}
	return ra == rb
}

func dataDirMajor(dataDir string) (int, error) {
	raw, err := os.ReadFile(filepath.Join(dataDir, "PG_VERSION"))
	if err != nil {
		return 0, err
	}
	major, err := strconv.Atoi(strings.SplitN(strings.TrimSpace(string(raw)), ".", 2)[0])
	if err != nil {
		return 0, fmt.Errorf("bad PG_VERSION %q", raw)
	}
	return major, nil
}

// sweep removes clusters whose owners are gone: per-process dirs whose lock
// nobody holds, and shared clusters nobody is attached to. It never waits
// on a lock.
func sweep(baseDir string) {
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		path := filepath.Join(baseDir, name)
		switch {
		case entry.IsDir() && strings.HasPrefix(name, ownPrefix):
			sweepOwn(path)
		case !entry.IsDir() && strings.HasPrefix(name, sharedPrefix) && strings.HasSuffix(name, ".lock"):
			sweepShared(strings.TrimSuffix(path, ".lock"))
		}
	}
}

func sweepOwn(dir string) {
	lock, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_RDWR, 0)
	if err != nil {
		// No lock file: either a start that died before creating it or a
		// half-removed dir. Leave fresh ones alone; a start in progress
		// creates the lock right after the dir.
		if info, statErr := os.Stat(dir); statErr == nil && time.Since(info.ModTime()) > unownedSweepAge {
			os.RemoveAll(dir)
		}
		return
	}
	defer lock.Close()
	if _, err := os.Stat(filepath.Join(dir, ownedMarker)); err != nil {
		// The owner writes "owned" after taking the lock; until then an
		// unheld lock does not mean the owner is gone.
		if info, statErr := os.Stat(dir); statErr != nil || time.Since(info.ModTime()) < unownedSweepAge {
			return
		}
	}
	if ok, _ := lockFile(lock, true, false); !ok {
		return
	}
	stopServer("", filepath.Join(dir, "data"), true)
	os.RemoveAll(dir)
}

func sweepShared(dir string) {
	lock, err := os.OpenFile(dir+".lock", os.O_RDWR, 0)
	if err != nil {
		return
	}
	defer lock.Close()
	if ok, _ := lockFile(lock, true, false); !ok {
		return
	}
	defer unlockFile(lock)
	if !usersGone(dir + ".users") {
		return
	}
	stopServer("", filepath.Join(dir, "data"), true)
	if _, err := os.Stat(filepath.Join(dir, persistMarker)); err != nil {
		os.RemoveAll(dir)
	}
}

// dropLeftovers drops test databases and half-built templates that crashed
// processes left in a shared cluster. Only called with no other users.
func (c *Cluster) dropLeftovers(ctx context.Context) {
	conn, err := c.Connect(ctx, "postgres")
	if err != nil {
		return
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, "SELECT datname FROM pg_database WHERE datname LIKE 'pgs\\_t\\_%' OR datname LIKE 'pgs\\_build\\_%'")
	if err != nil {
		return
	}
	for _, row := range rows {
		c.dropWith(ctx, conn, row[0])
	}
}

func lastLines(out []byte, n int) string {
	lines := strings.Split(strings.TrimRight(string(out), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

var errStopped = errors.New("pgscratch: cluster stopped")
