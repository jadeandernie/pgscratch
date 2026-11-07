// Package pgscratch starts throwaway PostgreSQL clusters for tests.
//
// A cluster is an initdb'd data directory with durability turned off
// (fsync, synchronous_commit and full_page_writes are off), listening on a
// free loopback port and on a unix socket inside its data directory. Tests
// get their own databases from it, either empty (NewDatabase) or cloned from
// a template that was migrated and seeded once (Template).
//
// Clusters clean up after themselves. Each one has a watchdog process that
// holds one end of a pipe; when the owning process exits for any reason,
// including SIGKILL, the pipe closes and the watchdog stops the server and
// deletes its directory. A marker file naming the owner pid lets the next
// start sweep anything the watchdog could not remove.
//
// No lock is held while a test runs. A per-process cluster is started once
// (sync.Once); a shared cluster takes a file lock only around startup and its
// reference count; templates are built under a per-template lock; database
// creation is fully concurrent.
package pgscratch

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Environment variables read when the matching Options field is empty.
const (
	EnvBin     = "PGSCRATCH_BIN"     // directory holding initdb, pg_ctl and postgres
	EnvDir     = "PGSCRATCH_DIR"     // parent directory of cluster directories
	EnvShared  = "PGSCRATCH_SHARED"  // name of a cluster shared across processes
	EnvPersist = "PGSCRATCH_PERSIST" // 1 keeps a shared cluster's data between runs
)

const (
	defaultMaxConnections = 300
	defaultUser           = "postgres"
	defaultStartTimeout   = 2 * time.Minute
)

// Options configures a cluster. The zero value is usable.
type Options struct {
	// BinDir holds initdb, pg_ctl and postgres (and psql/pg_restore for
	// Dump steps). Empty: $PGSCRATCH_BIN, then PATH, then common install
	// locations (Homebrew, apt, yum, Postgres.app).
	BinDir string
	// Major, when non-zero, is the only Postgres major version accepted.
	Major int
	// BaseDir is the parent of cluster directories. Empty: $PGSCRATCH_DIR,
	// then a tmpfs such as /dev/shm/pgscratch on Linux when one has room,
	// then <os.TempDir()>/pgscratch.
	BaseDir string
	// Shared, when non-empty, names a cluster shared by every process that
	// uses the same BaseDir and name. The first process starts it, the last
	// one out stops it. The first starter's settings win. Empty: $PGSCRATCH_SHARED.
	Shared string
	// Persist keeps a shared cluster's data directory, stopped, after the
	// last process exits, so templates survive into the next run. It
	// implies Shared ("default" when no name is given). It trades disk for
	// speed and is off unless set here or with PGSCRATCH_PERSIST=1.
	Persist bool
	// MaxConnections is the server's max_connections (default 300).
	MaxConnections int
	// Settings are extra postgresql.conf lines, such as "work_mem = '64MB'".
	Settings []string
	// User is the superuser name (default "postgres"). Authentication is
	// trust on loopback and the socket, so there is no password.
	User string
	// StartTimeout bounds initdb plus server start (default two minutes).
	StartTimeout time.Duration
}

func (o Options) resolved() Options {
	if o.BinDir == "" {
		o.BinDir = strings.TrimSpace(os.Getenv(EnvBin))
	}
	if o.BaseDir == "" {
		o.BaseDir = defaultBaseDir()
	}
	if o.Shared == "" {
		o.Shared = strings.TrimSpace(os.Getenv(EnvShared))
	}
	if !o.Persist {
		o.Persist = truthy(os.Getenv(EnvPersist))
	}
	if o.Persist && o.Shared == "" {
		o.Shared = "default"
	}
	if o.MaxConnections <= 0 {
		o.MaxConnections = defaultMaxConnections
	}
	if o.User == "" {
		o.User = defaultUser
	}
	if o.StartTimeout <= 0 {
		o.StartTimeout = defaultStartTimeout
	}
	return o
}

func defaultBaseDir() string {
	if dir := strings.TrimSpace(os.Getenv(EnvDir)); dir != "" {
		return dir
	}
	if dir := tmpfsBaseDir(); dir != "" {
		return dir
	}
	return filepath.Join(os.TempDir(), "pgscratch")
}

func truthy(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "t", "yes", "y", "on":
		return true
	}
	return false
}

// Cluster is a running throwaway Postgres server. Its methods are safe for
// concurrent use.
type Cluster struct {
	opts      Options
	binDir    string
	major     int
	dir       string
	dataDir   string
	socketDir string
	port      int
	shared    bool

	processWide bool
	lifeline    *os.File // write end; closing it releases the cluster
	watchdog    *exec.Cmd
	ownLock     *os.File // per-process clusters: liveness lock, see startOwn
	stopOnce    sync.Once
	stopErr     error

	poolOnce sync.Once
	pool     chan *Conn

	tmplMu    sync.Mutex
	templates map[string]*templateEntry
}

// Host is the loopback address the server listens on.
func (c *Cluster) Host() string { return "127.0.0.1" }

// Port is the server's TCP port.
func (c *Cluster) Port() int { return c.port }

// SocketDir is the unix socket directory (the data directory), or "" when
// its path was too long for a socket and only TCP is available.
func (c *Cluster) SocketDir() string { return c.socketDir }

// User is the superuser name.
func (c *Cluster) User() string { return c.opts.User }

// Major is the server's major version.
func (c *Cluster) Major() int { return c.major }

// Dir is the cluster's directory; the data directory is Dir()/data.
func (c *Cluster) Dir() string { return c.dir }

// BinDir is the directory the server binaries came from.
func (c *Cluster) BinDir() string { return c.binDir }

// DSN returns a postgres:// URL for database over TCP.
func (c *Cluster) DSN(database string) string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.User(c.opts.User),
		Host:     c.Host() + ":" + strconv.Itoa(c.port),
		Path:     "/" + database,
		RawQuery: "sslmode=disable",
	}
	return u.String()
}

// keywordDSN is the libpq keyword form, used for the bundled client tools.
func (c *Cluster) keywordDSN(database string) string {
	return fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=disable", c.Host(), c.port, c.opts.User, database)
}

var processClusters struct {
	mu sync.Mutex
	m  map[string]*processCluster
}

type processCluster struct {
	once    sync.Once
	cluster *Cluster
	err     error
}

// Start returns this process's cluster for opts, starting it on first use.
// Calls with equal options share one cluster, which lives until the process
// exits; Stop on it is a no-op. A failed start is not retried.
func Start(opts Options) (*Cluster, error) {
	resolved := opts.resolved()
	key := fmt.Sprintf("%#v", resolved)
	processClusters.mu.Lock()
	if processClusters.m == nil {
		processClusters.m = map[string]*processCluster{}
	}
	entry := processClusters.m[key]
	if entry == nil {
		entry = &processCluster{}
		processClusters.m[key] = entry
	}
	processClusters.mu.Unlock()
	entry.once.Do(func() {
		entry.cluster, entry.err = New(opts)
		if entry.cluster != nil {
			entry.cluster.processWide = true
		}
	})
	return entry.cluster, entry.err
}

// New starts a cluster the caller owns and must Stop. Without Stop it is
// still removed when the process exits.
func New(opts Options) (*Cluster, error) {
	if !platformSupported {
		return nil, errors.New("pgscratch: unsupported platform")
	}
	o := opts.resolved()
	binDir, major, err := findBinDir(o)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(o.BaseDir, 0o700); err != nil {
		return nil, fmt.Errorf("pgscratch: %w", err)
	}
	c := &Cluster{opts: o, binDir: binDir, major: major, templates: map[string]*templateEntry{}}
	if o.Shared != "" {
		err = c.attachShared()
	} else {
		err = c.startOwn()
	}
	if err != nil {
		return nil, err
	}
	return c, nil
}

// Stop stops a cluster from New and deletes its directory (a shared cluster
// only when this was its last user). It waits for the cleanup to finish.
func (c *Cluster) Stop() error {
	if c.processWide {
		return nil
	}
	c.stopOnce.Do(func() {
		pool := c.adminPool()
	drain:
		for {
			select {
			case conn := <-pool:
				conn.Close()
			default:
				break drain
			}
		}
		_ = c.lifeline.Close()
		c.stopErr = c.watchdog.Wait()
		if c.ownLock != nil {
			c.ownLock.Close()
		}
	})
	return c.stopErr
}

var sharedNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)
