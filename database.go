package pgscratch

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"
)

const (
	testDBPrefix  = "pgs_t_"
	buildDBPrefix = "pgs_build_"
	tmplDBPrefix  = "pgs_tpl_"
	maxIdentLen   = 63
	adminPoolSize = 16
)

// admin returns a connection to the postgres database from a small idle
// pool, dialling a new one when the pool is empty. Callers never wait for
// each other here.
func (c *Cluster) admin(ctx context.Context) (*Conn, error) {
	select {
	case conn := <-c.adminPool():
		return conn, nil
	default:
	}
	return c.Connect(ctx, "postgres")
}

func (c *Cluster) release(conn *Conn, err error) {
	if err != nil && errCode(err) == "" {
		// A transport error leaves the connection unusable.
		conn.Close()
		return
	}
	select {
	case c.adminPool() <- conn:
	default:
		conn.Close()
	}
}

func (c *Cluster) adminPool() chan *Conn {
	c.poolOnce.Do(func() { c.pool = make(chan *Conn, adminPoolSize) })
	return c.pool
}

// CreateDatabase creates database name, cloned from template (template0
// when empty). It retries while the template is briefly in use by another
// session.
func (c *Cluster) CreateDatabase(ctx context.Context, name, template string) error {
	if template == "" {
		template = "template0"
	}
	sql := "CREATE DATABASE " + QuoteIdentifier(name) + " TEMPLATE " + QuoteIdentifier(template)
	return c.adminExecRetry(ctx, sql, codeObjectInUse)
}

// DropDatabase drops database name if it exists, disconnecting its sessions.
func (c *Cluster) DropDatabase(ctx context.Context, name string) error {
	conn, err := c.admin(ctx)
	if err != nil {
		return err
	}
	err = c.dropWith(ctx, conn, name)
	c.release(conn, err)
	return err
}

func (c *Cluster) dropWith(ctx context.Context, conn *Conn, name string) error {
	if c.major >= 13 {
		return conn.Exec(ctx, "DROP DATABASE IF EXISTS "+QuoteIdentifier(name)+" WITH (FORCE)")
	}
	var err error
	for attempt := 0; attempt < 50; attempt++ {
		conn.Exec(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = "+QuoteLiteral(name))
		err = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+QuoteIdentifier(name))
		if errCode(err) != codeObjectInUse {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
	return err
}

// adminExecRetry runs sql on an admin connection, retrying with backoff
// while it fails with one of the retryable SQLSTATEs.
func (c *Cluster) adminExecRetry(ctx context.Context, sql string, retryable ...string) error {
	delay := 5 * time.Millisecond
	deadline := time.Now().Add(30 * time.Second)
	for {
		conn, err := c.admin(ctx)
		if err != nil {
			return err
		}
		err = conn.Exec(ctx, sql)
		c.release(conn, err)
		if err == nil || !contains(retryable, errCode(err)) || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, 200*time.Millisecond)
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s && s != "" {
			return true
		}
	}
	return false
}

// NewDatabase creates an empty database for tb and drops it when tb's test
// finishes. It returns a postgres:// URL; the database name is its path.
func (c *Cluster) NewDatabase(ctx context.Context, tb testing.TB) string {
	tb.Helper()
	name := c.testDatabaseName(tb)
	if err := c.CreateDatabase(ctx, name, ""); err != nil {
		tb.Fatalf("pgscratch: creating database: %v", err)
	}
	c.dropOnCleanup(tb, name)
	return c.DSN(name)
}

func (c *Cluster) dropOnCleanup(tb testing.TB, name string) {
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := c.DropDatabase(ctx, name); err != nil {
			tb.Logf("pgscratch: dropping database %s: %v", name, err)
		}
	})
}

// testDatabaseName is pgs_t_<test name>_<random>, within the 63-byte limit.
func (c *Cluster) testDatabaseName(tb testing.TB) string {
	suffix := randomHex(5)
	room := maxIdentLen - len(testDBPrefix) - len(suffix) - 1
	return testDBPrefix + sanitizeName(tb.Name(), room) + "_" + suffix
}

func sanitizeName(name string, limit int) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		if b.Len() >= limit {
			break
		}
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else if b.Len() > 0 && !strings.HasSuffix(b.String(), "_") {
			b.WriteByte('_')
		}
	}
	return strings.TrimSuffix(b.String(), "_")
}

func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		panic(fmt.Sprintf("pgscratch: crypto/rand: %v", err))
	}
	return hex.EncodeToString(buf)
}
