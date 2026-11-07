package pgscratch

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"
)

// testOptions are the options every integration test uses; PGSCRATCH_BIN
// and PGSCRATCH_TEST_MAJOR pick the server.
func testOptions() Options {
	major, _ := strconv.Atoi(os.Getenv("PGSCRATCH_TEST_MAJOR"))
	return Options{Major: major}
}

func testCluster(t testing.TB) *Cluster {
	t.Helper()
	c, err := Start(testOptions())
	if errors.Is(err, ErrNotFound) {
		t.Skipf("no PostgreSQL binaries: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// shortTempDir is a temp dir with a short path, so unix sockets fit.
func shortTempDir(t testing.TB) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "pgs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func dbName(t testing.TB, dsn string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimPrefix(u.Path, "/")
}

func connectDSN(t testing.TB, c *Cluster, dsn string) *Conn {
	t.Helper()
	conn, err := c.Connect(context.Background(), dbName(t, dsn))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func queryOne(t testing.TB, conn *Conn, sql string) string {
	t.Helper()
	rows, err := conn.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("%s: got %v, want one value", sql, rows)
	}
	return rows[0][0]
}

func TestClusterSettings(t *testing.T) {
	c := testCluster(t)
	conn := connectDSN(t, c, c.DSN("postgres"))
	for setting, want := range map[string]string{
		"fsync":              "off",
		"synchronous_commit": "off",
		"full_page_writes":   "off",
		"max_connections":    strconv.Itoa(defaultMaxConnections),
		"listen_addresses":   "127.0.0.1",
	} {
		if got := queryOne(t, conn, "SHOW "+setting); got != want {
			t.Errorf("%s = %q, want %q", setting, got, want)
		}
	}
	if c.SocketDir() != "" && !strings.HasPrefix(c.SocketDir(), c.Dir()) {
		t.Errorf("socket dir %q is outside the cluster dir %q", c.SocketDir(), c.Dir())
	}
	if again, _ := Start(testOptions()); again != c {
		t.Error("Start with equal options returned a second cluster")
	}
}

func TestNewClusterOptionsAndStop(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a second cluster")
	}
	opts := testOptions()
	opts.BaseDir = shortTempDir(t)
	opts.MaxConnections = 222
	opts.Settings = []string{"work_mem = '7MB'"}
	c, err := New(opts)
	if errors.Is(err, ErrNotFound) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	conn, err := c.Connect(context.Background(), "postgres")
	if err != nil {
		t.Fatal(err)
	}
	if got := queryOne(t, conn, "SHOW max_connections"); got != "222" {
		t.Errorf("max_connections = %s", got)
	}
	if got := queryOne(t, conn, "SHOW work_mem"); got != "7MB" {
		t.Errorf("work_mem = %s", got)
	}
	if c.SocketDir() == "" {
		t.Errorf("no unix socket under the short base dir %s", opts.BaseDir)
	}
	conn.Close()
	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, err := os.Stat(c.Dir()); !os.IsNotExist(err) {
		t.Fatalf("cluster dir %s still exists after Stop: %v", c.Dir(), err)
	}
}

// TestParallelDatabases creates databases from many parallel tests at once.
func TestParallelDatabases(t *testing.T) {
	c := testCluster(t)
	for i := 0; i < 48; i++ {
		t.Run(fmt.Sprintf("db%02d", i), func(t *testing.T) {
			t.Parallel()
			dsn := c.NewDatabase(context.Background(), t)
			conn := connectDSN(t, c, dsn)
			if err := conn.Exec(context.Background(), "CREATE TABLE t (id int); INSERT INTO t VALUES (1), (2)"); err != nil {
				t.Fatal(err)
			}
			if got := queryOne(t, conn, "SELECT count(*) FROM t"); got != "2" {
				t.Fatalf("count = %s", got)
			}
		})
	}
}

func TestDatabaseDroppedOnCleanup(t *testing.T) {
	c := testCluster(t)
	var name string
	t.Run("inner", func(t *testing.T) {
		name = dbName(t, c.NewDatabase(context.Background(), t))
	})
	conn := connectDSN(t, c, c.DSN("postgres"))
	if got := queryOne(t, conn, "SELECT count(*) FROM pg_database WHERE datname = "+QuoteLiteral(name)); got != "0" {
		t.Fatalf("database %s survived its test", name)
	}
}

// TestTemplateParallel defines one template from many goroutines and clones
// it from many parallel tests; the template builds once.
func TestTemplateParallel(t *testing.T) {
	c := testCluster(t)
	var builds atomic.Int32
	define := func() (*Template, error) {
		return c.Template(context.Background(), "parallel_base",
			Func("v1", func(ctx context.Context, conn *Conn) error {
				builds.Add(1)
				return conn.Exec(ctx, "CREATE TABLE items (id int PRIMARY KEY, name text); INSERT INTO items SELECT g, 'item ' || g FROM generate_series(1, 1000) g")
			}))
	}
	var wg sync.WaitGroup
	templates := make([]*Template, 16)
	errs := make([]error, 16)
	for i := range templates {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			templates[i], errs[i] = define()
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Template call %d: %v", i, err)
		}
	}
	if n := builds.Load(); n != 1 {
		t.Fatalf("template built %d times, want 1", n)
	}
	tmpl := templates[0]
	t.Run("clones", func(t *testing.T) {
		for i := 0; i < 32; i++ {
			t.Run(fmt.Sprintf("clone%02d", i), func(t *testing.T) {
				t.Parallel()
				conn := connectDSN(t, c, tmpl.NewDatabase(context.Background(), t))
				if got := queryOne(t, conn, "SELECT count(*) FROM items"); got != "1000" {
					t.Fatalf("count = %s", got)
				}
				if err := conn.Exec(context.Background(), "DELETE FROM items WHERE id <= 500"); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
	admin := connectDSN(t, c, c.DSN("postgres"))
	if got := queryOne(t, admin, "SELECT datistemplate::text || '/' || datallowconn::text FROM pg_database WHERE datname = "+QuoteLiteral(tmpl.Database())); got != "true/false" {
		t.Fatalf("template flags = %s, want true/false", got)
	}
	if got := queryOne(t, admin, "SELECT count(*) FROM pg_stat_activity WHERE datname = "+QuoteLiteral(tmpl.Database())); got != "0" {
		t.Fatalf("%s connections remain on the template", got)
	}
}

func TestTemplateStepKinds(t *testing.T) {
	c := testCluster(t)
	ctx := context.Background()
	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	write("schema/001_users.sql", "CREATE TABLE users (id int PRIMARY KEY, email text NOT NULL);")
	write("schema/002_orders.sql", "CREATE TABLE orders (id int PRIMARY KEY, user_id int NOT NULL REFERENCES users(id), total numeric);\nCREATE SCHEMA audit; CREATE TABLE audit.events (id int, kind text);")
	usersCSV := write("users.csv", "\ufeffid,email\n1,a@example.test\n2,\"b,with comma\"\n3,c@example.test\n")
	write("seed/01-orders.csv", "id,user_id,total\n10,1,9.50\n11,2,1.25\n")
	write("seed/audit.events.csv", "kind,id\nlogin,1\n")
	var gz strings.Builder
	zw := gzip.NewWriter(&gz)
	zw.Write([]byte("id,user_id,total\n12,3,4.00\n"))
	zw.Close()
	write("seed/02-orders.csv.gz", gz.String())
	embedded := fstest.MapFS{"extra/view.sql": {Data: []byte("CREATE VIEW order_totals AS SELECT user_id, sum(total) AS total FROM orders GROUP BY user_id;")}}

	tmpl, err := c.Template(ctx, "steps",
		SQLFiles(nil, filepath.Join(dir, "schema", "*.sql")),
		CopyCSV("users", usersCSV),
		CopyCSVDir(filepath.Join(dir, "seed")),
		SQLFiles(embedded, "extra/*.sql"),
		Migrate("m1", func(ctx context.Context, dsn string) error {
			conn, err := c.Connect(ctx, dbName(t, dsn))
			if err != nil {
				return err
			}
			defer conn.Close()
			return conn.Exec(ctx, "ALTER TABLE users ADD COLUMN migrated bool NOT NULL DEFAULT true")
		}),
		Func("f1", func(ctx context.Context, conn *Conn) error {
			// A session left open by a step must not block publishing.
			leak, err := c.Connect(ctx, dbName(t, conn.DSN()))
			if err != nil {
				return err
			}
			_ = leak
			return conn.Exec(ctx, "INSERT INTO audit.events VALUES (2, 'func')")
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	conn := connectDSN(t, c, tmpl.NewDatabase(ctx, t))
	for sql, want := range map[string]string{
		"SELECT string_agg(email, '|' ORDER BY id) FROM users":       "a@example.test|b,with comma|c@example.test",
		"SELECT string_agg(id::text, ',' ORDER BY id) FROM orders":   "10,11,12",
		"SELECT string_agg(kind, ',' ORDER BY id) FROM audit.events": "login,func",
		"SELECT total FROM order_totals WHERE user_id = 1":           "9.50",
		"SELECT bool_and(migrated)::text FROM users":                 "true",
		"SELECT count(*) FROM pg_stats WHERE tablename = 'orders'":   "3",
	} {
		if got := queryOne(t, conn, sql); got != want {
			t.Errorf("%s = %q, want %q", sql, got, want)
		}
	}
	if _, err := c.Template(ctx, "badglob", SQLFiles(nil, filepath.Join(dir, "nope", "*.sql"))); err == nil || !strings.Contains(err.Error(), "matches no files") {
		t.Errorf("empty glob: got %v", err)
	}
	if _, err := c.Template(ctx, "badsql", SQLFiles(embedded, "extra/*.sql")); err == nil || !strings.Contains(err.Error(), "orders") {
		t.Errorf("failing step: got %v", err)
	}
	admin := connectDSN(t, c, c.DSN("postgres"))
	if got := queryOne(t, admin, "SELECT count(*) FROM pg_database WHERE datname LIKE 'pgs\\_build\\_badsql%'"); got != "0" {
		t.Errorf("a failed build left %s databases behind", got)
	}
}

func TestTemplateDump(t *testing.T) {
	c := testCluster(t)
	ctx := context.Background()
	pgDump, err := c.clientTool("pg_dump")
	if err != nil {
		t.Skip(err)
	}
	source := c.NewDatabase(ctx, t)
	conn := connectDSN(t, c, source)
	if err := conn.Exec(ctx, "CREATE TABLE dumped (id int PRIMARY KEY, note text); INSERT INTO dumped SELECT g, md5(g::text) FROM generate_series(1, 5000) g; CREATE INDEX ON dumped (note)"); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for _, format := range []struct{ name, flag, path string }{
		{"custom", "-Fc", filepath.Join(dir, "db.dump")},
		{"directory", "-Fd", filepath.Join(dir, "db.dir")},
		{"tar", "-Ft", filepath.Join(dir, "db.tar")},
		{"plain", "-Fp", filepath.Join(dir, "db.sql")},
	} {
		t.Run(format.name, func(t *testing.T) {
			out, err := exec.Command(pgDump, format.flag, "-f", format.path, "-d", c.keywordDSN(dbName(t, source))).CombinedOutput()
			if err != nil {
				t.Fatalf("pg_dump: %v: %s", err, out)
			}
			tmpl, err := c.Template(ctx, "dump_"+format.name, Dump(format.path))
			if err != nil {
				t.Fatal(err)
			}
			clone := connectDSN(t, c, tmpl.NewDatabase(ctx, t))
			if got := queryOne(t, clone, "SELECT count(*) FROM dumped"); got != "5000" {
				t.Fatalf("count = %s", got)
			}
		})
	}
}

func TestDerivedTemplates(t *testing.T) {
	c := testCluster(t)
	ctx := context.Background()
	schemaVersion := "s1"
	defineBase := func() *Template {
		base, err := c.Template(ctx, "layer_base", Func(schemaVersion, func(ctx context.Context, conn *Conn) error {
			return conn.Exec(ctx, "CREATE TABLE things (id int PRIMARY KEY, layer text); INSERT INTO things VALUES (1, 'base')")
		}))
		if err != nil {
			t.Fatal(err)
		}
		return base
	}
	var derivedBuilds atomic.Int32
	defineDerived := func() *Template {
		derived, err := c.Template(ctx, "layer_derived", From("layer_base"), Func("d1", func(ctx context.Context, conn *Conn) error {
			derivedBuilds.Add(1)
			return conn.Exec(ctx, "INSERT INTO things VALUES (2, 'derived')")
		}))
		if err != nil {
			t.Fatal(err)
		}
		return derived
	}
	base := defineBase()
	derived := defineDerived()
	conn := connectDSN(t, c, derived.NewDatabase(ctx, t))
	if got := queryOne(t, conn, "SELECT string_agg(layer, ',' ORDER BY id) FROM things"); got != "base,derived" {
		t.Fatalf("derived clone has %q", got)
	}
	baseClone := connectDSN(t, c, base.NewDatabase(ctx, t))
	if got := queryOne(t, baseClone, "SELECT count(*) FROM things"); got != "1" {
		t.Fatalf("base clone has %s rows; deriving must not change the base", got)
	}
	if again := defineDerived(); again != derived || derivedBuilds.Load() != 1 {
		t.Fatalf("redefining an unchanged derived template rebuilt it (%d builds)", derivedBuilds.Load())
	}
	schemaVersion = "s2"
	newBase := defineBase()
	newDerived := defineDerived()
	if newBase.Key() == base.Key() || newDerived.Key() == derived.Key() || derivedBuilds.Load() != 2 {
		t.Fatalf("changing the base did not rebuild the derived template (%d builds)", derivedBuilds.Load())
	}
	if _, err := c.Template(ctx, "orphan", From("undefined_base")); err == nil {
		t.Fatal("From an undefined base succeeded")
	}
	if _, err := c.Template(ctx, "late_from", Func("x", nil), From("layer_base")); err == nil {
		t.Fatal("From as a later step succeeded")
	}
}

// TestPersistentTemplateCache shows the opt-in cache: a persisted cluster
// keeps templates between runs, reuses them while their inputs are
// unchanged, and rebuilds them when a seed file changes.
func TestPersistentTemplateCache(t *testing.T) {
	if testing.Short() {
		t.Skip("starts extra clusters")
	}
	ctx := context.Background()
	base := shortTempDir(t)
	seed := filepath.Join(t.TempDir(), "seed.sql")
	if err := os.WriteFile(seed, []byte("CREATE TABLE seeded (v int); INSERT INTO seeded VALUES (1);"), 0o600); err != nil {
		t.Fatal(err)
	}
	var builds atomic.Int32
	run := func() (key, value string) {
		opts := testOptions()
		opts.BaseDir = base
		opts.Persist = true
		c, err := New(opts)
		if errors.Is(err, ErrNotFound) {
			t.Skip(err)
		}
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := c.Stop(); err != nil {
				t.Fatalf("Stop: %v", err)
			}
		}()
		tmpl, err := c.Template(ctx, "cached", SQLFiles(nil, seed), Func("count", func(context.Context, *Conn) error {
			builds.Add(1)
			return nil
		}))
		if err != nil {
			t.Fatal(err)
		}
		name := "pgs_t_cache_" + randomHex(4)
		if err := tmpl.CreateDatabase(ctx, name); err != nil {
			t.Fatal(err)
		}
		conn, err := c.Connect(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		value = queryOne(t, conn, "SELECT string_agg(v::text, ',') FROM seeded")
		conn.Close()
		if err := c.DropDatabase(ctx, name); err != nil {
			t.Fatal(err)
		}
		return tmpl.Key(), value
	}
	key1, v1 := run()
	if builds.Load() != 1 || v1 != "1" {
		t.Fatalf("first run: %d builds, value %q", builds.Load(), v1)
	}
	if _, err := os.Stat(filepath.Join(base, sharedPrefix+"default", "data", "PG_VERSION")); err != nil {
		t.Fatalf("persisted data dir is gone after the last user left: %v", err)
	}
	key2, v2 := run()
	if builds.Load() != 1 || key2 != key1 || v2 != "1" {
		t.Fatalf("second run with unchanged inputs rebuilt the template (%d builds)", builds.Load())
	}
	if err := os.WriteFile(seed, []byte("CREATE TABLE seeded (v int); INSERT INTO seeded VALUES (1), (2);"), 0o600); err != nil {
		t.Fatal(err)
	}
	key3, v3 := run()
	if builds.Load() != 2 || key3 == key1 || v3 != "1,2" {
		t.Fatalf("changed seed file: %d builds, value %q", builds.Load(), v3)
	}
	// Only the current version is kept.
	opts := testOptions()
	opts.BaseDir, opts.Persist = base, true
	c, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := c.Connect(ctx, "postgres")
	if err != nil {
		t.Fatal(err)
	}
	if got := queryOne(t, conn, "SELECT string_agg(datname, ',') FROM pg_database WHERE datname LIKE 'pgs\\_%'"); got != tmplDBPrefix+"cached_"+key3[:templateKeyChars] {
		t.Errorf("databases left in the persisted cluster: %s", got)
	}
	conn.Close()
	c.Stop()
	stopServer(c.binDir, filepath.Join(base, sharedPrefix+"default", "data"), true)
}

// The helper process below is this test binary re-run with
// PGSCRATCH_TEST_HELPER set. It starts a cluster, says where, and then
// behaves like a slow test: it holds a database for a while.
func TestHelperProcess(t *testing.T) {
	mode := os.Getenv("PGSCRATCH_TEST_HELPER")
	if mode == "" {
		t.Skip("helper for the multi-process tests")
	}
	opts := testOptions()
	opts.BaseDir = os.Getenv("PGSCRATCH_TEST_BASE")
	if mode == "shared" {
		opts.Shared = "multi"
	}
	c, err := Start(opts)
	if err != nil {
		t.Fatal(err)
	}
	hold, _ := time.ParseDuration(os.Getenv("PGSCRATCH_TEST_HOLD"))
	tmpl, err := c.Template(context.Background(), "multi", Func("v1", func(ctx context.Context, conn *Conn) error {
		return conn.Exec(ctx, "CREATE TABLE m (id int)")
	}))
	if err != nil {
		t.Fatal(err)
	}
	tmpl.NewDatabase(context.Background(), t)
	fmt.Printf("ready %s %d\n", c.Dir(), time.Now().UnixNano())
	os.Stdout.Sync()
	time.Sleep(hold)
	tmpl.NewDatabase(context.Background(), t)
	fmt.Printf("done %d\n", time.Now().UnixNano())
}

type helper struct {
	cmd     *exec.Cmd
	dir     string
	readyAt time.Time
	lines   *bufio.Scanner
}

func startHelper(t *testing.T, mode, base string, hold time.Duration) *helper {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestHelperProcess$", "-test.v")
	cmd.Env = append(os.Environ(), "PGSCRATCH_TEST_HELPER="+mode, "PGSCRATCH_TEST_BASE="+base, "PGSCRATCH_TEST_HOLD="+hold.String())
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	return &helper{cmd: cmd, lines: bufio.NewScanner(out)}
}

// waitFor reads the helper's output until a line starting with prefix.
func (h *helper) waitFor(t *testing.T, prefix string) []string {
	t.Helper()
	var seen []string
	for h.lines.Scan() {
		line := h.lines.Text()
		if strings.HasPrefix(line, prefix+" ") {
			return strings.Fields(line)
		}
		seen = append(seen, line)
	}
	t.Fatalf("helper ended without %q:\n%s", prefix, strings.Join(seen, "\n"))
	return nil
}

func unixTime(t *testing.T, field string) time.Time {
	ns, err := strconv.ParseInt(field, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return time.Unix(0, ns)
}

func waitGone(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s was not cleaned up", path)
}

// TestProcessesDoNotBlockEachOther runs two package-like processes at once,
// each holding a database for a while as a slow test would. Neither waits
// for the other: the second is ready long before the first finishes. Their
// clusters are removed when they exit, including after SIGKILL.
func TestProcessesDoNotBlockEachOther(t *testing.T) {
	if testing.Short() {
		t.Skip("runs helper processes")
	}
	testCluster(t) // skip early without binaries
	for _, mode := range []string{"own", "shared"} {
		t.Run(mode, func(t *testing.T) {
			base := shortTempDir(t)
			const hold = 6 * time.Second
			first := startHelper(t, mode, base, hold)
			ready1 := first.waitFor(t, "ready")
			// The second holds twice as long, so it is still running when
			// the first exits.
			second := startHelper(t, mode, base, 2*hold)
			ready2 := second.waitFor(t, "ready")
			done1 := first.waitFor(t, "done")
			if r2, d1 := unixTime(t, ready2[2]), unixTime(t, done1[1]); !r2.Before(d1) {
				t.Fatalf("second process became ready at %v, after the first finished at %v: it waited", r2, d1)
			}
			if mode == "shared" && ready1[1] != ready2[1] {
				t.Fatalf("shared mode used two clusters: %s and %s", ready1[1], ready2[1])
			}
			if mode == "own" && ready1[1] == ready2[1] {
				t.Fatal("two processes shared a per-process cluster")
			}
			if err := first.cmd.Wait(); err != nil {
				t.Fatalf("first helper: %v", err)
			}
			if mode == "shared" {
				// The second is still attached, so the cluster stays up.
				time.Sleep(300 * time.Millisecond)
				if _, err := os.Stat(ready1[1]); err != nil {
					t.Fatalf("shared cluster removed while a process still used it: %v", err)
				}
			} else {
				waitGone(t, ready1[1])
			}
			// Kill the second outright; its watchdog still cleans up.
			second.cmd.Process.Kill()
			second.cmd.Wait()
			waitGone(t, ready2[1])
		})
	}
}
