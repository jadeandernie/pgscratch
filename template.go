package pgscratch

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Step is one stage of building a template. Steps run in order against the
// template database; see Migrate, SQLFiles, Dump, CopyCSV, CopyCSVDir, Func
// and From.
type Step struct {
	kind string
	from string
	// prepare writes the step's inputs to the content hash and returns the
	// function that applies exactly those inputs.
	prepare func(h io.Writer) (func(ctx context.Context, b *build) error, error)
}

// Migrate runs fn with the template database's URL, for a migration tool or
// a schema loader. version stands in for fn's behaviour in the content key:
// change it when fn's output changes (a migration count or schema hash).
func Migrate(version string, fn func(ctx context.Context, dsn string) error) Step {
	return Step{kind: "migrate", prepare: func(h io.Writer) (func(context.Context, *build) error, error) {
		writeField(h, version)
		return func(ctx context.Context, b *build) error {
			b.closeConn()
			return fn(ctx, b.c.DSN(b.db))
		}, nil
	}}
}

// Func runs fn on a connection to the template database. version stands in
// for fn's behaviour in the content key, as in Migrate.
func Func(version string, fn func(ctx context.Context, conn *Conn) error) Step {
	return Step{kind: "func", prepare: func(h io.Writer) (func(context.Context, *build) error, error) {
		writeField(h, version)
		return func(ctx context.Context, b *build) error {
			conn, err := b.conn(ctx)
			if err != nil {
				return err
			}
			return fn(ctx, conn)
		}, nil
	}}
}

// SQLFiles runs SQL files in order: each pattern's matches sorted by name,
// patterns in the order given, duplicates once. With fsys nil the patterns
// are OS paths (filepath.Glob); otherwise fs.Glob patterns in fsys, so
// embed.FS works. Each file is sent as one simple-query message, so it may
// hold many statements but no psql meta-commands. A pattern that matches
// nothing is an error.
func SQLFiles(fsys fs.FS, patterns ...string) Step {
	return Step{kind: "sql", prepare: func(h io.Writer) (func(context.Context, *build) error, error) {
		names, err := globAll(fsys, patterns)
		if err != nil {
			return nil, err
		}
		contents := make([][]byte, len(names))
		for i, name := range names {
			if fsys == nil {
				contents[i], err = os.ReadFile(name)
			} else {
				contents[i], err = fs.ReadFile(fsys, name)
			}
			if err != nil {
				return nil, fmt.Errorf("pgscratch: %w", err)
			}
			writeField(h, filepath.Base(name))
			writeField(h, string(contents[i]))
		}
		return func(ctx context.Context, b *build) error {
			conn, err := b.conn(ctx)
			if err != nil {
				return err
			}
			for i, name := range names {
				if err := conn.Exec(ctx, string(contents[i])); err != nil {
					return fmt.Errorf("%s: %w", name, err)
				}
			}
			return nil
		}, nil
	}}
}

func globAll(fsys fs.FS, patterns []string) ([]string, error) {
	var names []string
	seen := map[string]bool{}
	for _, pattern := range patterns {
		var matches []string
		var err error
		if fsys == nil {
			matches, err = filepath.Glob(pattern)
		} else {
			matches, err = fs.Glob(fsys, pattern)
		}
		if err != nil {
			return nil, fmt.Errorf("pgscratch: pattern %q: %w", pattern, err)
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("pgscratch: pattern %q matches no files", pattern)
		}
		sort.Strings(matches)
		for _, m := range matches {
			if !seen[m] {
				seen[m] = true
				names = append(names, m)
			}
		}
	}
	return names, nil
}

// Dump restores a pg_dump archive. Custom (-Fc) and directory (-Fd) archives
// go through pg_restore -j with one job per CPU (up to 8); tar archives
// through pg_restore; plain SQL (optionally .gz) through psql with
// ON_ERROR_STOP, or as one simple query when psql is missing. Owners and
// privileges in the dump are skipped. The client tools come from the
// cluster's BinDir, or PATH.
func Dump(path string) Step {
	return Step{kind: "dump", prepare: func(h io.Writer) (func(context.Context, *build) error, error) {
		format, err := dumpFormat(path)
		if err != nil {
			return nil, err
		}
		writeField(h, format)
		if err := hashPath(h, path); err != nil {
			return nil, err
		}
		return func(ctx context.Context, b *build) error {
			b.closeConn()
			return b.restore(ctx, path, format)
		}, nil
	}}
}

func dumpFormat(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("pgscratch: %w", err)
	}
	if info.IsDir() {
		if _, err := os.Stat(filepath.Join(path, "toc.dat")); err != nil {
			return "", fmt.Errorf("pgscratch: %s is a directory without toc.dat, not a pg_dump -Fd archive", path)
		}
		return "directory", nil
	}
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("pgscratch: %w", err)
	}
	defer f.Close()
	head := make([]byte, 512)
	n, _ := io.ReadFull(f, head)
	head = head[:n]
	switch {
	case bytes.HasPrefix(head, []byte("PGDMP")):
		return "custom", nil
	case n >= 262 && bytes.Equal(head[257:262], []byte("ustar")):
		return "tar", nil
	case strings.HasSuffix(path, ".gz"):
		return "plain.gz", nil
	}
	return "plain", nil
}

func (b *build) restore(ctx context.Context, path, format string) error {
	dsn := b.c.keywordDSN(b.db)
	var cmd *exec.Cmd
	switch format {
	case "custom", "directory", "tar":
		tool, err := b.c.clientTool("pg_restore")
		if err != nil {
			return err
		}
		args := []string{"--no-owner", "--no-privileges", "--exit-on-error", "-d", dsn}
		if format != "tar" {
			args = append(args, "-j", strconv.Itoa(min(8, max(1, runtime.GOMAXPROCS(0)))))
		}
		cmd = exec.CommandContext(ctx, tool, append(args, path)...)
	default:
		tool, err := b.c.clientTool("psql")
		if err != nil {
			if format == "plain.gz" {
				return err
			}
			content, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			conn, connErr := b.conn(ctx)
			if connErr != nil {
				return connErr
			}
			return conn.Exec(ctx, string(content))
		}
		cmd = exec.CommandContext(ctx, tool, "-X", "-q", "-v", "ON_ERROR_STOP=1", "-d", dsn)
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		cmd.Stdin = f
		if format == "plain.gz" {
			gz, err := gzip.NewReader(f)
			if err != nil {
				return fmt.Errorf("pgscratch: %s: %w", path, err)
			}
			cmd.Stdin = gz
		}
	}
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	cmd.WaitDelay = 5 * time.Second
	cmd.Env = append(os.Environ(), "PGAPPNAME=pgscratch")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("pgscratch: restoring %s: %v: %s", path, err, lastLines(out.Bytes(), 20))
	}
	return nil
}

// CopyCSV loads a CSV file with a header row into table ("name" or
// "schema.name") with COPY FROM STDIN, streaming the file. A .gz file is
// decompressed. Triggers and constraints apply as usual.
func CopyCSV(table, path string) Step {
	return Step{kind: "csv", prepare: func(h io.Writer) (func(context.Context, *build) error, error) {
		writeField(h, table)
		if err := hashPath(h, path); err != nil {
			return nil, err
		}
		return func(ctx context.Context, b *build) error { return b.copyCSV(ctx, table, path) }, nil
	}}
}

// CopyCSVDir loads every *.csv and *.csv.gz file in dir, in name order, each
// into the table its name gives: "orders.csv" into orders,
// "audit.events.csv" into audit.events. A leading "NN-" orders loads
// without naming the table: "01-users.csv" loads users before
// "02-orders.csv" loads orders.
func CopyCSVDir(dir string) Step {
	return Step{kind: "csvdir", prepare: func(h io.Writer) (func(context.Context, *build) error, error) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, fmt.Errorf("pgscratch: %w", err)
		}
		var files, tables []string
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !(strings.HasSuffix(name, ".csv") || strings.HasSuffix(name, ".csv.gz")) {
				continue
			}
			table := strings.TrimSuffix(strings.TrimSuffix(name, ".gz"), ".csv")
			table = orderPrefix.ReplaceAllString(table, "")
			files = append(files, filepath.Join(dir, name))
			tables = append(tables, table)
		}
		if len(files) == 0 {
			return nil, fmt.Errorf("pgscratch: no .csv files in %s", dir)
		}
		for i := range files {
			writeField(h, tables[i])
			if err := hashPath(h, files[i]); err != nil {
				return nil, err
			}
		}
		return func(ctx context.Context, b *build) error {
			for i := range files {
				if err := b.copyCSV(ctx, tables[i], files[i]); err != nil {
					return err
				}
			}
			return nil
		}, nil
	}}
}

var orderPrefix = regexp.MustCompile(`^[0-9]+-`)

func (b *build) copyCSV(ctx context.Context, table, path string) error {
	columns, err := csvHeader(path)
	if err != nil {
		return err
	}
	r, closeFn, err := openMaybeGzip(path)
	if err != nil {
		return err
	}
	defer closeFn()
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = QuoteIdentifier(col)
	}
	sql := fmt.Sprintf("COPY %s (%s) FROM STDIN WITH (FORMAT csv, HEADER true)", quoteQualified(table), strings.Join(quoted, ", "))
	conn, err := b.conn(ctx)
	if err != nil {
		return err
	}
	if _, err := conn.CopyFrom(ctx, sql, r); err != nil {
		return fmt.Errorf("pgscratch: loading %s into %s: %w", path, table, err)
	}
	return nil
}

func csvHeader(path string) ([]string, error) {
	r, closeFn, err := openMaybeGzip(path)
	if err != nil {
		return nil, err
	}
	defer closeFn()
	header, err := csv.NewReader(bufio.NewReader(r)).Read()
	if err != nil {
		return nil, fmt.Errorf("pgscratch: reading CSV header of %s: %w", path, err)
	}
	if len(header) > 0 {
		header[0] = strings.TrimPrefix(header[0], "\ufeff")
	}
	return header, nil
}

func openMaybeGzip(path string) (io.Reader, func(), error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("pgscratch: %w", err)
	}
	if !strings.HasSuffix(path, ".gz") {
		return f, func() { f.Close() }, nil
	}
	gz, err := gzip.NewReader(f)
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("pgscratch: %s: %w", path, err)
	}
	return gz, func() { gz.Close(); f.Close() }, nil
}

func quoteQualified(name string) string {
	parts := strings.SplitN(name, ".", 2)
	for i, p := range parts {
		parts[i] = QuoteIdentifier(p)
	}
	return strings.Join(parts, ".")
}

// From starts the template as a clone of the template named base, built
// earlier in this process with Cluster.Template. It must be the first step.
// The derived template's key includes base's key, so it rebuilds whenever
// base changes.
func From(base string) Step {
	return Step{kind: "from", from: base}
}

// writeField writes a length-prefixed field, so field boundaries are part of
// the hash.
func writeField(h io.Writer, value string) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(value)))
	h.Write(n[:])
	io.WriteString(h, value)
}

// hashPath hashes a file's bytes, or every file under a directory with its
// relative name.
func hashPath(h io.Writer, path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("pgscratch: %w", err)
	}
	if !info.IsDir() {
		return hashFile(h, path)
	}
	return filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(path, p)
		writeField(h, filepath.ToSlash(rel))
		return hashFile(h, p)
	})
}

func hashFile(h io.Writer, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("pgscratch: %w", err)
	}
	defer f.Close()
	sum := sha256.New()
	if _, err := io.Copy(sum, f); err != nil {
		return fmt.Errorf("pgscratch: %w", err)
	}
	writeField(h, string(sum.Sum(nil)))
	return nil
}

// build is a template database under construction.
type build struct {
	c    *Cluster
	db   string
	open *Conn
}

func (b *build) conn(ctx context.Context) (*Conn, error) {
	if b.open == nil {
		conn, err := b.c.Connect(ctx, b.db)
		if err != nil {
			return nil, err
		}
		b.open = conn
	}
	return b.open, nil
}

func (b *build) closeConn() {
	if b.open != nil {
		b.open.Close()
		b.open = nil
	}
}

// Template is a database built once from steps and cloned for each test.
type Template struct {
	c       *Cluster
	name    string
	key     string
	db      string
	base    *Template
	runners []func(context.Context, *build) error
}

// Name is the name the template was defined with.
func (t *Template) Name() string { return t.name }

// Key is the template's content key: a SHA-256 over every step's inputs.
func (t *Template) Key() string { return t.key }

// Database is the template database's name, pgs_tpl_<name>_<key prefix>.
func (t *Template) Database() string { return t.db }

type templateEntry struct {
	mu      sync.Mutex // per template name; never held while tests run
	current *Template
}

var templateNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,23}$`)

const templateKeyChars = 16

// Template returns the template name built from steps, building it only when
// no template with the same content key exists in the cluster. In a shared
// or persisted cluster, a template another process (or an earlier run)
// built is reused as is. Builds of one name are serialised across goroutines
// and processes; different names build in parallel. After the steps the
// database is analyzed and frozen, every connection to it is closed, and it
// is marked as a template that accepts no connections.
func (c *Cluster) Template(ctx context.Context, name string, steps ...Step) (*Template, error) {
	if !templateNamePattern.MatchString(name) {
		return nil, fmt.Errorf("pgscratch: template name %q must match %s", name, templateNamePattern)
	}
	h := sha256.New()
	writeField(h, "pgscratch template v1")
	writeField(h, strconv.Itoa(c.major))
	t := &Template{c: c, name: name}
	for i, step := range steps {
		writeField(h, step.kind)
		if step.kind == "from" {
			if i != 0 {
				return nil, fmt.Errorf("pgscratch: template %s: From must be the first step", name)
			}
			if step.from == name {
				return nil, fmt.Errorf("pgscratch: template %s cannot be built from itself", name)
			}
			base := c.entry(step.from).latest()
			if base == nil {
				return nil, fmt.Errorf("pgscratch: template %s: base template %q is not defined in this process", name, step.from)
			}
			t.base = base
			writeField(h, base.key)
			continue
		}
		if step.prepare == nil {
			return nil, fmt.Errorf("pgscratch: template %s: step %d is the zero Step", name, i)
		}
		run, err := step.prepare(h)
		if err != nil {
			return nil, fmt.Errorf("pgscratch: template %s step %d (%s): %w", name, i, step.kind, err)
		}
		t.runners = append(t.runners, run)
	}
	t.key = hex.EncodeToString(h.Sum(nil))
	t.db = tmplDBPrefix + name + "_" + t.key[:templateKeyChars]

	e := c.entry(name)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.current != nil && e.current.key == t.key {
		return e.current, nil
	}
	if err := t.ensureLocked(ctx); err != nil {
		return nil, err
	}
	e.current = t
	return t, nil
}

func (c *Cluster) entry(name string) *templateEntry {
	c.tmplMu.Lock()
	defer c.tmplMu.Unlock()
	e := c.templates[name]
	if e == nil {
		e = &templateEntry{}
		c.templates[name] = e
	}
	return e
}

func (e *templateEntry) latest() *Template {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.current
}

// ensure makes sure t's database exists, rebuilding it if another version
// replaced it.
func (t *Template) ensure(ctx context.Context) error {
	e := t.c.entry(t.name)
	e.mu.Lock()
	defer e.mu.Unlock()
	return t.ensureLocked(ctx)
}

func (t *Template) ensureLocked(ctx context.Context) error {
	if ok, err := t.ready(ctx); err != nil || ok {
		return err
	}
	lockDir := filepath.Join(t.c.dir, "locks")
	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		return fmt.Errorf("pgscratch: %w", err)
	}
	lock, err := os.OpenFile(filepath.Join(lockDir, "tpl-"+t.name+".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("pgscratch: %w", err)
	}
	defer lock.Close()
	if err := lockWait(ctx, lock); err != nil {
		return fmt.Errorf("pgscratch: waiting to build template %s: %w", t.name, err)
	}
	defer unlockFile(lock)
	if ok, err := t.ready(ctx); err != nil || ok {
		return err
	}
	if err := t.build(ctx); err != nil {
		return fmt.Errorf("pgscratch: building template %s: %w", t.name, err)
	}
	return nil
}

func (t *Template) comment() string { return "pgscratch:" + t.key }

// ready reports whether the finished template database exists.
func (t *Template) ready(ctx context.Context) (bool, error) {
	conn, err := t.c.admin(ctx)
	if err != nil {
		return false, err
	}
	rows, err := conn.Query(ctx, "SELECT d.datistemplate AND NOT d.datallowconn, COALESCE(shobj_description(d.oid, 'pg_database'), '') FROM pg_database d WHERE d.datname = "+QuoteLiteral(t.db))
	t.c.release(conn, err)
	if err != nil {
		return false, err
	}
	return len(rows) == 1 && rows[0][0] == "t" && rows[0][1] == t.comment(), nil
}

func (t *Template) build(ctx context.Context) error {
	c := t.c
	buildDB := buildDBPrefix + t.name + "_" + randomHex(4)
	source := "template0"
	if t.base != nil {
		source = t.base.db
	}
	err := c.CreateDatabase(ctx, buildDB, source)
	if errCode(err) == codeInvalidCatalog && t.base != nil {
		if err = t.base.ensure(ctx); err == nil {
			err = c.CreateDatabase(ctx, buildDB, source)
		}
	}
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			dropCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			c.DropDatabase(dropCtx, buildDB)
		}
	}()
	b := &build{c: c, db: buildDB}
	for _, run := range t.runners {
		if err := run(ctx, b); err != nil {
			b.closeConn()
			return err
		}
	}
	conn, err := b.conn(ctx)
	if err != nil {
		return err
	}
	if err := conn.Exec(ctx, "VACUUM (FREEZE, ANALYZE)"); err != nil {
		b.closeConn()
		return err
	}
	b.closeConn()
	if err := t.publish(ctx, buildDB); err != nil {
		return err
	}
	published = true
	t.dropOtherVersions(ctx)
	return nil
}

// publish turns the build database into the template: no connections, a
// comment holding the content key, the final name, and the template flag.
// Readers check the flag and the comment, so a half-published template is
// never cloned.
func (t *Template) publish(ctx context.Context, buildDB string) error {
	c := t.c
	conn, err := c.admin(ctx)
	if err != nil {
		return err
	}
	defer func() { c.release(conn, err) }()
	quotedBuild := QuoteIdentifier(buildDB)
	if err = conn.Exec(ctx, "ALTER DATABASE "+quotedBuild+" WITH ALLOW_CONNECTIONS false"); err != nil {
		return err
	}
	// Close whatever the steps left open (a Migrate pool, say).
	for attempt := 0; ; attempt++ {
		var rows [][]string
		rows, err = conn.Query(ctx, "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE datname = "+QuoteLiteral(buildDB)+" AND pid <> pg_backend_pid()")
		if err != nil {
			return err
		}
		if rows[0][0] == "0" {
			break
		}
		if attempt > 500 {
			return fmt.Errorf("sessions on %s did not end", buildDB)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err = conn.Exec(ctx, "COMMENT ON DATABASE "+quotedBuild+" IS "+QuoteLiteral(t.comment())); err != nil {
		return err
	}
	if err = dropTemplateDB(ctx, c, conn, t.db); err != nil {
		return err
	}
	for attempt := 0; ; attempt++ {
		err = conn.Exec(ctx, "ALTER DATABASE "+quotedBuild+" RENAME TO "+QuoteIdentifier(t.db))
		if errCode(err) != codeObjectInUse || attempt > 200 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		return err
	}
	err = conn.Exec(ctx, "ALTER DATABASE "+QuoteIdentifier(t.db)+" IS_TEMPLATE true")
	return err
}

func dropTemplateDB(ctx context.Context, c *Cluster, conn *Conn, name string) error {
	err := conn.Exec(ctx, "ALTER DATABASE "+QuoteIdentifier(name)+" IS_TEMPLATE false")
	if err != nil && errCode(err) != codeInvalidCatalog {
		return err
	}
	return c.dropWith(ctx, conn, name)
}

// dropOtherVersions drops this template name's databases with other keys.
// A process still cloning an old version rebuilds it on demand.
func (t *Template) dropOtherVersions(ctx context.Context) {
	conn, err := t.c.admin(ctx)
	if err != nil {
		return
	}
	defer t.c.release(conn, nil)
	rows, err := conn.Query(ctx, "SELECT datname FROM pg_database WHERE datistemplate")
	if err != nil {
		return
	}
	prefix := tmplDBPrefix + t.name + "_"
	for _, row := range rows {
		name := row[0]
		rest, ok := strings.CutPrefix(name, prefix)
		if !ok || name == t.db || len(rest) != templateKeyChars || strings.Trim(rest, "0123456789abcdef") != "" {
			continue
		}
		dropTemplateDB(ctx, t.c, conn, name)
	}
}

// CreateDatabase clones the template into a new database the caller owns.
func (t *Template) CreateDatabase(ctx context.Context, name string) error {
	err := t.c.CreateDatabase(ctx, name, t.db)
	if errCode(err) == codeInvalidCatalog {
		// Another process replaced this version; build it again.
		if err = t.ensure(ctx); err == nil {
			err = t.c.CreateDatabase(ctx, name, t.db)
		}
	}
	return err
}

// NewDatabase clones the template into a database for tb, dropped when tb's
// test finishes, and returns its postgres:// URL.
func (t *Template) NewDatabase(ctx context.Context, tb testing.TB) string {
	tb.Helper()
	name := t.c.testDatabaseName(tb)
	if err := t.CreateDatabase(ctx, name); err != nil {
		tb.Fatalf("pgscratch: cloning template %s: %v", t.name, err)
	}
	t.c.dropOnCleanup(tb, name)
	return t.c.DSN(name)
}
