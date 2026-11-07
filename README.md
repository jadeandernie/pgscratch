# pgscratch

Give every Go test its own real, throwaway PostgreSQL database: ready in a few
dozen milliseconds, already holding your schema and test data, and deleted
afterwards. No Docker, and nothing to set up by hand.

```go
import "github.com/jadeandernie/pgscratch"
```

## The problem

If your code talks to PostgreSQL, the honest way to test it is against a real
PostgreSQL. Getting one for tests is where it goes wrong. The usual choices
each cost something:

- **One shared test database.** Tests see each other's rows, so one test's
  leftovers break another. They can't safely run at the same time, so the
  suite is slow. Someone has to install it, start it and keep it clean.
- **A database in Docker** (testcontainers and the like). It works, but every
  laptop and CI runner needs Docker, each container takes seconds to start,
  and it is one more thing that can break.
- **A stand-in instead of Postgres** (an in-memory fake, SQLite, mocks). Fast,
  but it isn't Postgres. Triggers, stored functions, extensions, JSON
  operators, locking and plenty of ordinary SQL behave differently or not at
  all, so the bugs you most need to catch slip through.
- **Skipping database tests on laptops** and leaving them to CI. A developer
  then learns a query is broken only after pushing, and waits on CI again for
  every attempt at a fix.

All of them share one more cost: before a test can run, its database needs
your schema (the migrations) and some test data. Doing that from scratch for
every test is slow, often slower than the test itself.

## What pgscratch does

Most developer machines and CI images already have PostgreSQL installed, even
when it isn't running. pgscratch uses that copy:

1. **The first test that asks starts a private Postgres server** for this test
   run, in a temporary directory, on a random port that only this machine can
   reach. It is tuned for speed over durability, since nothing in it needs to
   survive.
2. **Every test gets its own database on that server.** Tests can run in
   parallel (`t.Parallel()`), never see each other's data, and never wait on
   one another.
3. **Your schema and test data are built once, not once per test.** You list
   the setup steps (migrations, SQL files, CSV files, a database dump);
   pgscratch runs them once into a *template* database, and each test gets a
   copy. Postgres copies a database by copying its files, which is far faster
   than replaying the setup. If the setup hasn't changed, the template is
   reused rather than rebuilt.
4. **When the tests finish, everything is deleted:** the databases, the server
   and its directory. That holds even if the test process crashes, times out
   or is killed.

If PostgreSQL isn't installed, tests can skip with a clear message instead of
failing.

### Is it for me?

Use it when you write Go tests for code that uses PostgreSQL and want them to
run against the real thing, quickly, on laptops and in CI alike.

It is not for running Postgres in production or for holding data you want to
keep; see [Security](#security). It needs the PostgreSQL server programs on
the machine (version 9.6 or newer; tested with 15 and 18), which are one
package install away (`brew install postgresql@17`, `apt install postgresql`).
It runs on Linux, macOS and the BSDs, not Windows. The library itself has no
dependencies outside Go's standard library; your tests connect with whatever
driver they already use (`pgx`, `database/sql`, ...).

## Usage

```go
func TestOrders(t *testing.T) {
	t.Parallel()
	c, err := pgscratch.Start(pgscratch.Options{Major: 15})
	if errors.Is(err, pgscratch.ErrNotFound) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	dsn := c.NewDatabase(context.Background(), t) // dropped on t.Cleanup
	db, err := sql.Open("pgx", dsn)
	// ...
}
```

`Start` returns one cluster per process (per distinct `Options`), started on
first use behind a `sync.Once`. Every test in the package shares it, and each
test gets its own database, so tests run in parallel without seeing each other.

### Seeded templates

Most suites run the same migrations and fixtures for every test. Build them
once into a template and clone it per test with
`CREATE DATABASE ... TEMPLATE`, which copies files instead of replaying SQL:

```go
tmpl, err := c.Template(ctx, "orders",
	pgscratch.Migrate("v42", func(ctx context.Context, dsn string) error {
		return migrations.Up(ctx, dsn) // your migration runner
	}),
	pgscratch.SQLFiles(testdata.FS, "seed/*.sql"),
	pgscratch.CopyCSVDir("testdata/fixtures"), // accounts.csv, orders.csv, ...
)
dsn := tmpl.NewDatabase(ctx, t) // a clone, dropped on t.Cleanup
```

Steps run in order:

| Step | What it does |
|---|---|
| `Migrate(version, fn)` | Calls `fn` with the build database's DSN. Bump `version` when the migrations change. |
| `Func(version, fn)` | Calls `fn` with a `*pgscratch.Conn` on the build database. |
| `SQLFiles(fsys, patterns...)` | Runs the files matching each glob, in order. `fsys == nil` reads the OS file system. |
| `Dump(path)` | Restores a dump: custom, directory and tar formats through `pg_restore -j`, plain SQL (optionally `.gz`) through `psql`. |
| `CopyCSV(table, path)` | Loads one CSV with a header row via `COPY ... FROM STDIN`. |
| `CopyCSVDir(dir)` | Loads every `table.csv` (or `schema.table.csv`, `.csv.gz`) in a dir, in name order; a `NN-` prefix sets the order and is dropped from the table name. |
| `From(base)` | Starts from another template instead of `template0`; must be the first step. |

Each template has a content key: a SHA-256 over the server major and every
step's inputs (file contents for files, the version string for `Migrate` and
`Func`, the base's key for `From`). A template is rebuilt only when its key
changes. After the steps the build runs `VACUUM (FREEZE, ANALYZE)`, closes every
connection, marks the database `datistemplate` and disallows connections to it,
so clones never race with a lingering session.

Layering keeps the expensive part shared:

```go
schema, _ := c.Template(ctx, "schema", pgscratch.Migrate("v42", migrate))
small, _ := c.Template(ctx, "small", pgscratch.From("schema"), pgscratch.CopyCSVDir("testdata/small"))
large, _ := c.Template(ctx, "large", pgscratch.From("schema"), pgscratch.Dump("testdata/large.dump"))
```

### Sharing one cluster between processes

`go test ./...` runs one process per package. To have them share a server, set
`Options.Shared` (or `PGSCRATCH_SHARED=name`). The first process starts it, the
rest attach, and the last one out stops it. Templates are then built once for
all packages.

### Keeping templates between runs

`Options.Persist` (or `PGSCRATCH_PERSIST=1`) keeps a shared cluster's data
directory after the last process exits, so the next run reuses templates whose
key has not changed and rebuilds the rest. It implies `Shared: "default"` when
no name is given. Off by default; delete the directory under `PGSCRATCH_DIR` to
reset it.

## Finding PostgreSQL

In order, the first directory holding `initdb`, `pg_ctl` and `postgres` whose
major matches `Options.Major` (any major when it is 0):

1. `Options.BinDir`, else `PGSCRATCH_BIN`. An explicit directory must match;
   pgscratch does not fall back past it.
2. Each directory on `PATH`, in order.
3. Well-known install locations, newest major first: Homebrew
   (`/opt/homebrew/opt/postgresql@N/bin`, `/usr/local/opt/...`, linuxbrew),
   Postgres.app, MacPorts, Debian/Ubuntu `/usr/lib/postgresql/N/bin`,
   RHEL `/usr/pgsql-N/bin`.

When nothing matches, the error wraps `ErrNotFound` and lists the directories it
rejected and why.

## Options

| Field | Env | Default |
|---|---|---|
| `BinDir` | `PGSCRATCH_BIN` | discovered |
| `Major` | | any |
| `BaseDir` | `PGSCRATCH_DIR` | `/dev/shm/pgscratch` on Linux when it is tmpfs with 512 MB free, else `$TMPDIR/pgscratch` |
| `Shared` | `PGSCRATCH_SHARED` | per process |
| `Persist` | `PGSCRATCH_PERSIST=1` | off |
| `MaxConnections` | | 300 |
| `Settings` | | extra `postgresql.conf` lines |
| `User` | | `postgres` |
| `StartTimeout` | | 2 minutes |

The server always runs with `fsync`, `synchronous_commit` and
`full_page_writes` off, trust authentication, `listen_addresses = '127.0.0.1'`
on a free port, and a unix socket in the data directory (left off when the path
is too long for a socket). Never point it at data you want to keep.

## Design

**Layout.** Each cluster lives in a directory under the base dir:
`p-<random>/` for a per-process cluster, `shared-<name>/` (plus
`shared-<name>.lock` and `shared-<name>.users`) for a shared one. The data
directory is `<dir>/data`, and the server log is `<dir>/server.log`.

**Cleanup without relying on the test binary.** Go test binaries exit through
`os.Exit`, panics, timeouts and `kill -9`, so deferred cleanup is not enough.
Before `initdb`, pgscratch re-executes the test binary as a small watchdog
(caught by an `init` hook before the test runner starts). The watchdog inherits
the read end of a pipe whose only write end the test process holds. When the
test process stops for any reason, the kernel closes that end, the watchdog
reads EOF, stops the server (`pg_ctl stop -m immediate`) and deletes the
directory. The watchdog also holds the cluster's `flock`.

**Sweeping stale clusters.** If a machine reboots or the watchdog itself is
killed, a directory can be left behind. Every `Start` sweeps the base dir: a
cluster directory whose lock file can be locked exclusively has no live owner,
so its server is stopped (after checking via `SHOW data_directory` that the
`postmaster.pid` really belongs to it) and the directory removed. Directories
still being created (no `owned` marker yet) are left alone unless they are more
than 10 minutes old. A `flock` belongs to an open file description, so a held
lock proves its holder is alive; there are no pid files to go stale.

**Parallel safety.** No lock is held while a test runs.

- A per-process cluster has no cross-process locks at all after start.
- A shared cluster takes its `.lock` only to start, attach and detach. Each
  attached process's watchdog holds a shared `flock` on `.users`; the last one
  out is the one whose exclusive non-blocking `flock` on `.users` succeeds, and
  it stops the server.
- `NewDatabase` and `CreateDatabase` take no lock. Database names are random,
  admin connections come from a small pool, and `CREATE DATABASE ... TEMPLATE`
  failing with "source database is being accessed by other users" (`55006`) is
  retried with backoff.
- Templates are serialized per template only: an in-process mutex per name (a
  singleflight), then a `flock` per template name for other processes sharing
  the cluster. Different templates build concurrently, and a template that is
  already built is used without taking the file lock.

**Template versions.** A template build runs in a scratch database
(`pgs_build_*`), then is renamed to `pgs_tpl_<name>_<key prefix>` with the full
key in the database comment. A process that finds the right name, comment and
`datistemplate` uses it as is. Older versions of the same template are dropped
after a new one is published.

**macOS.** Postgres uses SysV semaphores, and macOS defaults allow only a few
clusters. If `initdb` fails with "could not create semaphores", raise them:

```sh
sudo sysctl -w kern.sysv.semmni=256 kern.sysv.semmns=4096 kern.sysv.semmsl=87
```

## Security

A pgscratch cluster is for tests only. It uses `trust` authentication, with no
password, on its unix socket (in a directory only the current user can open)
and on a random `127.0.0.1` port. While it runs, any local process that can
reach that port can connect as the superuser. Do not put real data in it, and
do not run it on a machine shared with users you do not trust. `fsync` is off,
so a crash can lose or corrupt the cluster; pgscratch deletes it afterwards
anyway.

## Benchmark

`go test -run '^$' -bench . -benchtime 3s` on an Apple M3 Pro (12 cores),
PostgreSQL 15.19, macOS, base dir on APFS. The fixture is 20 tables with two
indexes each plus 50,000 rows loaded with COPY; each iteration creates the
database and drops it.

| Benchmark | Per test database |
|---|---|
| `BenchmarkCreateEmptyDatabase` (floor: empty database) | 50 ms |
| `BenchmarkCloneSeededTemplate` (clone of the seeded template) | 71 ms |
| `BenchmarkSeedFreshDatabase` (run schema + COPY each time) | 183 ms |

Cloning costs about 21 ms over an empty database and is 2.6 times faster than
seeding each database. The gap grows with the fixture: migrations that take
seconds cost seconds per test without a template and once per run with one.

## Testing pgscratch

```sh
go test -race ./...                          # any major found on the machine
PGSCRATCH_TEST_MAJOR=15 go test -race ./...  # a specific major
```

The tests skip, rather than fail, when no server binaries are found. They cover
binary discovery, 48 databases created in parallel, templates cloned 32 at a
time under `-race`, every step kind, derived templates, the persistent cache's
hit and miss, and two processes holding clusters at once (own and shared) to
show neither waits on the other.

## Contributing

Contributions are welcome, from people and from AI coding agents alike. Raise a
pull request against `main`; there is no need to open an issue first. See
[CONTRIBUTING.md](CONTRIBUTING.md) for the checks to run and what a pull
request should include.

## License

MIT
