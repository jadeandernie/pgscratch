package pgscratch

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// fakeBinDir makes a directory with executable initdb, pg_ctl and postgres
// stubs; its major version is recorded for the stub Version function.
func fakeBinDir(t *testing.T, majors map[string]int, major int) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range serverBinaries {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	majors[dir] = major
	return dir
}

func stubVersion(majors map[string]int) func(string) (int, error) {
	return func(dir string) (int, error) {
		if m, ok := majors[dir]; ok {
			return m, nil
		}
		return 0, errors.New("unknown dir")
	}
}

func TestBinSearchExplicitWins(t *testing.T) {
	majors := map[string]int{}
	explicit := fakeBinDir(t, majors, 15)
	onPath := fakeBinDir(t, majors, 18)
	dir, major, err := binSearch{Explicit: explicit, PathDirs: []string{onPath}, Version: stubVersion(majors)}.find()
	if err != nil || dir != explicit || major != 15 {
		t.Fatalf("got %q %d %v, want the explicit dir at 15", dir, major, err)
	}
}

func TestBinSearchExplicitWrongMajorFails(t *testing.T) {
	majors := map[string]int{}
	explicit := fakeBinDir(t, majors, 17)
	onPath := fakeBinDir(t, majors, 15)
	_, _, err := binSearch{Explicit: explicit, Major: 15, PathDirs: []string{onPath}, Version: stubVersion(majors)}.find()
	if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "PostgreSQL 17, want 15") {
		t.Fatalf("got %v, want a major mismatch on the explicit dir", err)
	}
}

func TestBinSearchExplicitMissingBinaries(t *testing.T) {
	_, _, err := binSearch{Explicit: t.TempDir(), Version: stubVersion(nil)}.find()
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestBinSearchPathSkipsWrongMajor(t *testing.T) {
	majors := map[string]int{}
	pg18 := fakeBinDir(t, majors, 18)
	pg15 := fakeBinDir(t, majors, 15)
	empty := t.TempDir()
	dir, major, err := binSearch{Major: 15, PathDirs: []string{empty, pg18, pg15}, Version: stubVersion(majors)}.find()
	if err != nil || dir != pg15 || major != 15 {
		t.Fatalf("got %q %d %v, want the PATH dir holding 15", dir, major, err)
	}
}

func TestBinSearchPathFirstWithoutMajor(t *testing.T) {
	majors := map[string]int{}
	pg14 := fakeBinDir(t, majors, 14)
	pg18 := fakeBinDir(t, majors, 18)
	dir, _, err := binSearch{PathDirs: []string{pg14, pg18}, Version: stubVersion(majors)}.find()
	if err != nil || dir != pg14 {
		t.Fatalf("got %q %v, want the first PATH match", dir, err)
	}
}

func TestBinSearchWellKnownFallback(t *testing.T) {
	majors := map[string]int{}
	pg18 := fakeBinDir(t, majors, 18)
	brew15 := fakeBinDir(t, majors, 15)
	brew16 := fakeBinDir(t, majors, 16)
	var asked []int
	wellKnown := func(major int) []string {
		asked = append(asked, major)
		return []string{brew15, brew16}
	}
	dir, major, err := binSearch{Major: 15, PathDirs: []string{pg18}, WellKnown: wellKnown, Version: stubVersion(majors)}.find()
	if err != nil || dir != brew15 || major != 15 {
		t.Fatalf("got %q %d %v, want the well-known 15", dir, major, err)
	}
	if len(asked) != 1 || asked[0] != 15 {
		t.Fatalf("well-known dirs asked for %v, want [15]", asked)
	}
	// Without a wanted major, the newest well-known install wins.
	dir, major, err = binSearch{WellKnown: wellKnown, Version: stubVersion(majors)}.find()
	if err != nil || dir != brew16 || major != 16 {
		t.Fatalf("got %q %d %v, want the newest well-known install", dir, major, err)
	}
}

func TestBinSearchNotFoundListsRejected(t *testing.T) {
	majors := map[string]int{}
	pg18 := fakeBinDir(t, majors, 18)
	_, _, err := binSearch{Major: 15, PathDirs: []string{pg18}, Version: stubVersion(majors)}.find()
	if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), pg18+" (PostgreSQL 18)") {
		t.Fatalf("got %v, want ErrNotFound naming the rejected dir", err)
	}
}

func TestBinSearchIgnoresNonExecutable(t *testing.T) {
	majors := map[string]int{}
	dir := fakeBinDir(t, majors, 15)
	if err := os.Chmod(filepath.Join(dir, "pg_ctl"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := (binSearch{PathDirs: []string{dir}, Version: stubVersion(majors)}).find(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound for a dir with a non-executable pg_ctl", err)
	}
}

func TestParseServerMajor(t *testing.T) {
	for out, want := range map[string]int{
		"postgres (PostgreSQL) 15.19\n":                          15,
		"postgres (PostgreSQL) 18.0 (Homebrew)":                  18,
		"postgres (PostgreSQL) 9.6.24":                           9,
		"postgres (PostgreSQL) 17beta1":                          17,
		"postgres (PostgreSQL) 16.4 (Ubuntu 16.4-1.pgdg24.04+1)": 16,
	} {
		got, err := parseServerMajor(out)
		if err != nil || got != want {
			t.Errorf("parseServerMajor(%q) = %d, %v; want %d", out, got, err, want)
		}
	}
	if _, err := parseServerMajor("garbage"); err == nil {
		t.Error("parseServerMajor(garbage) succeeded")
	}
}

func TestWellKnownDirsAreVersioned(t *testing.T) {
	for _, dir := range wellKnownBinDirs(15) {
		if strings.Contains(dir, "*") {
			t.Fatalf("unexpanded glob %q", dir)
		}
	}
}

func TestOptionsResolveFromEnv(t *testing.T) {
	t.Setenv(EnvBin, "/opt/pg/bin")
	t.Setenv(EnvDir, "/scratch")
	t.Setenv(EnvShared, "")
	t.Setenv(EnvPersist, "1")
	o := Options{}.resolved()
	if o.BinDir != "/opt/pg/bin" || o.BaseDir != "/scratch" || !o.Persist || o.Shared != "default" {
		t.Fatalf("resolved %+v", o)
	}
	if o.MaxConnections != defaultMaxConnections || o.User != "postgres" {
		t.Fatalf("defaults %+v", o)
	}
	o = Options{BinDir: "/explicit", BaseDir: "/base", Shared: "ci", MaxConnections: 50}.resolved()
	if o.BinDir != "/explicit" || o.BaseDir != "/base" || o.Shared != "ci" || o.MaxConnections != 50 {
		t.Fatalf("explicit options lost: %+v", o)
	}
}

func TestSanitizeName(t *testing.T) {
	for in, want := range map[string]string{
		"TestFoo/sub_case#01":   "testfoo_sub_case_01",
		"__x__":                 "x",
		strings.Repeat("a", 80): strings.Repeat("a", 30),
	} {
		if got := sanitizeName(in, 30); got != want {
			t.Errorf("sanitizeName(%q) = %q, want %q", in, got, want)
		}
	}
	name := (&Cluster{}).testDatabaseName(&fakeTB{name: strings.Repeat("Long/Name", 20)})
	if len(name) > maxIdentLen || !strings.HasPrefix(name, testDBPrefix) {
		t.Fatalf("test database name %q", name)
	}
}

type fakeTB struct {
	testing.TB
	name string
}

func (f *fakeTB) Name() string { return f.name }

func TestQuoting(t *testing.T) {
	if got := QuoteIdentifier(`a"b`); got != `"a""b"` {
		t.Errorf("QuoteIdentifier = %s", got)
	}
	if got := QuoteLiteral(`it's`); got != `'it''s'` {
		t.Errorf("QuoteLiteral = %s", got)
	}
	if got := quoteQualified("audit.events"); got != `"audit"."events"` {
		t.Errorf("quoteQualified = %s", got)
	}
}

func TestSweepRemovesOnlyAbandonedClusters(t *testing.T) {
	base := t.TempDir()
	abandoned := filepath.Join(base, ownPrefix+"dead")
	live := filepath.Join(base, ownPrefix+"live")
	starting := filepath.Join(base, ownPrefix+"starting")
	for _, dir := range []string{abandoned, live, starting} {
		if err := os.MkdirAll(filepath.Join(dir, "data"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "lock"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, dir := range []string{abandoned, live} {
		if err := os.WriteFile(filepath.Join(dir, ownedMarker), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	held, err := os.OpenFile(filepath.Join(live, "lock"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if ok, err := lockFile(held, true, false); !ok {
		t.Fatalf("locking: %v", err)
	}
	sweep(base)
	if _, err := os.Stat(abandoned); !os.IsNotExist(err) {
		t.Errorf("abandoned cluster dir survived the sweep: %v", err)
	}
	if _, err := os.Stat(live); err != nil {
		t.Errorf("live cluster dir was swept: %v", err)
	}
	if _, err := os.Stat(starting); err != nil {
		t.Errorf("a cluster still starting (no owned marker) was swept: %v", err)
	}
}
