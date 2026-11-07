package pgscratch

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// ErrNotFound reports that no usable PostgreSQL server binaries were found.
// Errors from Start and New wrap it, so callers can skip tests with
// errors.Is(err, pgscratch.ErrNotFound).
var ErrNotFound = errors.New("pgscratch: PostgreSQL server binaries not found")

// serverBinaries must all be present for a directory to count.
var serverBinaries = []string{"initdb", "pg_ctl", "postgres"}

// binSearch describes where to look; tests replace PathDirs, WellKnown and
// Version.
type binSearch struct {
	Explicit  string
	Major     int
	PathDirs  []string
	WellKnown func(major int) []string
	Version   func(dir string) (int, error)
}

func findBinDir(o Options) (string, int, error) {
	return binSearch{
		Explicit:  o.BinDir,
		Major:     o.Major,
		PathDirs:  filepath.SplitList(os.Getenv("PATH")),
		WellKnown: wellKnownBinDirs,
		Version:   cachedServerMajor,
	}.find()
}

// find returns the first directory holding initdb, pg_ctl and postgres whose
// major version is acceptable: the explicit directory (which must match),
// then PATH in order, then well-known install locations, highest major first
// when no major is wanted.
func (s binSearch) find() (string, int, error) {
	if s.Explicit != "" {
		if !hasServerBinaries(s.Explicit) {
			return "", 0, fmt.Errorf("%w: %s does not contain %s", ErrNotFound, s.Explicit, strings.Join(serverBinaries, ", "))
		}
		major, err := s.Version(s.Explicit)
		if err != nil {
			return "", 0, err
		}
		if s.Major != 0 && major != s.Major {
			return "", 0, fmt.Errorf("%w: %s holds PostgreSQL %d, want %d", ErrNotFound, s.Explicit, major, s.Major)
		}
		return s.Explicit, major, nil
	}
	var rejected []string
	seen := map[string]bool{}
	try := func(dir string) (int, bool) {
		if dir == "" || seen[dir] {
			return 0, false
		}
		seen[dir] = true
		if !hasServerBinaries(dir) {
			return 0, false
		}
		major, err := s.Version(dir)
		if err != nil {
			rejected = append(rejected, fmt.Sprintf("%s (%v)", dir, err))
			return 0, false
		}
		if s.Major != 0 && major != s.Major {
			rejected = append(rejected, fmt.Sprintf("%s (PostgreSQL %d)", dir, major))
			return 0, false
		}
		return major, true
	}
	for _, dir := range s.PathDirs {
		if major, ok := try(dir); ok {
			return dir, major, nil
		}
	}
	type found struct {
		dir   string
		major int
	}
	var candidates []found
	if s.WellKnown != nil {
		for _, dir := range s.WellKnown(s.Major) {
			if major, ok := try(dir); ok {
				candidates = append(candidates, found{dir, major})
			}
		}
	}
	if len(candidates) > 0 {
		sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].major > candidates[j].major })
		return candidates[0].dir, candidates[0].major, nil
	}
	want := "any version"
	if s.Major != 0 {
		want = "PostgreSQL " + strconv.Itoa(s.Major)
	}
	msg := fmt.Sprintf("%v: looked for %s in $%s, PATH and common install locations", ErrNotFound, want, EnvBin)
	if len(rejected) > 0 {
		msg += "; rejected " + strings.Join(rejected, ", ")
	}
	return "", 0, &notFoundError{msg: msg}
}

type notFoundError struct{ msg string }

func (e *notFoundError) Error() string { return e.msg }
func (e *notFoundError) Unwrap() error { return ErrNotFound }

func hasServerBinaries(dir string) bool {
	for _, name := range serverBinaries {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil || info.IsDir() || info.Mode()&0o111 == 0 {
			return false
		}
	}
	return true
}

// wellKnownBinDirs lists package-manager install locations. With major set it
// lists only that major's locations; otherwise every installed version.
func wellKnownBinDirs(major int) []string {
	version := "*"
	if major != 0 {
		version = strconv.Itoa(major)
	}
	var patterns []string
	switch runtime.GOOS {
	case "darwin":
		patterns = []string{
			"/opt/homebrew/opt/postgresql@" + version + "/bin",
			"/usr/local/opt/postgresql@" + version + "/bin",
			"/Applications/Postgres.app/Contents/Versions/" + version + "/bin",
			"/opt/local/lib/postgresql" + version + "/bin", // MacPorts
		}
	default:
		patterns = []string{
			"/usr/lib/postgresql/" + version + "/bin", // Debian, Ubuntu
			"/usr/pgsql-" + version + "/bin",          // PGDG RPMs
			"/home/linuxbrew/.linuxbrew/opt/postgresql@" + version + "/bin",
		}
	}
	unversioned := []string{
		"/opt/homebrew/opt/postgresql/bin",
		"/usr/local/opt/postgresql/bin",
		"/Applications/Postgres.app/Contents/Versions/latest/bin",
		"/usr/local/pgsql/bin",
		"/usr/lib/postgresql/bin",
	}
	var dirs []string
	for _, pattern := range patterns {
		matches, _ := filepath.Glob(pattern)
		sort.Strings(matches)
		dirs = append(dirs, matches...)
	}
	return append(dirs, unversioned...)
}

var majorPattern = regexp.MustCompile(`\(PostgreSQL\)\s+(\d+)(?:\.(\d+))?`)

// parseServerMajor reads the major version from `postgres --version` output,
// such as "postgres (PostgreSQL) 15.19" or "postgres (PostgreSQL) 9.6.24".
func parseServerMajor(output string) (int, error) {
	m := majorPattern.FindStringSubmatch(output)
	if m == nil {
		return 0, fmt.Errorf("unrecognised version output %q", strings.TrimSpace(output))
	}
	major, _ := strconv.Atoi(m[1])
	return major, nil
}

var majorCache sync.Map // dir -> int

func cachedServerMajor(dir string) (int, error) {
	if v, ok := majorCache.Load(dir); ok {
		return v.(int), nil
	}
	out, err := exec.Command(filepath.Join(dir, "postgres"), "--version").Output()
	if err != nil {
		return 0, fmt.Errorf("postgres --version: %w", err)
	}
	major, err := parseServerMajor(string(out))
	if err != nil {
		return 0, err
	}
	majorCache.Store(dir, major)
	return major, nil
}

// clientTool returns the path of a client program (psql, pg_restore) that
// ships next to the server binaries, or one on PATH.
func (c *Cluster) clientTool(name string) (string, error) {
	path := filepath.Join(c.binDir, name)
	if info, err := os.Stat(path); err == nil && !info.IsDir() {
		return path, nil
	}
	if path, err := exec.LookPath(name); err == nil {
		return path, nil
	}
	return "", fmt.Errorf("pgscratch: %s not found in %s or PATH", name, c.binDir)
}
