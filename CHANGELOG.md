# Changelog

All notable changes to pgscratch are listed here. The project follows
[semantic versioning](https://semver.org/).

## Unreleased

## v1.0.1 (2026-11-07)

- README: a plain-language account of the problem pgscratch solves, what it
  does step by step, and who it is for. No code changes.

## v1.0.0 (2026-11-07)

- First stable release. The API in v0.1.0 is unchanged.
- CONTRIBUTING.md, a CI matrix (Linux and macOS, several PostgreSQL majors,
  `-race`) and dependabot.
- README: contributing and security sections.

## v0.1.0 (2026-11-07)

- Throwaway clusters found from local binaries: `Start` (one per process),
  `New` (caller-owned), shared mode across processes, a watchdog that cleans
  up on any exit, and a sweep of stale clusters.
- A database per test with `NewDatabase`. Creation is lock-free and runs in
  parallel.
- Seeded templates: `Migrate`, `Func`, `SQLFiles`, `Dump`, `CopyCSV`,
  `CopyCSVDir` and `From`. Templates are content-keyed, built once under a
  per-template lock and cloned per test, with an optional persistent cache.
