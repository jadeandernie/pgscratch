# Contributing to pgscratch

Contributions are welcome: bug fixes, new seeding steps, platform support,
docs, benchmarks. There is no need to ask first. Raise a pull request against
`main`.

Work done by AI coding agents is welcome too, on the same terms as anyone
else's. You don't have to say a change was agent-written, though it helps
reviewers to know. Whoever opens the pull request is answerable for it: it
should build, pass the tests, and do what its description says.

## Making a change

1. Fork the repository and branch from `main`.
2. Make the change. Keep the core on the standard library. A new dependency
   needs a strong reason in the pull request description.
3. Add or update tests. Anything that touches locking, templates or cleanup
   needs a test that runs in parallel.
4. Run the checks below, then open a pull request against `main`.

## Checks

Install PostgreSQL server binaries (`initdb`, `pg_ctl`, `postgres`, plus
`pg_restore` and `psql` for the dump tests). Any major from 9.6 up works, and
CI runs several. Then:

```sh
gofmt -s -l .        # prints nothing
go vet ./...
go test -race ./...
```

`PGSCRATCH_TEST_MAJOR=15 go test -race ./...` pins the tests to one major. With
no binaries installed the tests skip rather than fail, so check that the output
shows them running.

## Pull requests

- One change per pull request, with a description of what it changes and how
  you tested it.
- Keep the public API backwards compatible. pgscratch follows semantic
  versioning from v1.0.0, so a breaking change needs a major version and a
  strong case.
- Add a line to `CHANGELOG.md` under "Unreleased".

## Reporting bugs

Open an issue with the PostgreSQL major, the OS, the Go version, and the
smallest test that shows the problem. Report security problems privately
through GitHub's "Report a vulnerability" on the Security tab, not in a public
issue.
