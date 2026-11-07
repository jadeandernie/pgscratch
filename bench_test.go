package pgscratch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// benchSeed writes a schema of 20 tables (each with two indexes) and a
// 50,000-row CSV, the kind of fixture a service's tests load.
func benchSeed(b *testing.B) (schemaPath, csvPath string) {
	b.Helper()
	dir := b.TempDir()
	var schema strings.Builder
	for i := 0; i < 20; i++ {
		fmt.Fprintf(&schema, "CREATE TABLE t%02d (id bigint PRIMARY KEY, account text NOT NULL, amount numeric(12,2), created timestamptz NOT NULL DEFAULT now(), note text);\n", i)
		fmt.Fprintf(&schema, "CREATE INDEX ON t%02d (account); CREATE INDEX ON t%02d (created);\n", i, i)
	}
	schemaPath = filepath.Join(dir, "schema.sql")
	if err := os.WriteFile(schemaPath, []byte(schema.String()), 0o600); err != nil {
		b.Fatal(err)
	}
	var rows strings.Builder
	rows.WriteString("id,account,amount,note\n")
	for i := 1; i <= 50000; i++ {
		fmt.Fprintf(&rows, "%d,acct-%d,%d.%02d,row %d of the benchmark fixture\n", i, i%977, i%10000, i%100, i)
	}
	csvPath = filepath.Join(dir, "t00.csv")
	if err := os.WriteFile(csvPath, []byte(rows.String()), 0o600); err != nil {
		b.Fatal(err)
	}
	return schemaPath, csvPath
}

func benchCluster(b *testing.B) *Cluster {
	b.Helper()
	c, err := Start(testOptions())
	if err != nil {
		b.Skip(err)
	}
	return c
}

// BenchmarkCloneSeededTemplate is the cost per test with a template: one
// CREATE DATABASE ... TEMPLATE and one DROP DATABASE.
func BenchmarkCloneSeededTemplate(b *testing.B) {
	c := benchCluster(b)
	ctx := context.Background()
	schemaPath, csvPath := benchSeed(b)
	tmpl, err := c.Template(ctx, "bench_seeded", SQLFiles(nil, schemaPath), CopyCSV("t00", csvPath))
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("pgs_t_bench_clone_%d", i)
		if err := tmpl.CreateDatabase(ctx, name); err != nil {
			b.Fatal(err)
		}
		if err := c.DropDatabase(ctx, name); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSeedFreshDatabase is the cost per test without one: create an
// empty database, run the schema and load the CSV, then drop it.
func BenchmarkSeedFreshDatabase(b *testing.B) {
	c := benchCluster(b)
	ctx := context.Background()
	schemaPath, csvPath := benchSeed(b)
	schema, err := os.ReadFile(schemaPath)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("pgs_t_bench_fresh_%d", i)
		if err := c.CreateDatabase(ctx, name, ""); err != nil {
			b.Fatal(err)
		}
		bld := &build{c: c, db: name}
		conn, err := bld.conn(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if err := conn.Exec(ctx, string(schema)); err != nil {
			b.Fatal(err)
		}
		if err := bld.copyCSV(ctx, "t00", csvPath); err != nil {
			b.Fatal(err)
		}
		bld.closeConn()
		if err := c.DropDatabase(ctx, name); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCreateEmptyDatabase is the floor: an empty database per test.
func BenchmarkCreateEmptyDatabase(b *testing.B) {
	c := benchCluster(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("pgs_t_bench_empty_%d", i)
		if err := c.CreateDatabase(ctx, name, ""); err != nil {
			b.Fatal(err)
		}
		if err := c.DropDatabase(ctx, name); err != nil {
			b.Fatal(err)
		}
	}
}
