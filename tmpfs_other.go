//go:build !linux

package pgscratch

func tmpfsBaseDir() string { return "" }
