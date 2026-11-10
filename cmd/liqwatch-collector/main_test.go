package main

import (
	"testing"

	"github.com/wicker/wicker/internal/collector"
)

func TestLockInstanceIsExclusive(t *testing.T) {
	dir := t.TempDir()
	unlock, err := lockInstance(dir, "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockInstance(dir, "a"); err == nil {
		t.Fatal("second lock on the same slot must fail")
	}
	other, err := lockInstance(dir, "b")
	if err != nil {
		t.Fatal("other slot must be independent:", err)
	}
	other()
	unlock()
	again, err := lockInstance(dir, "a")
	if err != nil {
		t.Fatal("slot must be lockable after release:", err)
	}
	again()
}

func TestReadinessCountsDistinctShards(t *testing.T) {
	var r readiness
	r.expectShards(3)
	r.shardOpened(collector.Shard{Name: "trade00"})
	r.shardOpened(collector.Shard{Name: "trade00"}) // reconnect of the same shard
	r.shardOpened(collector.Shard{Name: "depth"})
	if r.open() != 2 || r.expected() != 3 {
		t.Fatalf("open=%d expected=%d", r.open(), r.expected())
	}
}
