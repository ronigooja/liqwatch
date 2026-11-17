package checks

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCompressedChannelDiscovery(t *testing.T) {
	for _, name := range []string{"trade.jsonl", "trade.a.jsonl", "trade.jsonl.zst", "trade.a.jsonl.zst"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
			dir := filepath.Join(root, "v", "s", "20261117")
			if err := os.MkdirAll(dir, 0750); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, name)
			if err := os.WriteFile(path, nil, 0600); err != nil {
				t.Fatal(err)
			}
			at := now.Add(-time.Minute)
			if err := os.Chtimes(path, at, at); err != nil {
				t.Fatal(err)
			}
			age, found, err := newestChannelAge(root, "v", "trade", now)
			if err != nil || !found || age != time.Minute {
				t.Fatalf("age=%v found=%v err=%v", age, found, err)
			}
		})
	}
}
