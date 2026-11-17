package rawlog

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestSweepIgnoresDeleting(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	for _, day := range []string{"20261116", "20261115.deleting"} {
		path := filepath.Join(root, "v/s", day, "trade.jsonl")
		if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("x\n"), 0640); err != nil {
			t.Fatal(err)
		}
	}
	want := []Closed{{Path: "v/s/20261116/trade.jsonl", Venue: "v", Symbol: "s", Channel: "trade", Day: time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC), Bytes: 2, MsgCount: -1}}
	if got := Sweep(root, now); !reflect.DeepEqual(got, want) {
		t.Fatal(got, want)
	}
}
