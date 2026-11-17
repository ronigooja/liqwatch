package pack

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRenameExclusiveRefusesEmptyDestination(t *testing.T) {
	root := t.TempDir()
	from, to := filepath.Join(root, "day.deleting"), filepath.Join(root, "day")
	for _, p := range []string{from, to} {
		if err := os.Mkdir(p, 0750); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(from, "keep"), []byte("unverified"), 0640); err != nil {
		t.Fatal(err)
	}
	if err := RenameExclusive(from, to); err == nil {
		t.Fatal("replaced existing directory")
	}
	if b, err := os.ReadFile(filepath.Join(from, "keep")); err != nil || string(b) != "unverified" {
		t.Fatal(string(b), err)
	}
	entries, err := os.ReadDir(to)
	if err != nil || len(entries) != 0 {
		t.Fatal(entries, err)
	}
}
