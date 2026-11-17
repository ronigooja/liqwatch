package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRecordConfig(t *testing.T) {
	for _, tc := range []struct {
		body     string
		compress bool
		level    int
		bad      bool
	}{
		{"", false, 1, false},
		{"[record]\ncompress = true\n", true, 1, false},
		{"[record]\ncompress = true\nlevel = 3\n", true, 3, false},
		{"[record]\nlevel = 0\n", false, 0, true},
		{"[record]\nlevel = 23\n", false, 0, true},
	} {
		p := filepath.Join(t.TempDir(), "config.toml")
		if err := os.WriteFile(p, []byte(tc.body), 0600); err != nil {
			t.Fatal(err)
		}
		c, err := Load(p)
		if tc.bad {
			if err == nil {
				t.Fatal("accepted invalid level")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if c.Record.Compress != tc.compress || c.Record.Level != tc.level {
			t.Fatalf("%+v", c.Record)
		}
	}
}
