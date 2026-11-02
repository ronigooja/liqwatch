package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaultsAndValidation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	contents := "data_dir = \"/tmp/wicker\"\n[collect]\ndepth_symbols = [\"BTCUSDT\"]\n"
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Archive.ChunkMB != 64 || c.Checks.DiskWarnPct != 80 {
		t.Fatalf("defaults not applied: %+v", c)
	}
	bad := filepath.Join(dir, "bad.toml")
	if err := os.WriteFile(bad, []byte("[collect]\ndepth_symbols=[]\n[archive]\nchunk_mb=0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(bad); err == nil {
		t.Fatal("expected validation error")
	}
}
