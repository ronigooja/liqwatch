package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestPackPaths(t *testing.T) {
	for _, tc := range []struct {
		name, table string
		want        [3]string
		bad         bool
	}{
		{"omitted", "", [3]string{"/data/liqwatch", "/data/liqwatch/packs", "/data/liqwatch/manifest/manifest.db"}, false},
		{"empty", "[pack]\ndir = \"\"", [3]string{"/data/liqwatch", "/data/liqwatch/packs", "/data/liqwatch/manifest/manifest.db"}, false},
		{"separate", "[pack]\ndir = \"/var/lib/liqwatch/pack\"", [3]string{"/var/lib/liqwatch/pack", "/var/lib/liqwatch/pack/packs", "/var/lib/liqwatch/pack/manifest/manifest.db"}, false},
		{"relative", "[pack]\ndir = \"pack\"", [3]string{}, true},
		{"raw", "[pack]\ndir = \"/data/liqwatch/raw\"", [3]string{}, true},
		{"inside", "[pack]\ndir = \"/data/liqwatch/raw/v/../packs\"", [3]string{}, true},
		{"prefix-only", "[pack]\ndir = \"/data/liqwatch/raw-other\"", [3]string{"/data/liqwatch/raw-other", "/data/liqwatch/raw-other/packs", "/data/liqwatch/raw-other/manifest/manifest.db"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.toml")
			if err := os.WriteFile(path, []byte(fmt.Sprintf("data_dir = %q\n%s\n", "/data/liqwatch", tc.table)), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(path)
			if tc.bad {
				if err == nil {
					t.Fatal("accepted invalid pack.dir")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got := [3]string{c.PackDir(), c.PacksPath(), c.ManifestPath()}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("paths = %#v, want %#v", got, tc.want)
			}
		})
	}
}
