package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestArchiveDefaultsAndValidation(t *testing.T) {
	for _, tc := range []struct {
		text, mode      string
		deletion, valid bool
	}{
		{"", "webdav", true, true},
		{"[archive]\nmode='local'", "local", true, true},
		{"[archive]\nmode='local'\ndelete_sources=false", "local", false, true},
		{"[archive]\nmode='other'", "", false, false},
		{"[archive]\nmode=''", "", false, false},
		{"data_dir='/tmp/raw-test'\n[pack]\ndir='/tmp/raw-test/raw/packs'", "", false, false},
	} {
		t.Run(tc.text, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "config.toml")
			if err := os.WriteFile(p, []byte(tc.text), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(p)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, err)
			}
			if err != nil {
				return
			}
			want := Archive{Mode: tc.mode, DeleteSources: tc.deletion, MaxPackMB: 400, FullVerifyDays: 7, VerifySamplePercent: 10}
			if !reflect.DeepEqual(c.Archive, want) {
				t.Fatalf("archive=%+v want=%+v", c.Archive, want)
			}
		})
	}
}
