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
			want := Archive{Mode: tc.mode, DeleteSources: tc.deletion, Retention: true, RetentionHighPct: 90, RetentionLowPct: 80, RetentionMaxDaysPerRun: 3, MaxPackMB: 400, FullVerifyDays: 7, VerifySamplePercent: 10}
			if !reflect.DeepEqual(c.Archive, want) {
				t.Fatalf("archive=%+v want=%+v", c.Archive, want)
			}
		})
	}
}

func TestRetentionConfig(t *testing.T) {
	for _, text := range []string{"retention_low_pct=0", "retention_high_pct=0", "retention_low_pct=90", "retention_high_pct=80", "retention_high_pct=100", "retention_max_days_per_run=0", "retention_max_days_per_run=-1"} {
		t.Run(text, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "config.toml")
			if err := os.WriteFile(p, []byte("[archive]\nmode='local'\n"+text), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(p); err == nil {
				t.Fatal("accepted invalid retention")
			}
		})
	}
	p := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(p, []byte("[archive]\nmode='local'\nretention=false\nretention_low_pct=61\nretention_high_pct=93\nretention_max_days_per_run=5"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	want := Archive{Mode: "local", DeleteSources: true, Retention: false, RetentionLowPct: 61, RetentionHighPct: 93, RetentionMaxDaysPerRun: 5, MaxPackMB: 400, FullVerifyDays: 7, VerifySamplePercent: 10}
	if !reflect.DeepEqual(c.Archive, want) {
		t.Fatal(c.Archive, want)
	}
}
