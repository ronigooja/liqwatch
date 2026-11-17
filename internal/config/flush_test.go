package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRecordFlush(t *testing.T) {
	for _, tc := range []struct {
		name, body        string
		want              Record
		collector, poller time.Duration
	}{
		{"plain", "", Record{Level: 1}, 5 * time.Second, time.Second},
		{"compressed", "compress=true", Record{Compress: true, Level: 1}, 30 * time.Second, 30 * time.Second},
		{"plain override", "flush='120ms'", Record{Level: 1, Flush: "120ms"}, 120 * time.Millisecond, 120 * time.Millisecond},
		{"compressed override", "compress=true\nflush='5m'", Record{Compress: true, Level: 1, Flush: "5m"}, 5 * time.Minute, 5 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "config.toml")
			if err := os.WriteFile(p, []byte("[record]\n"+tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(p)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(c.Record, tc.want) {
				t.Fatalf("record got %+v want %+v", c.Record, tc.want)
			}
			if got := c.Record.FlushEvery(5 * time.Second); got != tc.collector {
				t.Fatalf("collector %s want %s", got, tc.collector)
			}
			if got := c.Record.FlushEvery(time.Second); got != tc.poller {
				t.Fatalf("poller %s want %s", got, tc.poller)
			}
		})
	}
}

func TestRecordFlushInvalid(t *testing.T) {
	for _, value := range []string{"0s", "-1s", "301s", "5m1ns", "thirty", "30"} {
		t.Run(value, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "config.toml")
			if err := os.WriteFile(p, []byte("[record]\nflush='"+value+"'"), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(p)
			if err == nil || !strings.Contains(err.Error(), "record.flush") || !strings.Contains(err.Error(), "5m") {
				t.Fatalf("error %v", err)
			}
		})
	}
}
