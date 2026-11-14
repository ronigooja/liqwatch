package bybit

import (
	"bufio"
	"github.com/ronigooja/liqwatch/internal/venue"
	"os"
	"path/filepath"
	"testing"
)

func TestThrottleChannelsInFixtures(t *testing.T) {
	a := &Adapter{}
	seen := map[string]bool{}
	paths, err := filepath.Glob("testdata/*.jsonl")
	if err != nil || len(paths) == 0 {
		t.Fatal("fixtures", err)
	}
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		s := bufio.NewScanner(f)
		s.Buffer(make([]byte, 4096), 16<<20)
		for s.Scan() {
			for _, r := range a.Split(venue.Shard{Name: "fixture"}, s.Bytes()) {
				seen[r.Channel] = true
			}
		}
		if err := s.Err(); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	for ch := range a.Throttles() {
		if !seen[ch] {
			t.Errorf("declared channel %q absent from fixtures", ch)
		}
	}
}
