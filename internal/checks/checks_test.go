package checks

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAlertStateTransitionsOnce(t *testing.T) {
	dir := t.TempDir()
	s, err := NewAlertState(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.Now = func() time.Time { return time.Unix(1, 0) }
	if err := s.Set("disk.high", true, "warning", "disk.high", "high", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Set("disk.high", true, "warning", "disk.high", "high", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Set("disk.high", false, "info", "disk.high", "resolved", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Set("disk.high", false, "info", "disk.high", "resolved", nil); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(dir, "state", "alerts.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var events []Event
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		var e Event
		if err := json.Unmarshal(scan.Bytes(), &e); err != nil {
			t.Fatal(err)
		}
		events = append(events, e)
	}
	if len(events) != 2 || events[0].State != "firing" || events[1].State != "resolved" {
		t.Fatalf("events = %+v", events)
	}
}
