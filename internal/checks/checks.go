package checks

import (
	"bufio"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "modernc.org/sqlite"

	"github.com/wicker/wicker/internal/config"
)

type Event struct {
	Schema   int               `json:"schema"`
	ID       string            `json:"id"`
	TS       string            `json:"ts"`
	Severity string            `json:"severity"`
	State    string            `json:"state"`
	Type     string            `json:"type"`
	Source   string            `json:"source"`
	Subject  map[string]string `json:"subject"`
	DedupKey string            `json:"dedup_key"`
	Summary  string            `json:"summary"`
	Details  map[string]any    `json:"details"`
}

type stateFile struct {
	Alerts map[string]bool `json:"alerts"`
}

type AlertState struct {
	Path   string
	Events string
	Active map[string]bool
	Now    func() time.Time
}

func NewAlertState(dataDir string) (*AlertState, error) {
	s := &AlertState{Path: filepath.Join(dataDir, "state", "checks-state.json"), Events: filepath.Join(dataDir, "state", "alerts.jsonl"), Active: map[string]bool{}, Now: time.Now}
	b, err := os.ReadFile(s.Path)
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	var f stateFile
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("read checks state: %w", err)
	}
	for k, v := range f.Alerts {
		s.Active[k] = v
	}
	return s, nil
}

func (s *AlertState) Set(dedup string, firing bool, severity, typ, summary string, details map[string]any) error {
	if s.Active[dedup] == firing {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.Path), 0750); err != nil {
		return err
	}
	e := Event{Schema: 1, ID: newULID(s.Now()), TS: s.Now().UTC().Format(time.RFC3339), Severity: severity, State: map[bool]string{true: "firing", false: "resolved"}[firing], Type: typ, Source: "wicker-checks", Subject: map[string]string{}, DedupKey: dedup, Summary: summary, Details: details}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(s.Events, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0640)
	if err != nil {
		return err
	}
	if _, err = fmt.Fprintln(f, string(b)); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	s.Active[dedup] = firing
	b, err = json.Marshal(stateFile{Alerts: s.Active})
	if err != nil {
		return err
	}
	return os.WriteFile(s.Path, append(b, '\n'), 0640)
}

type finding struct {
	Key, Severity, Type, Summary string
	Details                      map[string]any
}

func Run(c *config.Config) error {
	s, err := NewAlertState(c.DataDir)
	if err != nil {
		return err
	}
	findings, err := evaluate(c)
	if err != nil {
		return err
	}
	wanted := make(map[string]finding, len(findings))
	for _, f := range findings {
		wanted[f.Key] = f
		if err := s.Set(f.Key, true, f.Severity, f.Type, f.Summary, f.Details); err != nil {
			return err
		}
	}
	for key := range s.Active {
		if s.Active[key] {
			if _, ok := wanted[key]; !ok {
				if err := s.Set(key, false, "info", typeForKey(key), "condition resolved", nil); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func evaluate(c *config.Config) ([]finding, error) {
	var out []finding
	if pct, err := diskPct(c.DataDir); err != nil {
		return nil, err
	} else if pct >= float64(c.Checks.DiskWarnPct) {
		out = append(out, finding{"disk.high", "warning", "disk.high", "disk usage is high", map[string]any{"percent": pct}})
	}
	if age, ok, err := newestPartAge(filepath.Join(c.DataDir, "raw")); err != nil {
		return nil, err
	} else if ok && age > time.Duration(c.Checks.StaleWriteSec)*time.Second {
		out = append(out, finding{"data.stale_write", "critical", "data.stale_write", "raw part file has not been updated", map[string]any{"age_seconds": int(age.Seconds())}})
	}
	metrics, err := readMetrics(c.Collect.MetricsAddr)
	if err != nil {
		out = append(out, finding{"collector.metrics", "critical", "collector.metrics", "collector metrics endpoint is unreachable", map[string]any{"error": err.Error()}})
	} else {
		if v, ok := metrics["wicker_queue_depth_ratio"]; ok && v > .5 {
			out = append(out, finding{"collector.queue", "warning", "data.queue", "collector queue is above 50%", map[string]any{"ratio": v}})
		}
		for k, v := range metrics {
			if strings.HasPrefix(k, "wicker_dropped_total{") && v > 0 {
				out = append(out, finding{"collector.drops:" + k, "critical", "data.drop", "collector dropped messages", map[string]any{"metric": k, "count": v}})
			}
		}
	}
	if n, ok, err := unverified(c.DataDir, time.Duration(c.Checks.UnverifiedHours)*time.Hour); err != nil {
		return nil, err
	} else if ok && n > 0 {
		out = append(out, finding{"archive.unverified", "warning", "archive.unverified", "closed files remain unverified", map[string]any{"count": n}})
	}
	if offset, ok := clockOffset(); ok && offset > .05 {
		out = append(out, finding{"clock.offset", "warning", "clock.offset", "clock offset is high", map[string]any{"seconds": offset}})
	}
	return out, nil
}

func diskPct(path string) (float64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	if st.Blocks == 0 {
		return 0, nil
	}
	return float64(st.Blocks-st.Bfree) * 100 / float64(st.Blocks), nil
}

func newestPartAge(root string) (time.Duration, bool, error) {
	var newest time.Time
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".part") && info.ModTime().After(newest) {
			newest = info.ModTime()
		}
		return nil
	})
	if err != nil {
		return 0, false, err
	}
	if newest.IsZero() {
		return 0, false, nil
	}
	return time.Since(newest), true, nil
}

func readMetrics(addr string) (map[string]float64, error) {
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("metrics returned %s", resp.Status)
	}
	result := map[string]float64{}
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		v, err := strconv.ParseFloat(fields[1], 64)
		if err == nil {
			result[fields[0]] = v
		}
	}
	return result, scanner.Err()
}

func unverified(dataDir string, age time.Duration) (int, bool, error) {
	path := filepath.Join(dataDir, "manifest", "manifest.db")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return 0, false, nil
	} else if err != nil {
		return 0, true, err
	}
	db, err := sql.Open("sqlite", path+"?mode=ro")
	if err != nil {
		return 0, true, err
	}
	defer db.Close()
	cutoff := time.Now().Add(-age)
	rows, err := db.Query("SELECT closed_at FROM files WHERE state <> 'verified' AND state <> 'deleted' AND closed_at < ?", cutoff.UnixNano())
	if err != nil {
		return 0, true, err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	return n, true, rows.Err()
}

func clockOffset() (float64, bool) {
	out, err := exec.Command("chronyc", "tracking").Output()
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.Contains(strings.ToLower(line), "last offset") {
			fields := strings.Fields(line)
			if len(fields) == 0 {
				continue
			}
			v, err := strconv.ParseFloat(strings.TrimSuffix(fields[len(fields)-1], "s"), 64)
			if err == nil {
				return abs(v), true
			}
		}
	}
	return 0, false
}
func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
func typeForKey(key string) string {
	if i := strings.IndexByte(key, ':'); i >= 0 {
		return key[:i]
	}
	return key
}

func newULID(t time.Time) string {
	var b [10]byte
	_, _ = io.ReadFull(rand.Reader, b[:])
	data := make([]byte, 16)
	ms := t.UnixMilli()
	for i := 5; i >= 0; i-- {
		data[i] = byte(ms)
		ms >>= 8
	}
	copy(data[6:], b[:])
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	value := new(big.Int).SetBytes(data)
	var out [26]byte
	for i := range out {
		shift := uint(5 * (25 - i))
		out[i] = alphabet[new(big.Int).Rsh(new(big.Int).Set(value), shift).Uint64()&31]
	}
	return string(out[:])
}
