package venuemap

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "map.toml")
	write := func(s string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(s), 0600); err != nil {
			t.Fatal(err)
		}
	}
	pair := func(b, v, s, status string) string {
		return "[[pair]]\nbinance = \"" + b + "\"\nvenue = \"" + v + "\"\nsymbol = \"" + s + "\"\nexp10 = 3\nstatus = \"" + status + "\"\n"
	}
	write(pair("BTCUSDT", "bybit", "BTCUSDT", "auto") + pair("ETHUSDT", "bybit", "ETHUSDT", "review"))
	m, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Symbols("bybit"); !reflect.DeepEqual(got, []string{"BTCUSDT"}) {
		t.Fatal(got)
	}
	if got, ok := m.Binance("bybit", "BTCUSDT"); !ok || got != "BTCUSDT" {
		t.Fatal(got, ok)
	}
	if got, ok := m.Exp10("bybit", "BTCUSDT"); !ok || got != 3 {
		t.Fatal(got, ok)
	}
	if _, ok := m.Binance("bybit", "ETHUSDT"); ok {
		t.Fatal("review is usable")
	}
	if len(m.ByStatus("review")) != 1 {
		t.Fatal("missing review")
	}
	for _, input := range []string{
		pair("A", "other", "A", "auto"), pair("A", "bybit", "A", "bad"),
		pair("A", "bybit", "A", "auto") + pair("B", "bybit", "A", "review"),
		pair("A", "bybit", "A", "auto") + pair("A", "bybit", "B", "reviewed"),
	} {
		write(input)
		if _, err := Load(path); err == nil {
			t.Errorf("accepted invalid input %s", input)
		}
	}
}
