// Package symbols loads the tradable universe from exchangeInfo and ranks it by volume.
// Minimal Symbol Manager for stage 1: one fetch at startup, snapshot saved under state/symbols/.
package symbols

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const restBase = "https://fapi.binance.com"

type Universe struct {
	All               []string // USDT perpetual, TRADING, excluding TRADIFI_PERPETUAL
	WeightPerMinLimit int
}

func Load(ctx context.Context, stateDir string) (*Universe, error) {
	body, err := get(ctx, restBase+"/fapi/v1/exchangeInfo")
	if err != nil {
		return nil, err
	}
	var v struct {
		RateLimits []struct {
			RateLimitType string `json:"rateLimitType"`
			Interval      string `json:"interval"`
			IntervalNum   int    `json:"intervalNum"`
			Limit         int    `json:"limit"`
		} `json:"rateLimits"`
		Symbols []struct {
			Symbol            string   `json:"symbol"`
			Status            string   `json:"status"`
			ContractType      string   `json:"contractType"`
			QuoteAsset        string   `json:"quoteAsset"`
			UnderlyingType    string   `json:"underlyingType"`
			UnderlyingSubType []string `json:"underlyingSubType"`
		} `json:"symbols"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return nil, err
	}
	u := &Universe{WeightPerMinLimit: 1200}
	for _, l := range v.RateLimits {
		if l.RateLimitType == "REQUEST_WEIGHT" && l.Interval == "MINUTE" && l.IntervalNum == 1 && l.Limit > 0 {
			u.WeightPerMinLimit = l.Limit
		}
	}
	for _, s := range v.Symbols {
		if s.ContractType != "PERPETUAL" || s.Status != "TRADING" || s.QuoteAsset != "USDT" || isTradfi(s.UnderlyingType, s.UnderlyingSubType) {
			continue
		}
		u.All = append(u.All, s.Symbol)
	}
	sort.Strings(u.All)
	if len(u.All) == 0 {
		return nil, fmt.Errorf("exchangeInfo returned no tradable symbols")
	}
	saveSnapshot(stateDir, body)
	return u, nil
}

func isTradfi(t string, sub []string) bool {
	if strings.EqualFold(t, "TRADIFI_PERPETUAL") {
		return true
	}
	for _, s := range sub {
		if strings.EqualFold(s, "TRADIFI_PERPETUAL") {
			return true
		}
	}
	return false
}

// TopByVolume returns up to n symbols from allowed, ranked by 24h quote volume.
func TopByVolume(ctx context.Context, allowed []string, n int) ([]string, error) {
	body, err := get(ctx, restBase+"/fapi/v1/ticker/24hr")
	if err != nil {
		return nil, err
	}
	var t []struct {
		Symbol      string `json:"symbol"`
		QuoteVolume string `json:"quoteVolume"`
	}
	if err := json.Unmarshal(body, &t); err != nil {
		return nil, err
	}
	ok := make(map[string]bool, len(allowed))
	for _, s := range allowed {
		ok[s] = true
	}
	type kv struct {
		s string
		v float64
	}
	var l []kv
	for _, x := range t {
		if ok[x.Symbol] {
			f, _ := strconv.ParseFloat(x.QuoteVolume, 64)
			l = append(l, kv{x.Symbol, f})
		}
	}
	sort.Slice(l, func(i, j int) bool { return l[i].v > l[j].v })
	var out []string
	for i := 0; i < len(l) && i < n; i++ {
		out = append(out, l[i].s)
	}
	return out, nil
}

func get(ctx context.Context, url string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func saveSnapshot(stateDir string, body []byte) {
	if stateDir == "" {
		return
	}
	dir := filepath.Join(stateDir, "symbols")
	if os.MkdirAll(dir, 0o750) != nil {
		return
	}
	sum := sha256.Sum256(body)
	name := time.Now().UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(sum[:4]) + ".json"
	_ = os.WriteFile(filepath.Join(dir, name), body, 0o640)
}
