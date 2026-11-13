package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type instrument struct {
	Symbol, Base string
	Price        float64
}
type fetcher interface{ Get(string, any) error }
type httpFetcher struct {
	client *http.Client
	last   time.Time
}

func (f *httpFetcher) Get(u string, out any) error {
	if d := 600*time.Millisecond - time.Since(f.last); d > 0 {
		time.Sleep(d)
	}
	f.last = time.Now()
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	r, err := f.client.Do(req)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return fmt.Errorf("%s: HTTP %d", u, r.StatusCode)
	}
	if err := json.NewDecoder(r.Body).Decode(out); err != nil {
		return fmt.Errorf("%s: %w", u, err)
	}
	return nil
}

type row map[string]any

func str(r row, key string) string {
	if s, ok := r[key].(string); ok {
		return s
	}
	return ""
}
func num(r row, key string) float64 {
	switch v := r[key].(type) {
	case float64:
		return v
	case string:
		n, _ := strconv.ParseFloat(v, 64)
		return n
	}
	return 0
}
func rows(f fetcher, u, key string) ([]row, error) {
	var v any
	if err := f.Get(u, &v); err != nil {
		return nil, err
	}
	if key != "" {
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s: invalid response", u)
		}
		v = m[key]
	}
	a, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%s: missing %s array", u, key)
	}
	out := make([]row, 0, len(a))
	for _, v := range a {
		if r, ok := v.(map[string]any); ok {
			out = append(out, r)
		}
	}
	return out, nil
}
func discover(f fetcher, venue string) ([]instrument, error) {
	var ins, ticks []row
	var err error
	var ikey, pkey, base string
	switch venue {
	case "binance":
		ins, err = rows(f, "https://fapi.binance.com/fapi/v1/exchangeInfo", "symbols")
		if err == nil {
			ticks, err = rows(f, "https://fapi.binance.com/fapi/v1/premiumIndex", "")
		}
		ikey = "symbol"
		pkey = "markPrice"
		base = "baseAsset"
	case "bybit":
		cursor := ""
		for {
			var d struct {
				RetCode int    `json:"retCode"`
				RetMsg  string `json:"retMsg"`
				Result  struct {
					List []row  `json:"list"`
					Next string `json:"nextPageCursor"`
				} `json:"result"`
			}
			u := "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
			if cursor != "" {
				u += "&cursor=" + url.QueryEscape(cursor)
			}
			if err = f.Get(u, &d); err != nil {
				return nil, err
			}
			if d.RetCode != 0 {
				return nil, fmt.Errorf("bybit: %s", d.RetMsg)
			}
			ins = append(ins, d.Result.List...)
			cursor = d.Result.Next
			if cursor == "" {
				break
			}
		}
		var d struct {
			RetCode int    `json:"retCode"`
			RetMsg  string `json:"retMsg"`
			Result  struct {
				List []row `json:"list"`
			} `json:"result"`
		}
		err = f.Get("https://api.bybit.com/v5/market/tickers?category=linear", &d)
		if err == nil && d.RetCode != 0 {
			err = fmt.Errorf("bybit: %s", d.RetMsg)
		}
		ticks = d.Result.List
		ikey = "symbol"
		pkey = "markPrice"
		base = "baseCoin"
	case "okx":
		ins, err = rows(f, "https://www.okx.com/api/v5/public/instruments?instType=SWAP", "data")
		if err == nil {
			ticks, err = rows(f, "https://www.okx.com/api/v5/public/mark-price?instType=SWAP", "data")
		}
		ikey = "instId"
		pkey = "markPx"
	case "bitget":
		ins, err = rows(f, "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES", "data")
		if err == nil {
			ticks, err = rows(f, "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES", "data")
		}
		ikey = "symbol"
		pkey = "markPrice"
		base = "baseCoin"
	case "gate":
		ins, err = rows(f, "https://api.gateio.ws/api/v4/futures/usdt/contracts", "")
		ticks = ins
		ikey = "name"
		pkey = "mark_price"
	case "mexc":
		ins, err = rows(f, "https://contract.mexc.com/api/v1/contract/detail", "data")
		if err == nil {
			ticks, err = rows(f, "https://contract.mexc.com/api/v1/contract/ticker", "data")
		}
		ikey = "symbol"
		pkey = "fairPrice"
		base = "baseCoin"
	case "kucoin":
		ins, err = rows(f, "https://api-futures.kucoin.com/api/v1/contracts/active", "data")
		ticks = ins
		ikey = "symbol"
		pkey = "markPrice"
		base = "baseCurrency"
	default:
		return nil, fmt.Errorf("unknown venue %q", venue)
	}
	if err != nil {
		return nil, err
	}
	prices := map[string]float64{}
	for _, r := range ticks {
		prices[str(r, ikey)] = num(r, pkey)
	}
	var out []instrument
	for _, r := range ins {
		s := str(r, ikey)
		b := str(r, base)
		ok := false
		switch venue {
		case "binance":
			ok = str(r, "contractType") == "PERPETUAL" && str(r, "status") == "TRADING" && str(r, "quoteAsset") == "USDT"
		case "bybit":
			ok = str(r, "status") == "Trading" && str(r, "contractType") == "LinearPerpetual" && str(r, "quoteCoin") == "USDT" && str(r, "settleCoin") == "USDT"
		case "okx":
			ok = str(r, "ctType") == "linear" && str(r, "settleCcy") == "USDT" && str(r, "state") == "live"
			b = strings.Split(s, "-")[0]
		case "bitget":
			ok = str(r, "symbolType") == "perpetual" && str(r, "symbolStatus") == "normal" && str(r, "quoteCoin") == "USDT"
		case "gate":
			ok = str(r, "status") == "trading" && r["in_delisting"] != true && (str(r, "type") == "direct" || str(r, "type") == "")
			b = strings.Split(s, "_")[0]
		case "mexc":
			ok = num(r, "state") == 0 && num(r, "futureType") == 1 && str(r, "quoteCoin") == "USDT" && str(r, "settleCoin") == "USDT"
		case "kucoin":
			ok = str(r, "settleCurrency") == "USDT" && str(r, "type") == "FFWCSX" && str(r, "status") == "Open"
		}
		if ok && s != "" && b != "" {
			out = append(out, instrument{s, b, prices[s]})
		}
	}
	return out, nil
}
