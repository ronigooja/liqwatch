package pickdepth

import (
	"testing"
	"time"
)

func candle(day int, hour int, high, low, close, volume float64) Candle {
	return Candle{OpenTime: time.Date(2026, 1, day, hour, 0, 0, 0, time.UTC), Open: 100, High: high, Low: low, Close: close, QuoteVol: volume}
}

func TestComputeMetrics(t *testing.T) {
	m := ComputeMetrics("X", []Candle{
		candle(1, 0, 103, 99, 101.5, 10),
		candle(1, 1, 104, 100, 100.5, 20),
		candle(2, 0, 101, 99.5, 100, 30),
	}, 2)
	if m.MeanDailyQuote != 30 {
		t.Fatalf("mean volume = %v", m.MeanDailyQuote)
	}
	if m.WickCount != 2 {
		t.Fatalf("wick count = %d", m.WickCount)
	}
	if m.DominantWickCount != 1 {
		t.Fatalf("dominant count = %d", m.DominantWickCount)
	}
	if m.MaxRangePct != 4 {
		t.Fatalf("max range = %v", m.MaxRangePct)
	}
}

func TestSelectControlsAndSpikes(t *testing.T) {
	metrics := []Metrics{
		{Symbol: "A", MeanDailyQuote: 100, WickCount: 1},
		{Symbol: "B", MeanDailyQuote: 90, WickCount: 3},
		{Symbol: "C", MeanDailyQuote: 80, WickCount: 9},
		{Symbol: "D", MeanDailyQuote: 70, WickCount: 8},
		{Symbol: "E", MeanDailyQuote: 1, WickCount: 99},
	}
	got, err := Select(metrics, Options{N: 4, Controls: 2, MinQuoteVol: 50})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Symbol != "A" || got[0].Role != "control" || got[1].Symbol != "B" || got[1].Role != "control" {
		t.Fatalf("controls = %#v", got[:2])
	}
	if got[2].Symbol != "C" || got[3].Symbol != "D" {
		t.Fatalf("spikes = %#v", got[2:])
	}
}

func TestEligibleSymbols(t *testing.T) {
	got := eligibleSymbols([]Symbol{
		{Symbol: "ok", ContractType: "PERPETUAL", Status: "TRADING", QuoteAsset: "USDT"},
		{Symbol: "type", ContractType: "PERPETUAL", Status: "TRADING", QuoteAsset: "USDT", UnderlyingType: "TRADIFI_PERPETUAL"},
		{Symbol: "subtype", ContractType: "PERPETUAL", Status: "TRADING", QuoteAsset: "USDT", UnderlyingSubType: []string{"TRADIFI_PERPETUAL"}},
	})
	if len(got) != 1 || got[0].Symbol != "ok" {
		t.Fatalf("eligible = %#v", got)
	}
}
