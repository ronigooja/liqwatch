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

func TestSpikeCount(t *testing.T) {
	var cs []Candle
	for i := 0; i < 20; i++ { // quiet: 0.1% range
		cs = append(cs, Candle{OpenTime: time.Date(2026, 1, 1, 0, i, 0, 0, time.UTC), Open: 100, High: 100.1, Low: 100, Close: 100.05, QuoteVol: 1})
	}
	cs = append(cs, Candle{OpenTime: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC), Open: 100, High: 100.2, Low: 97, Close: 99.9, QuoteVol: 1}) // spike: lower wick, pulled back
	cs = append(cs, Candle{OpenTime: time.Date(2026, 1, 1, 1, 1, 0, 0, time.UTC), Open: 100, High: 100.2, Low: 97, Close: 97.2, QuoteVol: 1}) // trend candle: body large
	m := ComputeMetrics("X", cs, 2)
	if m.SpikeCount != 1 {
		t.Fatalf("spike count = %d", m.SpikeCount)
	}
	loud := make([]Candle, 0, 21)
	for i := 0; i < 20; i++ { // always loud: median range 4%, a 3% wick is not a spike
		loud = append(loud, Candle{OpenTime: time.Date(2026, 1, 1, 0, i, 0, 0, time.UTC), Open: 100, High: 102, Low: 98, Close: 100, QuoteVol: 1})
	}
	loud = append(loud, Candle{OpenTime: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC), Open: 100, High: 100.1, Low: 97, Close: 99.9, QuoteVol: 1})
	if m := ComputeMetrics("Y", loud, 2); m.SpikeCount != 0 {
		t.Fatalf("loud symbol spike count = %d", m.SpikeCount)
	}
}
