package binance

import "testing"

func TestSplitAllMarket(t *testing.T) {
	tests := []struct{ name, input, symbol, channel, payload string }{
		{"mark", `{"stream":"!markPrice@arr@1s","data":[{"s":"BTCUSDT","p":"1"},{"s":"OUTUSDT"}]}`, "BTCUSDT", "markPrice_all", `{"data":{"p":"1"}}`},
		{"liquidation", `{"stream":"!forceOrder@arr","data":{"o":{"s":"BTCUSDT"}}}`, "BTCUSDT", "forceOrder_all", `{"stream":"!forceOrder@arr","data":{"o":{"s":"BTCUSDT"}}}`},
		{"out of scope", `{"stream":"!forceOrder@arr","data":{"o":{"s":"OUTUSDT"}}}`, "", "", ""},
		{"empty", `{"stream":"!markPrice@arr@1s","data":[]}`, "", "", ""},
		{"malformed", `{`, "", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SplitAllMarket([]byte(tt.input), map[string]struct{}{"BTCUSDT": {}})
			if tt.symbol == "" {
				if len(got) != 0 {
					t.Fatalf("got %v", got)
				}
				return
			}
			if len(got) != 1 || got[0].Symbol != tt.symbol || got[0].Channel != tt.channel || string(got[0].Payload) != tt.payload {
				t.Fatalf("got %+v", got)
			}
		})
	}
}
