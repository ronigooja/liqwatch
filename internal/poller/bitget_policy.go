package poller

func init() {
	// Ratio channels need no policy: every documented row field is retained.
	for channel, p := range map[string]Policy{
		"takerBuySell": {DropChannel: true, TimestampKey: "ts"},
		"positionTiers": {
			Keep: []string{"data"}, Drop: []string{"code", "msg", "requestTime"},
			Nested: map[string]Policy{"data[]": {Keep: []string{"level", "startUnit", "endUnit", "leverage", "keepMarginRate"}, Drop: []string{"symbol"}}},
		},
		"contracts": {
			Keep: []string{"data"}, Drop: []string{"code", "msg", "requestTime"},
			Nested: map[string]Policy{"data[]": {
				Keep: []string{"symbol", "baseCoin", "quoteCoin", "symbolType", "symbolStatus", "fundInterval", "minLever", "maxLever"},
				Drop: []string{"buyLimitPriceRatio", "sellLimitPriceRatio", "feeRateUpRatio", "makerFeeRate", "takerFeeRate", "openCostUpRatio", "supportMarginCoins", "minTradeNum", "priceEndStep", "volumePlace", "pricePlace", "sizeMultiplier", "minTradeUSDT", "maxSymbolOrderNum", "maxProductOrderNum", "maxPositionNum", "offTime", "limitOpenTime", "deliveryTime", "deliveryStartTime", "deliveryPeriod", "launchTime", "posLimit", "maintainTime", "openTime", "maxMarketOrderQty", "maxOrderQty", "isRwa"},
			}},
		},
	} {
		if err := RegisterPolicy("bitget", channel, p); err != nil {
			panic(err)
		}
	}
}
