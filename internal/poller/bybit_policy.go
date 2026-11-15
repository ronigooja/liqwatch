package poller

func init() {
	policies := map[string]Policy{
		"accountRatio": {Keep: []string{"buyRatio", "sellRatio", "timestamp"}, Drop: []string{"symbol"}, TimestampKey: "timestamp"},
		"riskLimit": {Keep: []string{"time", "result"}, Drop: []string{"retCode", "retMsg", "retExtInfo"}, Nested: map[string]Policy{
			"result":        {Keep: []string{"category", "list"}, Drop: []string{"nextPageCursor"}},
			"result.list[]": {Keep: []string{"id", "symbol", "riskLimitValue", "maintenanceMargin", "initialMargin", "isLowestRisk", "maxLeverage", "mmDeduction"}},
		}},
		"instruments": {Keep: []string{"time", "result"}, Drop: []string{"retCode", "retMsg", "retExtInfo"}, Nested: map[string]Policy{
			"result":                       {Keep: []string{"category", "list"}, Drop: []string{"nextPageCursor"}},
			"result.list[]":                {Keep: []string{"symbol", "contractType", "baseCoin", "quoteCoin", "settleCoin", "fundingInterval", "leverageFilter"}, Drop: []string{"symbolId", "status", "launchTime", "deliveryTime", "deliveryFeeRate", "priceScale", "priceFilter", "lotSizeFilter", "unifiedMarginTrade", "copyTrading", "upperFundingRate", "lowerFundingRate", "displayName", "fullName", "marketRegion", "underlyingTicker", "tags", "forbidUplWithdrawal", "riskParameters", "isPreListing", "preListingInfo", "symbolType"}},
			"result.list[].leverageFilter": {Keep: []string{"minLeverage", "maxLeverage", "leverageStep"}},
		}},
	}
	for channel, policy := range policies {
		if err := RegisterPolicy("bybit", channel, policy); err != nil {
			panic(err)
		}
	}
}
