package poller

import "strings"

func init() {
	contracts := Policy{Keep: strings.Fields("symbol baseCurrency quoteCurrency settleCurrency multiplier isInverse isQuanto type fundingFeeRate nextFundingRateDateTime nextFundingRateTime fundingRateGranularity currentFundingRateGranularity effectiveFundingRateCycleStartTime openInterest maxLeverage initialMargin maintainMargin maxRiskLimit minRiskLimit riskStep supportCross"), Drop: strings.Fields("adjustActiveTime adjustK adjustM adjustMmrLevConstant assetClass buyLimit commonConfigItems crossRiskLimit dailyInterestRate displayBaseCurrency displaySymbol expireDate f fairMethod firstOpenDate fundingBaseSymbol fundingBaseSymbol1M fundingQuoteSymbol fundingQuoteSymbol1M fundingRateCap fundingRateFloor fundingRateSymbol highPrice indexPrice indexPriceTickSize indexSymbol isDeleverage k lastTimeFundingRate lastTradePrice lotSize lowPrice m makerFeeRate makerFixFee markMethod markPrice marketMaxOrderQty marketStage marketType maxOrderQty maxPrice mmrLevConstant mmrLimit orderPriceRange period preMarketToPerpDate predictedFundingFeeRate premiumsSymbol1M premiumsSymbol8H priceChg priceChgPct rootSymbol sellLimit settleDate settlementFee settlementSymbol sourceExchanges st status subMarketType takerFeeRate takerFixFee tickSize turnoverOf24h volumeOf24h")}
	risk := Policy{Keep: strings.Fields("level maxRiskLimit minRiskLimit maxLeverage initialMargin maintainMargin"), Drop: []string{"symbol"}}
	for ch, row := range map[string]Policy{"contracts": contracts, "riskLimit": risk} {
		p := Policy{Keep: []string{"data"}, Drop: []string{"code"}, Nested: map[string]Policy{"data[]": row}}
		if err := RegisterPolicy("kucoin", ch, p); err != nil {
			panic(err)
		}
	}
}
