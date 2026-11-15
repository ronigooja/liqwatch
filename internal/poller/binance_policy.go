package poller

import "strings"

// Binance REST payload fields required for modelling and interpretation.
var binancePolicies = map[string]Policy{
	"constituents":                {DropChannel: true},
	"takerlongshortRatio":         {DropChannel: true},
	"openInterestHist":            {Keep: strings.Fields("sumOpenInterest sumOpenInterestValue timestamp"), Drop: strings.Fields("symbol CMCCirculatingSupply"), TimestampKey: "timestamp"},
	"topLongShortPositionRatio":   {Keep: strings.Fields("longShortRatio longAccount shortAccount timestamp"), Drop: strings.Fields("symbol"), TimestampKey: "timestamp"},
	"topLongShortAccountRatio":    {Keep: strings.Fields("longShortRatio longAccount shortAccount timestamp"), Drop: strings.Fields("symbol"), TimestampKey: "timestamp"},
	"globalLongShortAccountRatio": {Keep: strings.Fields("longShortRatio longAccount shortAccount timestamp"), Drop: strings.Fields("symbol"), TimestampKey: "timestamp"},
	"fundingRate":                 {Keep: strings.Fields("fundingTime fundingRate markPrice rateType"), Drop: strings.Fields("symbol"), TimestampKey: "fundingTime"},
	"brackets": {Keep: strings.Fields("updateTime notionalLimit riskBrackets"), Drop: strings.Fields("symbol"), Nested: map[string]Policy{
		"riskBrackets[]": {Keep: strings.Fields("bracketSeq bracketNotionalFloor bracketNotionalCap bracketMaintenanceMarginRate cumFastMaintenanceAmount minOpenPosLeverage maxOpenPosLeverage")},
	}},
	"exchangeInfo": {Keep: strings.Fields("symbols"), Drop: strings.Fields("timezone serverTime futuresType rateLimits exchangeFilters assets"), Nested: map[string]Policy{
		"symbols[]": {Keep: strings.Fields("symbol pair contractType status deliveryDate onboardDate baseAsset quoteAsset marginAsset liquidationFee"), Drop: strings.Fields("pricePrecision quantityPrecision baseAssetPrecision quotePrecision maintMarginPercent requiredMarginPercent underlyingType underlyingSubType settlePlan triggerProtect marketTakeBound orderTypes timeInForce permissionSets filters maxMoveOrderLimit")},
	}},
}

func init() {
	for channel, policy := range binancePolicies {
		if err := RegisterPolicy("binance", channel, policy); err != nil {
			panic(err)
		}
	}
}
