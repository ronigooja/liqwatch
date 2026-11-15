package poller

func init() {
	if err := RegisterPolicy("mexc", "contractDetail", Policy{
		Keep:   []string{"contractSize", "minLeverage", "maxLeverage", "countryConfigContractMaxLeverage", "maintenanceMarginRate", "initialMarginRate", "riskBaseVol", "riskIncrVol", "riskIncrMmr", "riskIncrImr", "riskLevelLimit", "riskLongShortSwitch", "riskLimitType", "riskLimitMode", "riskLimitCustom"},
		Drop:   []string{"symbol", "displayName", "displayNameEn", "fn", "baseCoin", "quoteCoin", "baseCoinName", "quoteCoinName", "settleCoin", "positionOpenType", "futureType", "priceScale", "volScale", "amountScale", "priceUnit", "volUnit", "minVol", "maxVol", "limitMaxVol", "maxNumOrders", "bidLimitPriceRate", "askLimitPriceRate", "marketOrderMaxLevel", "marketOrderPriceLimitRate1", "marketOrderPriceLimitRate2", "triggerProtect", "threshold", "takerFeeRate", "makerFeeRate", "liquidationFeeRate", "feeRateMode", "feeRateType", "leverageFeeRates", "tieredFeeRates", "priceCoefficientVariation", "indexOrigin", "state", "automaticDelivery", "type", "typeLabel", "preMarket", "stopOnlyFair", "apiAllowed", "isNew", "isHot", "isHidden", "isMaxLeverage", "isZeroFeeRate", "isZeroFeeSymbol", "showBeforeOpen", "openingCountdownOption", "appraisal", "showAppraisalCountdown", "conceptPlate", "conceptPlateId", "tagIdList", "depthStepList", "baseCoinIconUrl", "baseCoinId", "vid", "id", "createTime", "openingTime", "tempMaxLeverageLimited", "regularMaxLeverage", "tempFeeRateEffectiveStartTime"},
		Nested: map[string]Policy{"riskLimitCustom[]": {Keep: []string{"level", "maxVol", "mmr", "imr", "maxLeverage"}}},
	}); err != nil {
		panic(err)
	}
}
