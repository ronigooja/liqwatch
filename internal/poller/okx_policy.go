package poller

import "strings"

func init() {
	// Ratio histories are [timestamp, ratio]; both values are retained unchanged.
	rules := map[string]Policy{
		"takerVolumeContract": {DropChannel: true},
		"instruments": {Keep: []string{"data"}, Drop: []string{"code", "msg"}, Nested: map[string]Policy{
			"data[]": {Keep: strings.Fields("instId instFamily ctVal ctMult ctValCcy ctType settleCcy lever"), Drop: strings.Fields("instType uly groupId category instCategory baseCcy quoteCcy optType stk alias seriesId listTime expTime auctionEndTime contTdSwTime preMktSwTime openType state ruleType tickSz lotSz minSz maxLmtSz maxMktSz maxLmtAmt maxMktAmt maxTwapSz maxIcebergSz maxTriggerSz maxStopSz initPxLmtPct floatPxLmtPct maxPxLmtPct rpiMinLevel rpiMinPxBand futureSettlement tradeQuoteCcyList instIdCode upcChg freq method longPosRemainingQuota shortPosRemainingQuota maxPlatOICoinLmt maxPlatOILmt posLmtAmt posLmtPct")},
		}},
		"positionTiers": {Keep: []string{"data"}, Drop: []string{"code", "msg"}, Nested: map[string]Policy{
			"data[]": {Keep: strings.Fields("instFamily tier minSz maxSz mmr imr maxLever"), Drop: strings.Fields("uly instId optMgnFactor baseMaxLoan quoteMaxLoan")},
		}},
	}
	for channel, policy := range rules {
		if err := RegisterPolicy("okx", channel, policy); err != nil {
			panic(err)
		}
	}
}
