package poller

func init() {
	stats := Policy{
		Keep: []string{"time", "lsr_taker", "lsr_account", "top_lsr_account", "top_lsr_size", "open_interest", "open_interest_usd", "mark_price", "long_liq_size", "short_liq_size", "long_liq_usd", "short_liq_usd"},
		Drop: []string{"long_liq_amount", "short_liq_amount", "long_liq_usd_new", "short_liq_usd_new", "top_long_size", "top_short_size", "top_long_account", "top_short_account", "long_users", "short_users", "long_taker_size", "short_taker_size", "last_funding_rate"},
	}
	tiers := Policy{
		Keep: []string{"tier", "risk_limit", "initial_rate", "maintenance_rate", "leverage_max", "deduction"},
		Drop: []string{"contract"},
	}
	contracts := Policy{
		Keep: []string{"name", "type", "quanto_multiplier", "enable_decimal", "funding_rate", "funding_next_apply", "funding_interval", "leverage_min", "leverage_max", "mark_price", "index_price", "position_size", "config_change_time"},
		Drop: []string{"maintenance_rate", "risk_limit_base", "risk_limit_step", "risk_limit_max", "funding_rate_indicative", "order_price_round", "order_size_min", "order_size_max", "order_price_deviate", "market_order_slip_ratio", "market_order_size_max", "orders_limit", "maker_fee_rate", "taker_fee_rate", "ref_discount_rate", "ref_rebate_rate", "mark_price_round", "last_price", "trade_size", "trade_id", "orderbook_id", "long_users", "short_users", "in_delisting", "status", "create_time", "launch_time", "contract_type", "enable_bonus", "enable_credit", "enable_circuit_breaker", "mark_type", "funding_cap_ratio", "interest_rate", "funding_impact_value", "funding_rate_limit", "funding_offset", "cross_leverage_default", "voucher_leverage", "is_pre_market"},
	}
	// History emits one object per row, whereas responses and fixtures are arrays.
	stats.Nested = map[string]Policy{"[]": {Keep: stats.Keep, Drop: stats.Drop}}
	stats.TimestampPath, stats.TimestampKey = "[]", "time"
	for ch, p := range map[string]Policy{
		"contractStats":  stats,
		"riskLimitTiers": {Nested: map[string]Policy{"[]": tiers}},
		"contracts":      {Nested: map[string]Policy{"[]": contracts}},
	} {
		if err := RegisterPolicy("gate", ch, p); err != nil {
			panic(err)
		}
	}
}
