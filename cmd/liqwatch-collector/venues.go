package main

// New venue packages register by import; Binance is imported in main.go for configuration.
import (
	_ "github.com/ronigooja/liqwatch/internal/venue/gate"
	_ "github.com/ronigooja/liqwatch/internal/venue/mexc"
	_ "github.com/ronigooja/liqwatch/internal/venue/okx"
)
