package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/poller"
)

func run(ctx context.Context, args []string, client poller.HTTPDoer) error {
	f := flag.NewFlagSet("liqwatch-poller", flag.ContinueOnError)
	path := f.String("config", "/etc/liqwatch/config.toml", "configuration")
	venue := f.String("venue", "binance", "venue")
	once := f.Bool("once", false, "single sweep")
	syms := f.String("syms", "", "comma-separated symbols")
	if err := f.Parse(args); err != nil {
		return err
	}
	b, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	cfg := struct {
		config.Config
		Poller poller.PollConfig `toml:"poller"`
	}{}
	cfg.DataDir = "/var/lib/liqwatch"
	if err = toml.Unmarshal(b, &cfg); err != nil {
		return err
	}
	if cfg.DataDir == "" {
		return errors.New("data_dir is empty")
	}
	var selected []string
	if *syms != "" {
		selected = strings.Split(*syms, ",")
		valid := regexp.MustCompile(`^[A-Z0-9_]+$`)
		for _, s := range selected {
			if !valid.MatchString(s) {
				return fmt.Errorf("invalid symbol %q", s)
			}
		}
	}
	raw := filepath.Join(cfg.DataDir, "raw")
	sink := poller.NewFileSink(raw, time.Second)
	defer sink.Close()
	bn := &poller.Binance{Doer: client, Clock: poller.RealClock{}, Config: cfg.Poller, Selected: selected}
	channels, err := poller.Venue(*venue, bn)
	if err != nil {
		return err
	}
	scheduler, err := poller.NewScheduler(channels, sink, bn.Clock, filepath.Join(raw, "poll-state.json"), cfg.Poller.WeightPerMinute)
	if err != nil {
		return err
	}
	bn.Scheduler = scheduler
	scheduler.Universe = bn.Symbols
	if *once {
		return scheduler.RunOnce(ctx)
	}
	err = scheduler.Run(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], &http.Client{Timeout: 30 * time.Second}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
