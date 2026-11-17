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
	"sort"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/pelletier/go-toml/v2"
	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/poller"
)

func run(ctx context.Context, args []string, client poller.HTTPDoer) error {
	f := flag.NewFlagSet("liqwatch-poller", flag.ContinueOnError)
	f.Usage = func() {
		fmt.Fprintf(f.Output(), "usage: liqwatch-poller [-config path] [-venue name] [-once] [-syms A,B]\nregistered venues: %s\n", strings.Join(poller.Venues(), ", "))
		f.PrintDefaults()
	}
	path := f.String("config", "/etc/liqwatch/config.toml", "configuration")
	venue := f.String("venue", "binance", "venue: "+strings.Join(poller.Venues(), ", "))
	once := f.Bool("once", false, "single sweep")
	syms := f.String("syms", "", "comma-separated symbols")
	if err := f.Parse(args); err != nil {
		return err
	}
	b, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	base, err := config.Load(*path)
	if err != nil {
		return err
	}
	cfg := struct {
		Poller poller.PollConfig `toml:"poller"`
	}{}
	if err = toml.Unmarshal(b, &cfg); err != nil {
		return err
	}
	if base.DataDir == "" {
		return errors.New("data_dir is empty")
	}
	var selected []string
	symsSet := false
	f.Visit(func(flag *flag.Flag) {
		if flag.Name == "syms" {
			symsSet = true
		}
	})
	if symsSet {
		selected = strings.Split(*syms, ",")
		for _, s := range selected {
			if !validSymbol(s) {
				return fmt.Errorf("invalid symbol %q", s)
			}
		}
	}
	raw := filepath.Join(base.DataDir, "raw")
	sink, err := poller.NewFileSinkCompressed(raw, time.Second, base.Record.Compress, base.Record.Level)
	if err != nil {
		return err
	}
	defer sink.Close()
	if cfg.Poller.MapPath == "" {
		cfg.Poller.MapPath = base.Poller.MapPath
	}
	env, channels, err := poller.NewVenue(*venue, cfg.Poller, poller.Deps{Doer: client, Clock: poller.RealClock{}, Sink: sink, Selected: selected})
	if err != nil {
		return err
	}
	scheduler, err := poller.NewScheduler(channels, sink, env.Clock, poller.StatePath(raw, *venue), env.WeightPerMinute)
	if err != nil {
		return err
	}
	env.Bind(scheduler)
	scheduler.OnSweep = func() {
		for _, line := range unknownFieldLines(channels, poller.UnknownFields) {
			fmt.Fprintln(os.Stderr, line)
		}
	}
	if *once {
		return scheduler.RunOnce(ctx)
	}
	err = scheduler.Run(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func unknownFieldLines(channels []poller.Channel, count func(string, string) uint64) []string {
	seen := make(map[string]bool)
	var lines []string
	for _, c := range channels {
		key := c.Venue + "\x00" + c.Name
		if seen[key] {
			continue
		}
		seen[key] = true
		if n := count(c.Venue, c.Name); n > 0 {
			lines = append(lines, fmt.Sprintf("poller unknown fields venue=%s channel=%s count=%d", c.Venue, c.Name, n))
		}
	}
	sort.Strings(lines)
	return lines
}

func validSymbol(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '-' && r != '.' {
			return false
		}
	}
	return true
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], &http.Client{Timeout: 30 * time.Second}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
