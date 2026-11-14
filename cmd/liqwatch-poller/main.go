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
	if *syms != "" {
		selected = strings.Split(*syms, ",")
		valid := regexp.MustCompile(`^[A-Z0-9_]+$`)
		for _, s := range selected {
			if !valid.MatchString(s) {
				return fmt.Errorf("invalid symbol %q", s)
			}
		}
	}
	raw := filepath.Join(base.DataDir, "raw")
	sink := poller.NewFileSink(raw, time.Second)
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
