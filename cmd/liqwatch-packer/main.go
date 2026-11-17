package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/manifest"
	"github.com/ronigooja/liqwatch/internal/pack"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func main() {
	path := flag.String("config", "/etc/liqwatch/config.toml", "configuration file")
	flag.Parse()
	cfg, err := config.Load(*path)
	if err == nil {
		err = run(*cfg)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(cfg config.Config) error {
	raw := filepath.Join(cfg.DataDir, "raw")
	if err := os.MkdirAll(filepath.Dir(cfg.ManifestPath()), 0750); err != nil {
		return err
	}
	s, err := manifest.Open(cfg.ManifestPath())
	if err != nil {
		return err
	}
	defer s.Close()
	count, skipped := 0, 0
	venues, err := os.ReadDir(raw)
	if errors.Is(err, os.ErrNotExist) {
		fmt.Println("packed=0 skipped=0")
		return nil
	}
	if err != nil {
		return err
	}
	for _, v := range venues {
		if !v.IsDir() {
			continue
		}
		symbols, e := os.ReadDir(filepath.Join(raw, v.Name()))
		if e != nil {
			return e
		}
		for _, sym := range symbols {
			if !sym.IsDir() {
				continue
			}
			days, e := os.ReadDir(filepath.Join(raw, v.Name(), sym.Name()))
			if e != nil {
				return e
			}
			for _, d := range days {
				if !d.IsDir() {
					continue
				}
				if _, e = time.Parse("20060102", d.Name()); e != nil {
					continue
				}
				ext := ".tar.zst"
				if cfg.Pack.Unit == pack.ZstdThenTar {
					ext = ".tar"
				}
				rel := filepath.Join(v.Name(), sym.Name(), d.Name()+ext)
				known, e := s.KnownPack(rel)
				if e != nil {
					return e
				}
				if known {
					skipped++
					continue
				}
				r, e := pack.Pack(raw, v.Name(), sym.Name(), d.Name(), pack.Options{PackRoot: cfg.PacksPath(), Unit: cfg.Pack.Unit, Level: cfg.Pack.Level, QuietPeriod: time.Duration(cfg.Pack.QuietMinutes) * time.Minute})
				if errors.Is(e, pack.ErrIncomplete) {
					skipped++
					continue
				}
				if e != nil {
					return e
				}
				day, e := strconv.Atoi(d.Name())
				if e != nil {
					return e
				}
				p := manifest.Pack{Venue: v.Name(), Symbol: sym.Name(), Day: day, Path: r.Path, Bytes: r.Bytes, SHA256: r.SHA256, State: manifest.StateClosed, ClosedAt: time.Now().UTC()}
				for _, m := range r.Members {
					p.Members = append(p.Members, manifest.Member{Name: m.Name, Bytes: m.Bytes, SHA256: m.SHA256})
				}
				if e = s.RegisterPack(p); e != nil {
					return e
				}
				count++
			}
		}
	}
	fmt.Printf("packed=%d skipped=%d\n", count, skipped)
	return nil
}
