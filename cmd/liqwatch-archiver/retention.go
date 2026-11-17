package main

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/manifest"
)

var archiveUsage = func(path string) (float64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	if st.Blocks == 0 {
		return 0, nil
	}
	return float64(st.Blocks-st.Bfree) * 100 / float64(st.Blocks), nil
}

func retainLocal(cfg config.Config, s *manifest.SQLiteStore) error {
	if !cfg.Archive.Retention {
		return nil
	}
	// Inspect files even below the watermark: never silently adopt unknown packs.
	newest := 0
	err := filepath.WalkDir(cfg.PacksPath(), func(path string, d fs.DirEntry, err error) error {
		if os.IsNotExist(err) && path == cfg.PacksPath() {
			return nil
		}
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := strings.TrimSuffix(strings.TrimSuffix(d.Name(), ".zst"), ".tar")
		day, e := time.Parse("20060102", name)
		if e != nil {
			return nil
		}
		n := day.Year()*10000 + int(day.Month())*100 + day.Day()
		if n > newest {
			newest = n
		}
		rel, e := filepath.Rel(cfg.PacksPath(), path)
		if e != nil {
			return e
		}
		known, e := s.KnownPack(rel)
		if e != nil {
			return e
		}
		if !known {
			log.Printf("WARNING local retention unknown pack=%s; untouched", rel)
		}
		return nil
	})
	if err != nil {
		return err
	}
	pct, err := archiveUsage(cfg.PackDir())
	if err != nil {
		return err
	}
	if pct < float64(cfg.Archive.RetentionHighPct) {
		return nil
	}
	ps, err := s.ListByState(manifest.StateLocalDeleted)
	if err != nil {
		return err
	}
	sort.Slice(ps, func(i, j int) bool {
		a, b := ps[i], ps[j]
		if a.Day != b.Day {
			return a.Day < b.Day
		}
		if a.Venue != b.Venue {
			return a.Venue < b.Venue
		}
		return a.Symbol < b.Symbol
	})
	days := 0
	for i := 0; i < len(ps) && pct > float64(cfg.Archive.RetentionLowPct); {
		end := i + 1
		for end < len(ps) && ps[end].Day == ps[i].Day {
			end++
		}
		group := ps[i:end]
		i = end
		if group[0].Day >= newest {
			continue
		}
		if days >= cfg.Archive.RetentionMaxDaysPerRun {
			break
		}
		count := 0
		var freed int64
		for _, p := range group {
			raw, err := exists(rawDay(cfg, p))
			if err != nil {
				return err
			}
			deleting, err := exists(rawDay(cfg, p) + ".deleting")
			if err != nil {
				return err
			}
			if raw || deleting {
				continue
			}
			day := fmt.Sprintf("%08d", p.Day)
			if p.Path != filepath.Join(p.Venue, p.Symbol, day+".tar") && p.Path != filepath.Join(p.Venue, p.Symbol, day+".tar.zst") {
				return fmt.Errorf("unsafe retention pack path %q", p.Path)
			}
			filename := filepath.Join(cfg.PacksPath(), p.Path)
			st, err := os.Lstat(filename)
			if err != nil && !os.IsNotExist(err) {
				return err
			}
			if err == nil {
				if !st.Mode().IsRegular() {
					return fmt.Errorf("unsafe retention file %s", filename)
				}
				if err = os.Remove(filename); err != nil {
					return err
				}
				freed += st.Size()
				if err = syncParent(filename); err != nil {
					return err
				}
			}
			// Missing file can be a crash after unlink and before state update.
			if err = s.SetState(p.ID, manifest.StatePruned); err != nil {
				return err
			}
			count++
		}
		if count == 0 {
			continue
		}
		days++
		log.Printf("local pruned day=%08d packs=%d bytes=%d", group[0].Day, count, freed)
		pct, err = archiveUsage(cfg.PackDir())
		if err != nil {
			return err
		}
	}
	if pct >= float64(cfg.Archive.RetentionHighPct) {
		log.Printf("WARNING local retention usage=%.1f%% remains above high=%d%% pruned_days=%d cap=%d", pct, cfg.Archive.RetentionHighPct, days, cfg.Archive.RetentionMaxDaysPerRun)
		return fmt.Errorf("local retention: usage remains above high watermark")
	}
	return nil
}
