package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/manifest"
	"github.com/ronigooja/liqwatch/internal/pack"
)

func runLocal(ctx context.Context, cfg config.Config, s *manifest.SQLiteStore, now time.Time) error {
	verified, wouldDelete, blocked, deleted, failures := 0, 0, 0, 0, 0
	limiter := &pack.ReadLimiter{Context: ctx, KBps: cfg.Archive.MaxKBps}
	recovering, err := s.ListByState(manifest.StateLocalDeleting)
	if err != nil {
		return err
	}
	for _, p := range recovering {
		if err := reconcileDeletion(cfg, s, p); err != nil {
			failures++
			log.Printf("WARNING local recovery blocked day=%s/%s/%08d reason=%v", p.Venue, p.Symbol, p.Day, err)
		}
	}
	var pending []manifest.Pack
	for _, state := range []manifest.State{manifest.StateClosed, manifest.StateLocalVerified} {
		ps, err := s.ListByState(state)
		if err != nil {
			return err
		}
		pending = append(pending, ps...)
	}
	for _, p := range pending {
		var snapshot []pack.Member
		members, err := s.Members(p.ID)
		var ms []pack.Member
		for _, m := range members {
			ms = append(ms, pack.Member{Name: m.Name, Bytes: m.Bytes, SHA256: m.SHA256, MtimeNS: m.MtimeNS})
		}
		if err == nil && len(members) != p.MemberCount {
			err = errors.New("manifest member count mismatch")
		}
		if err == nil {
			err = pack.VerifyLocal(filepath.Join(cfg.DataDir, "raw"), cfg.PacksPath(), p.Venue, p.Symbol, fmt.Sprintf("%08d", p.Day), p.Path, p.Bytes, p.SHA256, ms, time.Duration(cfg.Pack.QuietMinutes)*time.Minute, now, limiter, &snapshot)
		}
		if err != nil {
			blocked++
			if !errors.Is(err, pack.ErrBlocked) {
				failures++
			}
			log.Printf("local blocked day=%s/%s/%08d reason=%v", p.Venue, p.Symbol, p.Day, err)
			continue
		}
		if err = s.MarkLocalVerified(p.ID, now); err != nil {
			return err
		}
		verified++
		wouldDelete++
		if !cfg.Archive.DeleteSources {
			continue
		}
		if err = beginDeletion(cfg, s, p, snapshot); err != nil {
			failures++
			blocked++
			log.Printf("WARNING local deletion blocked day=%s/%s/%08d reason=%v", p.Venue, p.Symbol, p.Day, err)
			continue
		}
		deleted++
		log.Printf("local deleted day=%s/%s/%08d members=%d source_bytes=%d", p.Venue, p.Symbol, p.Day, len(ms), memberBytes(ms))
	}
	log.Printf("local verified=%d would-delete=%d blocked=%d deleted=%d", verified, wouldDelete, blocked, deleted)
	if err := retainLocal(cfg, s); err != nil {
		return err
	}
	if failures != 0 {
		return fmt.Errorf("local archive: %d integrity/I/O failures", failures)
	}
	return nil
}

func memberBytes(ms []pack.Member) int64 {
	var n int64
	for _, m := range ms {
		n += m.Bytes
	}
	return n
}
