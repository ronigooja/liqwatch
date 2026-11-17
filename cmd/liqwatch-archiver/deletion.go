package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ronigooja/liqwatch/internal/config"
	"github.com/ronigooja/liqwatch/internal/manifest"
	"github.com/ronigooja/liqwatch/internal/pack"
)

// Tests inject process interruptions; production has no hook side effects.
var deletionPoint = func(string) error { return nil }
var removeDeletion = os.RemoveAll

func rawDay(cfg config.Config, p manifest.Pack) string {
	return filepath.Join(cfg.DataDir, "raw", p.Venue, p.Symbol, fmt.Sprintf("%08d", p.Day))
}

func exists(path string) (bool, error) {
	_, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}

func syncParent(path string) error {
	f, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func beginDeletion(cfg config.Config, s *manifest.SQLiteStore, p manifest.Pack, ms []pack.Member) error {
	var evidence []manifest.Member
	for _, m := range ms {
		evidence = append(evidence, manifest.Member{Name: m.Name, Bytes: m.Bytes, MtimeNS: m.MtimeNS})
	}
	if err := s.BeginLocalDeletion(p.ID, evidence); err != nil {
		return err
	}
	if err := deletionPoint("state"); err != nil {
		return err
	}
	dir := rawDay(cfg, p)
	if err := pack.RenameExclusive(dir, dir+".deleting"); err != nil {
		return err
	}
	if err := syncParent(dir); err != nil {
		return err
	}
	if err := deletionPoint("rename"); err != nil {
		return err
	}
	return finishDeletion(cfg, s, p, false)
}

func finishDeletion(cfg config.Config, s *manifest.SQLiteStore, p manifest.Pack, recovering bool) error {
	dir := rawDay(cfg, p)
	renamed := dir + ".deleting"
	evidence, err := s.DeletionMembers(p.ID)
	if err != nil {
		return err
	}
	var ms []pack.Member
	for _, m := range evidence {
		ms = append(ms, pack.Member{Name: m.Name, Bytes: m.Bytes, MtimeNS: m.MtimeNS})
	}
	removing, err := s.GetSetting(fmt.Sprintf("local-removing:%d", p.ID))
	if err != nil {
		return err
	}
	// Missing members are permitted only after a successful complete recheck was
	// durably recorded; this distinguishes a pre-removal disappearance from a crash.
	if err = pack.RecheckDeletion(renamed, ms, recovering && removing == "true"); err != nil {
		original, e := exists(dir)
		if e != nil {
			return e
		}
		if original {
			return fmt.Errorf("BLOCKED changed deletion; original exists, retained %s: %w", renamed, err)
		}
		if e = pack.RenameExclusive(renamed, dir); e != nil {
			return fmt.Errorf("BLOCKED restore collision, retained %s: %w", renamed, e)
		}
		if e = syncParent(dir); e != nil {
			return e
		}
		if e = s.SetState(p.ID, manifest.StateLocalVerified); e != nil {
			return e
		}
		return fmt.Errorf("BLOCKED changed deletion; restored %s: %w", dir, err)
	}
	if err = s.SetSetting(fmt.Sprintf("local-removing:%d", p.ID), "true"); err != nil {
		return err
	}
	if err = deletionPoint("recheck"); err != nil {
		return err
	}
	if err = removeDeletion(renamed); err != nil {
		return err
	}
	if err = syncParent(dir); err != nil {
		return err
	}
	if err = deletionPoint("removed"); err != nil {
		return err
	}
	return s.SetState(p.ID, manifest.StateLocalDeleted)
}

func reconcileDeletion(cfg config.Config, s *manifest.SQLiteStore, p manifest.Pack) error {
	dir := rawDay(cfg, p)
	renamed, err := exists(dir + ".deleting")
	if err != nil {
		return err
	}
	if renamed {
		return finishDeletion(cfg, s, p, true)
	}
	original, err := exists(dir)
	if err != nil {
		return err
	}
	if original {
		return s.SetState(p.ID, manifest.StateLocalVerified)
	}
	return s.SetState(p.ID, manifest.StateLocalDeleted)
}
