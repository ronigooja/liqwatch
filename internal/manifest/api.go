// Package manifest is the SQLite index of raw files (docs/p0-support-design.md, Component 7).
// Pure-Go driver (modernc.org/sqlite), WAL mode. Used by the collector (register closed files,
// gaps, drops) and the archiver (state transitions).
package manifest

import "time"

type State string

const (
	StateOpen      State = "open"
	StateClosed    State = "closed"
	StateUploading State = "uploading"
	StateUploaded  State = "uploaded"
	StateVerified  State = "verified"
	StateDeleted   State = "deleted"
)

type File struct {
	ID         int64
	Path       string // relative to raw dir, e.g. 20261103/trade-2026110312.jsonl.zst
	Group      string
	HourStart  time.Time
	MsgCount   int64
	Bytes      int64
	SHA256     string
	ClosedAt   time.Time
	State      State
	RemotePath string
	ChunkCount int
	VerifiedAt time.Time // zero until verified
}

// Store is the manifest API. Rules enforced inside the implementation:
//   - transitions only move forward: closed -> uploading -> uploaded -> verified -> deleted;
//   - MarkDeleted fails unless State is verified and VerifiedAt is at least the given age ago.
type Store interface {
	RegisterClosed(f File) error
	ListByState(s State) ([]File, error)
	SetState(id int64, s State) error
	MarkVerified(id int64, remotePath string, chunks int, at time.Time) error
	DeletableFiles(minAge time.Duration, now time.Time) ([]File, error)
	MarkDeleted(id int64, minAge time.Duration, now time.Time) error
	AddGap(stream string, startNs, endNs int64, cause string) error
	AddDrop(windowStart int64, channel string, count int64) error
	Close() error
}
