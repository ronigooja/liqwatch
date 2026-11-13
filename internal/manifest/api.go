// Package manifest maintains the durable index of daily packs.
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

type Member struct {
	Name   string `json:"name"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}
type Pack struct {
	ID                   int64
	Venue, Symbol        string
	Day                  int
	Path                 string
	Bytes                int64
	SHA256               string
	MemberCount          int
	Members              []Member
	State                State
	RemotePath           string
	ClosedAt, VerifiedAt time.Time
}
type Store interface {
	RegisterPack(Pack) error
	KnownPack(string) (bool, error)
	ListByState(State) ([]Pack, error)
	SetState(int64, State) error
	SetSHA256(int64, string) error
	SetUploadMetadata(int64, string) error
	MarkVerified(int64, time.Time) error
	Members(int64) ([]Member, error)
	SetSetting(string, string) error
	GetSetting(string) (string, error)
	IncrSetting(string) error
	AddGap(string, int64, int64, string) error
	AddDrop(int64, string, int64) error
	Close() error
}
