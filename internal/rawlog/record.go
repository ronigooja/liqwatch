// Package rawlog defines the raw log record and the sink other packages write to.
// Envelope and file layout: docs/p0-core-design.md (Component 4); layout follows the trial recorder.
package rawlog

// Channel groups; one file per hour per group: raw/<YYYYMMDD>/<group>-<YYYYMMDDHH>.jsonl.zst
const (
	GroupTrade = "trade"
	GroupQuote = "quote"
	GroupDepth = "depth"
	GroupLiq   = "liq"
	GroupGap   = "gap"
)

// Record is one envelope line: {"v":1,"wseq","conn","cseq","rw","rm","ch","sym","p"}.
// Payload is embedded exactly as received. The writer assigns wseq.
type Record struct {
	Conn    string
	CSeq    uint64
	RecvWal int64 // wall clock, unix ns
	RecvMon int64 // monotonic ns
	Channel string
	Symbol  string
	Payload []byte
}

// Sink accepts records. Implementations must not block the caller beyond the bounded queue policy.
type Sink interface {
	// Write enqueues a record for the group. Droppable groups follow the drop order in the design doc.
	Write(group string, r Record)
}
