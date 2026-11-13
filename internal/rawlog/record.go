// Package rawlog defines the raw log record and the sink other packages write to.
// Envelope and file layout: docs/storage-design.md.
package rawlog

// Channel groups retained for compatibility with existing producers.
const (
	GroupTrade = "trade"
	GroupQuote = "quote"
	GroupDepth = "depth"
	GroupLiq   = "liq"
	GroupGap   = "gap"
)

// Record is one envelope line, including venue and the original payload.
// Payload is embedded exactly as received. The writer assigns wseq.
type Record struct {
	Venue   string
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
