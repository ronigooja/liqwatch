package rawlog

import (
	"io"

	"github.com/klauspost/compress/zstd"
)

// NewFrameEncoder creates bounded compression state shared by a serialized writer.
// Callers must serialize all FrameWriter writes using this encoder.
func NewFrameEncoder(level int) (*zstd.Encoder, error) {
	if level == 0 {
		level = 1
	}
	return zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)),
		zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(64<<10), zstd.WithLowerEncoderMem(true))
}

// FrameWriter finishes an independent frame on every Write. Put a bufio.Writer
// above it: flushing that buffer publishes complete frames, including on close.
// Reopening in append mode naturally starts another concatenated frame.
type FrameWriter struct {
	Encoder *zstd.Encoder
	Dst     io.Writer
}

func (w FrameWriter) Write(p []byte) (int, error) {
	w.Encoder.Reset(w.Dst)
	n, err := w.Encoder.Write(p)
	if err != nil {
		return n, err
	}
	return n, w.Encoder.Close()
}
