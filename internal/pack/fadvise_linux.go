//go:build linux

package pack

import (
	"os"

	"golang.org/x/sys/unix"
)

// dropFileCache is best effort: cache policy must never fail publication.
func dropFileCache(f *os.File) {
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
}
