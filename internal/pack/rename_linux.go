//go:build linux

package pack

import "golang.org/x/sys/unix"

// RenameExclusive never replaces a newly recreated source directory.
func RenameExclusive(from, to string) error {
	return unix.Renameat2(unix.AT_FDCWD, from, unix.AT_FDCWD, to, unix.RENAME_NOREPLACE)
}
