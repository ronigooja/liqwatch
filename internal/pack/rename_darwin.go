//go:build darwin

package pack

import "golang.org/x/sys/unix"

func RenameExclusive(from, to string) error { return unix.RenamexNp(from, to, unix.RENAME_EXCL) }
