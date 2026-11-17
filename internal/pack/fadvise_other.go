//go:build !linux

package pack

import "os"

func dropFileCache(*os.File) {}
