package main

import (
	"fmt"
	"os"

	"github.com/wicker/wicker/internal/checks"
	"github.com/wicker/wicker/internal/config"
)

func main() {
	path := "/etc/wicker/config.toml"
	if len(os.Args) == 3 && os.Args[1] == "-config" {
		path = os.Args[2]
	}
	c, err := config.Load(path)
	if err == nil {
		err = checks.Run(c)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
