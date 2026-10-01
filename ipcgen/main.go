// Command ipcgen turns a go-ipc message schema into encode and decode code for Go, C, C++ and Python.
package main

import (
	"fmt"
	"os"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "ipcgen:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	rootCmd.SetArgs(normalizeArgs(args))
	return rootCmd.Execute()
}
