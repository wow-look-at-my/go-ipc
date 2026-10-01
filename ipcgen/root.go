package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wow-look-at-my/go-ipc/ipcgen/internal/gen"
	"github.com/wow-look-at-my/go-ipc/ipcgen/internal/schema"
)

var rootFlags struct {
	lang string
	out  string
}

var rootCmd = &cobra.Command{
	Use:           "ipcgen --lang <" + strings.Join(gen.Languages(), "|") + "> --out <file> <schema>",
	Short:         "Generate message encode and decode code from a go-ipc schema",
	Args:          cobra.ExactArgs(1),
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		s, err := schema.Load(args[0])
		if err != nil {
			return err
		}
		src, err := gen.Generate(rootFlags.lang, s)
		if err != nil {
			return err
		}
		if err := os.WriteFile(rootFlags.out, src, 0o644); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
		return nil
	},
}

func init() {
	f := rootCmd.Flags()
	f.StringVar(&rootFlags.lang, "lang", "", "output language: "+strings.Join(gen.Languages(), ", "))
	f.StringVarP(&rootFlags.out, "out", "o", "", "output file")
	_ = rootCmd.MarkFlagRequired("lang")
	_ = rootCmd.MarkFlagRequired("out")
}
