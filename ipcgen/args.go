package main

import "strings"

// longFlags are the flags that may also be spelled with one dash, as the Go flag package allows.
var longFlags = map[string]bool{"lang": true, "out": true}

// normalizeArgs rewrites -lang and -out to --lang and --out. Arguments after "--" stay as they are.
func normalizeArgs(args []string) []string {
	out := make([]string, len(args))
	copy(out, args)
	for i, a := range out {
		if a == "--" {
			break
		}
		if strings.HasPrefix(a, "-") && !strings.HasPrefix(a, "--") {
			name, _, _ := strings.Cut(a[1:], "=")
			if longFlags[name] {
				out[i] = "-" + a
			}
		}
	}
	return out
}
