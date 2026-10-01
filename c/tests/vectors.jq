# Turns spec/vectors/ring/manifest.json into one line per item for
# test_spec.c. An empty payload or an absent word becomes "-".
def hex: if (. // "") == "" then "-" else . end;
.cases[] |
	"case \(.name) \(.buffer_size) \(.head) \(.tail) \(.consumer // "0x0")",
	(.ops[] | "op \(.op) \(.type // 0) \(.payload | hex) \(.repeat // 1) \(.then // "-") \(.limit // 0) \(.error // "-") \(.slot // -1) \(.owner // "0x0")"),
	(.records[] | "rec \(.type) \(.payload | hex) \(.repeat // 1)"),
	"end"
