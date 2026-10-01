# Turns values.json and invalid.json into a C or C++ test fixture.
# Input: values.json. Arguments: $lang ("c" or "cpp"), $schema (ipcgen --lang json output) and $invalid (invalid.json), both slurped.

($schema[0]) as $S
| ($S.messages | INDEX(.name)) as $M

# The UTF-8 bytes of a string.
| def utf8: [explode[] |
	if . < 128 then .
	elif . < 2048 then (192 + (. / 64 | floor)), (128 + . % 64)
	elif . < 65536 then (224 + (. / 4096 | floor)), (128 + (. / 64 | floor) % 64), (128 + . % 64)
	else (240 + (. / 262144 | floor)), (128 + (. / 4096 | floor) % 64), (128 + (. / 64 | floor) % 64), (128 + . % 64)
	end];

# The bytes of a hex string.
def unhex: explode | map(if . >= 97 then . - 87 elif . >= 65 then . - 55 else . - 48 end)
	| [range(0; length; 2) as $i | .[$i] * 16 + .[$i + 1]];

# A string literal that holds the bytes, each as a three-digit octal escape.
def lit: "\"" + (map("\\" + ([(. / 64 | floor), ((. / 8 | floor) % 8), (. % 8)] | map(tostring) | join(""))) | join("")) + "\"";

def ctype($k):
	if $k == "bool" then "bool"
	elif $k == "f32" then "float"
	elif $k == "f64" then "double"
	else (if $lang == "cpp" then "std::" else "" end)
		+ (if $k[0:1] == "i" then "int" else "uint" end) + $k[1:] + "_t"
	end;

def cname($m): if $lang == "cpp" then $S.package + "::" + $m.name else $S.package + "_" + $m.snake end;

# Each scalar, string and bytes leaf of a value as {path, kind, value}.
def leaves($t; $v; $path):
	if $t.kind == "array" then range(0; $t.len) as $i | leaves($t.elem; $v[$i]; "\($path)[\($i)]")
	elif $t.kind == "message" then $M[$t.message].fields[] as $f | leaves($f.type; $v[$f.name]; "\($path).\($f.name)")
	else {path: $path, kind: $t.kind, value: $v}
	end;

# The literal for a scalar leaf.
def scalar:
	if .kind == "bool" then (.value | tostring)
	elif .kind == "u64" then "UINT64_C(\(.value))"
	elif .kind == "i64" then
		(if .value == "-9223372036854775808" then "(-INT64_C(9223372036854775807) - 1)" else "INT64_C(\(.value))" end)
	else "((\(ctype(.kind)))\(.value | tostring))"
	end;

# Statements that store a leaf into v.
def store:
	if .kind == "string" or .kind == "bytes" then
		(if .kind == "string" then .value | utf8 else .value | unhex end) as $b
		| if $lang == "cpp" and .kind == "string" then "\tv\(.path) = std::string(\($b | lit), \($b | length));"
		elif $lang == "cpp" then "\tv\(.path) = bytes_of(\($b | lit), \($b | length));"
		elif .kind == "string" then "\t(*v)\(.path).ptr = \($b | lit);\n\t(*v)\(.path).len = \($b | length);"
		else "\t(*v)\(.path).ptr = (const uint8_t *)\($b | lit);\n\t(*v)\(.path).len = \($b | length);"
		end
	elif $lang == "cpp" then "\tv\(.path) = \(scalar);"
	else "\t(*v)\(.path) = \(scalar);"
	end;

# A C condition that checks a decoded leaf of d.
def check:
	if .kind == "string" or .kind == "bytes" then
		(if .kind == "string" then .value | utf8 else .value | unhex end) as $b
		| "\tok &= eq_mem((*d)\(.path).ptr, (*d)\(.path).len, \($b | lit), \($b | length));"
	else "\tok &= (*d)\(.path) == \(scalar);"
	end;

[to_entries[] | .key as $i | .value as $e | $M[$e.message] as $m
	| [leaves({kind: "message", message: $e.message}; $e.value; "")] as $leaves
	| if $lang == "cpp" then
		"static \(cname($m)) fx_value_\($i)() {\n\t\(cname($m)) v{};\n"
		+ ($leaves | map(store + "\n") | join(""))
		+ "\treturn v;\n}\n"
	else
		"static void fx_build_\($i)(\(cname($m)) *v) {\n\tmemset(v, 0, sizeof *v);\n"
		+ ($leaves | map(store + "\n") | join(""))
		+ "}\n\nstatic int fx_check_\($i)(const \(cname($m)) *d) {\n\tint ok = 1;\n"
		+ ($leaves | map(check + "\n") | join(""))
		+ "\treturn ok;\n}\n"
	end
] as $funcs
| "/* Generated from values.json and invalid.json by codegen/fixture.jq. */\n\n"
+ ($funcs | join("\n"))
+ "\n#define FX_CASES \\\n"
+ (to_entries | map("\tFX_CASE(\(.key), \(cname($M[.value.message]))) \\\n") | join(""))
+ "\n#define FX_INVALID \\\n"
+ ($invalid[0] | map((.hex | unhex) as $b
	| "\tFX_BAD(\(cname($M[.message])), \"\(.error)\", \($b | lit), \($b | length)) \\\n") | join(""))
+ "\n"
