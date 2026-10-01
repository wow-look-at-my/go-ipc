# Turns spec/vectors/ring/manifest.json into a C++ header for vectors_test.cpp.
def str: tojson;
def hex: "\(. // "0x0")ULL";
def op: "{\(.op | str), \(.type // 0)u, \(.payload // "" | str), \(.repeat // 1), \(.limit // -1), \(.then // "" | str), \(.error // "" | str), \(.slot // -1), \(.owner | hex)}";
def rec: "{\(.type)u, \(.payload | str), \(.repeat // 1)}";
"// Generated from spec/vectors/ring/manifest.json by tests/vectors.jq. Do not edit.",
"#pragma once",
"#include <cstddef>",
"#include <cstdint>",
"#include <string_view>",
"#include <vector>",
"namespace spec {",
"struct op_spec { std::string_view op; std::uint32_t type; std::string_view payload; std::size_t repeat; std::int64_t limit; std::string_view then; std::string_view error; int slot; std::uint64_t owner; };",
"struct record_spec { std::uint32_t type; std::string_view payload; std::size_t repeat; };",
"struct case_spec { std::string_view name; std::size_t buffer_size; std::uint64_t consumer; std::vector<op_spec> ops; std::uint64_t head; std::uint64_t tail; std::vector<record_spec> records; };",
"inline const std::vector<case_spec> ring_cases = {",
(.cases[] | "\t{\(.name | str), \(.buffer_size), \(.consumer | hex), {\([.ops[] | op] | join(", "))}, \(.head), \(.tail), {\([.records[] | rec] | join(", "))}},"),
"};",
"}"
