// Tests the generated C++ header against spec/vectors/schema. The Makefile builds fixture.h with jq and defines VECTOR_DIR.
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include "demo.hpp"

namespace {

int failures = 0;
int cases = 0;
int bad = 0;

void fail(const std::string &msg) {
	std::fprintf(stderr, "%s\n", msg.c_str());
	failures++;
}

std::vector<std::byte> bytes_of(const char *p, std::size_t n) {
	std::vector<std::byte> out(n);
	for (std::size_t i = 0; i < n; i++) {
		out[i] = static_cast<std::byte>(p[i]);
	}
	return out;
}

std::vector<std::byte> read_vector(int index) {
	std::string path = std::string(VECTOR_DIR) + "/" + std::to_string(index) + ".bin";
	std::ifstream f(path, std::ios::binary);
	if (!f) {
		std::fprintf(stderr, "cannot read %s\n", path.c_str());
		std::exit(2);
	}
	std::vector<char> raw((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
	return bytes_of(raw.data(), raw.size());
}

// run_case checks one values.json entry: encode, decode, a short buffer, a truncated input and a trailing byte.
template <class T>
void run_case(int idx, const T &v, const char *name) {
	std::string where = "entry " + std::to_string(idx) + " (" + name + "): ";
	std::vector<std::byte> want = read_vector(idx);
	if (v.size() != want.size()) {
		fail(where + "size() is " + std::to_string(v.size()) + ", want " + std::to_string(want.size()));
	}
	std::vector<std::byte> got(want.size());
	if (v.encode(got) != want.size() || got != want) {
		fail(where + "encode does not match the .bin file");
	}
	if (!want.empty() && v.encode(std::span<std::byte>(got).first(want.size() - 1)) != 0) {
		fail(where + "encode into a short buffer does not return 0");
	}
	std::optional<T> d = T::decode(want);
	if (!d) {
		fail(where + "decode rejects the .bin file");
	} else if (!(*d == v)) {
		fail(where + "decode of the .bin file does not equal the value");
	}
	if (!want.empty()) {
		std::vector<std::byte> cut(want.begin(), want.end() - 1);
		if (T::decode(cut)) {
			fail(where + "decode accepts a truncated input");
		}
	}
	std::vector<std::byte> longer = want;
	longer.push_back(std::byte{0});
	if (T::decode(longer)) {
		fail(where + "decode accepts a trailing byte");
	}
	cases++;
}

template <class T>
void run_bad(const char *name, const char *why, const char *p, std::size_t n) {
	std::vector<std::byte> in = bytes_of(p, n);
	if (T::decode(in)) {
		fail("invalid entry " + std::to_string(bad) + " (" + name + ", " + why + "): decode accepts it");
	}
	bad++;
}

#include "fixture.h"

} // namespace

#define FX_CASE(idx, T) run_case<T>(idx, fx_value_##idx(), #T);
#define FX_BAD(T, why, bytes, len) run_bad<T>(#T, why, bytes, len);

int main() {
	FX_CASES
	FX_INVALID
	static_assert(demo::Scalars::type_id == 1 && demo::Tagged::type_id == 0xFFFFFFFEu);
	static_assert(demo::Shape::fixed_size == 88 && demo::Text::fixed_size == 0);
	if (failures > 0) {
		std::fprintf(stderr, "C++: %d failures\n", failures);
		return 1;
	}
	std::printf("C++: %d values and %d invalid inputs pass\n", cases, bad);
	return 0;
}
