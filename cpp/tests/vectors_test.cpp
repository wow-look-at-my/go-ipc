#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iterator>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <goipc/goipc.hpp>

#include "helpers.hpp"
#include "spec_vectors.hpp"

namespace {

using goipc::errc;

std::string spec_dir()
{
	if (const char *d = std::getenv("GOIPC_SPEC_DIR"); d && *d)
		return d;
	return GOIPC_SPEC_DIR;
}

std::vector<std::byte> read_file(const std::string &path)
{
	std::ifstream in(path, std::ios::binary);
	if (!in)
		throw std::runtime_error("cannot open " + path);
	std::vector<char> raw((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
	std::vector<std::byte> out(raw.size());
	std::memcpy(out.data(), raw.data(), raw.size());
	return out;
}

std::vector<std::byte> payload(std::string_view hex, std::size_t repeat)
{
	std::vector<std::byte> one;
	for (std::size_t i = 0; i + 1 < hex.size(); i += 2)
		one.push_back(static_cast<std::byte>(std::stoi(std::string(hex.substr(i, 2)), nullptr, 16)));
	std::vector<std::byte> out;
	for (std::size_t r = 0; r < repeat; r++)
		out.insert(out.end(), one.begin(), one.end());
	return out;
}

errc expected_error(std::string_view name)
{
	if (name == "full")
		return errc::full;
	if (name == "too_large")
		return errc::too_large;
	if (name == "reserved_type")
		return errc::reserved_type;
	ADD_FAILURE() << "unknown manifest error " << name;
	return errc::invalid;
}

// write_error runs a write that the manifest expects to fail, and returns the
// error it reports. A full ring is not an exception in this API.
errc write_error(goipc::Ring &ring, int slot, std::uint32_t type, std::span<const std::byte> p)
{
	try {
		bool ok = slot < 0 ? ring.try_write(type, p) : ring.try_write(slot, type, p);
		if (!ok)
			return errc::full;
	} catch (const goipc::error &e) {
		return e.value();
	}
	ADD_FAILURE() << "write succeeded";
	return errc::invalid;
}

std::size_t first_difference(std::span<const std::byte> a, std::span<const std::byte> b)
{
	std::size_t n = std::min(a.size(), b.size());
	for (std::size_t i = 0; i < n; i++)
		if (a[i] != b[i])
			return i;
	return n;
}

TEST(Vectors, ReplayMatchesImage)
{
	ASSERT_FALSE(spec::ring_cases.empty());
	for (const auto &c : spec::ring_cases) {
		SCOPED_TRACE(std::string(c.name));
		testutil::aligned_buffer buf(c.buffer_size);
		auto ring = goipc::Ring::init(buf.span(), c.consumer);
		// Claims left open must stay open until the image is compared.
		std::vector<goipc::Claim> open_claims;

		for (const auto &op : c.ops) {
			auto p = payload(op.payload, op.repeat);
			if (!op.error.empty()) {
				std::vector<std::byte> before(buf.data(), buf.data() + buf.size());
				EXPECT_EQ(write_error(ring, op.slot, op.type, p), expected_error(op.error));
				EXPECT_EQ(0, std::memcmp(before.data(), buf.data(), buf.size())) << "a failed op changed the ring";
			} else if (op.op == "write") {
				ASSERT_TRUE(op.slot < 0 ? ring.try_write(op.type, p) : ring.try_write(op.slot, op.type, p));
			} else if (op.op == "acquire") {
				int got = ring.acquire_slot(op.owner, [](std::uint64_t) { return false; });
				ASSERT_EQ(got, op.slot);
			} else if (op.op == "drop") {
				ring.drop_slot(op.owner, op.slot);
			} else if (op.op == "claim") {
				auto claim = op.slot < 0 ? ring.try_claim(op.type, p.size()) : ring.try_claim(op.slot, op.type, p.size());
				ASSERT_TRUE(claim.has_value());
				std::memcpy(claim->bytes().data(), p.data(), p.size());
				if (op.then == "commit")
					claim->commit();
				else if (op.then == "abort")
					claim->abort();
				else if (op.then == "none")
					open_claims.push_back(std::move(*claim));
				else
					FAIL() << "unknown claim outcome " << op.then;
			} else if (op.op == "read") {
				std::size_t limit = op.limit < 0 ? SIZE_MAX : static_cast<std::size_t>(op.limit);
				ring.read(limit, [](std::uint32_t, std::span<const std::byte>) {});
			} else {
				FAIL() << "unknown op " << op.op;
			}
		}

		auto want = read_file(spec_dir() + "/vectors/ring/" + std::string(c.name) + ".bin");
		ASSERT_EQ(want.size(), c.buffer_size);
		std::span<const std::byte> got(buf.data(), buf.size());
		EXPECT_EQ(first_difference(got, want), buf.size()) << "image differs at the byte shown";
	}
}

TEST(Vectors, AttachReadsExpectedRecords)
{
	for (const auto &c : spec::ring_cases) {
		SCOPED_TRACE(std::string(c.name));
		auto image = read_file(spec_dir() + "/vectors/ring/" + std::string(c.name) + ".bin");
		testutil::aligned_buffer buf(image.size());
		std::memcpy(buf.data(), image.data(), image.size());

		auto ring = goipc::Ring::attach(buf.span());
		EXPECT_EQ(0, std::memcmp(buf.data(), image.data(), image.size())) << "attach wrote to the buffer";
		EXPECT_EQ(ring.consumer(), c.consumer);
		EXPECT_EQ(ring.head(), c.head);
		EXPECT_EQ(ring.tail(), c.tail);

		std::vector<goipc::message> got;
		ring.read(SIZE_MAX, [&](std::uint32_t t, std::span<const std::byte> p) {
			got.push_back({t, std::vector<std::byte>(p.begin(), p.end())});
		});
		ASSERT_EQ(got.size(), c.records.size());
		for (std::size_t i = 0; i < got.size(); i++) {
			EXPECT_EQ(got[i].type, c.records[i].type) << "record " << i;
			EXPECT_EQ(got[i].payload, payload(c.records[i].payload, c.records[i].repeat)) << "record " << i;
		}
	}
}

} // namespace
