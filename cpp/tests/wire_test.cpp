#include <cstring>
#include <map>
#include <string>

#include <gtest/gtest.h>

#include <goipc/goipc.hpp>

#include "c/include/goipc.h"
#include "helpers.hpp"
#include "spec_wire.hpp"

namespace {

using goipc::errc;
namespace wire = goipc::wire;

TEST(Wire, ConstantsMatchSpec)
{
	EXPECT_EQ(spec::spec_version, 1u);
	EXPECT_EQ(wire::ring_magic, spec::ring_magic);
	EXPECT_EQ(wire::ring_version, spec::ring_version);
	EXPECT_EQ(wire::header_size, spec::header_size);
	EXPECT_EQ(wire::cache_line, spec::cache_line);
	EXPECT_EQ(wire::min_capacity, spec::min_capacity);
	EXPECT_EQ(wire::record_header_size, spec::record_header_size);
	EXPECT_EQ(wire::record_alignment, spec::record_alignment);
	EXPECT_EQ(wire::type_padding, spec::type_padding);
	EXPECT_EQ(wire::default_capacity, spec::default_capacity);
	EXPECT_EQ(static_cast<std::uint64_t>(wire::signal_max_tokens), spec::signal_max_tokens);
	EXPECT_EQ(wire::not_empty_suffix, spec::not_empty_suffix);
	EXPECT_EQ(wire::not_full_suffix, spec::not_full_suffix);
	EXPECT_EQ(wire::creator_to_opener_suffix, spec::creator_to_opener_suffix);
	EXPECT_EQ(wire::opener_to_creator_suffix, spec::opener_to_creator_suffix);
	EXPECT_EQ(wire::conn_type_data, spec::conn_type_data);
	EXPECT_EQ(wire::conn_type_eof, spec::conn_type_eof);
}

TEST(Wire, PathsMatchSpec)
{
	auto expand = [](std::string_view pattern, std::string_view name) {
		std::string s(pattern);
		s.replace(s.find("{name}"), 6, name);
		return s;
	};
	EXPECT_EQ(goipc::detail::segment_path("q1"), expand(spec::segment_path, "q1"));
	EXPECT_EQ(goipc::detail::event_path("q1.ne"), expand(spec::event_path, "q1.ne"));
}

TEST(Wire, FieldOffsetsMatchSpec)
{
	const std::map<std::string, std::pair<std::size_t, std::size_t>> ours = {
		{"magic", {wire::offset::magic, 8}},
		{"version", {wire::offset::version, 4}},
		{"flags", {wire::offset::flags, 4}},
		{"capacity", {wire::offset::capacity, 8}},
		{"tail", {wire::offset::tail, 8}},
		{"head", {wire::offset::head, 8}},
		{"head_cache", {wire::offset::head_cache, 8}},
		{"recv_waiters", {wire::offset::recv_waiters, 4}},
		{"send_waiters", {wire::offset::send_waiters, 4}},
	};
	EXPECT_EQ(std::size(spec::ring_fields), ours.size());
	for (const auto &f : spec::ring_fields) {
		auto it = ours.find(std::string(f.name));
		ASSERT_NE(it, ours.end()) << "spec field " << f.name << " is unknown here";
		EXPECT_EQ(it->second.first, f.offset) << f.name;
		EXPECT_EQ(it->second.second, f.size) << f.name;
	}
}

// The control block an init writes must hold each field at its spec offset.
TEST(Wire, InitWritesHeaderAtSpecOffsets)
{
	testutil::aligned_buffer buf(9000);
	std::memset(buf.data(), 0xAB, buf.size());
	auto ring = goipc::Ring::init(buf.span());
	ASSERT_TRUE(ring.try_write(3, testutil::bytes_of("xyz")));

	auto field = [&](std::string_view name) {
		for (const auto &f : spec::ring_fields) {
			if (f.name == name) {
				std::uint64_t v = 0;
				std::memcpy(&v, buf.data() + f.offset, f.size);
				return v;
			}
		}
		ADD_FAILURE() << "no field " << name;
		return std::uint64_t(0);
	};
	EXPECT_EQ(field("magic"), spec::ring_magic);
	EXPECT_EQ(field("version"), spec::ring_version);
	EXPECT_EQ(field("flags"), 0u);
	EXPECT_EQ(field("capacity"), 8192u);
	EXPECT_EQ(field("tail"), 16u);
	EXPECT_EQ(field("head"), 0u);
	EXPECT_EQ(field("head_cache"), 0u);
	EXPECT_EQ(field("recv_waiters"), 0u);
	EXPECT_EQ(field("send_waiters"), 0u);

	// Every other byte of the control block is zero.
	for (std::size_t i = 0; i < spec::header_size; i++) {
		bool in_field = false;
		for (const auto &f : spec::ring_fields)
			in_field = in_field || (i >= f.offset && i < f.offset + f.size);
		if (!in_field) {
			ASSERT_EQ(buf.data()[i], std::byte{0}) << "byte " << i;
		}
	}
}

TEST(Wire, ErrorNumbersMatchCHeader)
{
	static_assert(static_cast<int>(errc::closed) == GOIPC_ECLOSED);
	static_assert(static_cast<int>(errc::full) == GOIPC_EFULL);
	static_assert(static_cast<int>(errc::empty) == GOIPC_EEMPTY);
	static_assert(static_cast<int>(errc::too_large) == GOIPC_ETOOLARGE);
	static_assert(static_cast<int>(errc::reserved_type) == GOIPC_ERESERVED);
	static_assert(static_cast<int>(errc::invalid_name) == GOIPC_EINVALNAME);
	static_assert(static_cast<int>(errc::invalid_capacity) == GOIPC_EINVALCAP);
	static_assert(static_cast<int>(errc::too_small) == GOIPC_ETOOSMALL);
	static_assert(static_cast<int>(errc::bad_layout) == GOIPC_EBADLAYOUT);
	static_assert(static_cast<int>(errc::corrupt) == GOIPC_ECORRUPT);
	static_assert(static_cast<int>(errc::unaligned) == GOIPC_EUNALIGNED);
	static_assert(static_cast<int>(errc::timed_out) == GOIPC_ETIMEDOUT);
	static_assert(static_cast<int>(errc::sys) == GOIPC_ESYS);
	static_assert(static_cast<int>(errc::no_memory) == GOIPC_ENOMEM);
	static_assert(static_cast<int>(errc::invalid) == GOIPC_EINVAL);
	static_assert(static_cast<int>(errc::buffer) == GOIPC_EBUFFER);
	static_assert(static_cast<int>(errc::eof) == GOIPC_EOF);
	static_assert(wire::ring_magic == GOIPC_RING_MAGIC);
	static_assert(wire::type_padding == GOIPC_TYPE_PADDING);
	static_assert(wire::default_capacity == GOIPC_DEFAULT_CAPACITY);
	SUCCEED();
}

TEST(Wire, ErrorCategory)
{
	std::error_code ec = errc::full;
	EXPECT_EQ(ec.category(), goipc::category());
	EXPECT_EQ(ec.value(), -2);
	EXPECT_EQ(goipc::to_errc(ec), errc::full);
	EXPECT_EQ(goipc::to_errc(std::error_code(ENOENT, std::system_category())), errc::sys);
	EXPECT_FALSE(ec.message().empty());

	goipc::error e(std::error_code(EACCES, std::system_category()), "x");
	EXPECT_EQ(e.value(), errc::sys);
	EXPECT_EQ(e.code().value(), EACCES);
}

TEST(Wire, Names)
{
	for (const char *bad : {"", ".", "..", "a/b", "a\\b", "/x"})
		EXPECT_EQ(testutil::errc_of([&] { goipc::detail::validate_name(bad); }), errc::invalid_name) << bad;
	goipc::detail::validate_name("ok.name-1");
}

} // namespace
