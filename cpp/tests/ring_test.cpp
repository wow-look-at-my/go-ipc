#include <cstring>
#include <stdexcept>
#include <vector>

#include <gtest/gtest.h>

#include <goipc/goipc.hpp>

#include "helpers.hpp"

namespace {

using goipc::errc;
using goipc::Ring;
using testutil::bytes_of;
using testutil::errc_of;

std::int32_t len_at(testutil::aligned_buffer &b, std::size_t index)
{
	std::int32_t v;
	std::memcpy(&v, b.data() + goipc::wire::header_size + index, 4);
	return v;
}

std::uint32_t type_at(testutil::aligned_buffer &b, std::size_t index)
{
	std::uint32_t v;
	std::memcpy(&v, b.data() + goipc::wire::header_size + index + 4, 4);
	return v;
}

TEST(Ring, InitErrors)
{
	testutil::aligned_buffer small(4607);
	EXPECT_EQ(errc_of([&] { Ring::init(small.span()); }), errc::too_small);

	testutil::aligned_buffer big(4700);
	EXPECT_EQ(errc_of([&] { Ring::init(big.span().subspan(1)); }), errc::unaligned);
}

TEST(Ring, InitPicksLargestPowerOfTwo)
{
	testutil::aligned_buffer b(9000);
	auto r = Ring::init(b.span());
	EXPECT_EQ(r.capacity(), 8192u);
	EXPECT_EQ(Ring::size_for(8192), 8704u);
	EXPECT_TRUE(r.empty());
	EXPECT_EQ(r.buffered(), 0u);
}

TEST(Ring, AttachErrors)
{
	testutil::aligned_buffer zero(4608);
	EXPECT_EQ(errc_of([&] { Ring::attach(zero.span()); }), errc::bad_layout);

	testutil::aligned_buffer b(4608);
	Ring::init(b.span());
	auto poke32 = [&](std::size_t off, std::uint32_t v) { std::memcpy(b.data() + off, &v, 4); };
	auto poke64 = [&](std::size_t off, std::uint64_t v) { std::memcpy(b.data() + off, &v, 8); };

	poke32(goipc::wire::offset::version, 2);
	EXPECT_EQ(errc_of([&] { Ring::attach(b.span()); }), errc::bad_layout);
	poke32(goipc::wire::offset::version, 1);

	poke64(goipc::wire::offset::capacity, 6000);
	EXPECT_EQ(errc_of([&] { Ring::attach(b.span()); }), errc::bad_layout);
	poke64(goipc::wire::offset::capacity, 2048);
	EXPECT_EQ(errc_of([&] { Ring::attach(b.span()); }), errc::bad_layout);
	poke64(goipc::wire::offset::capacity, 8192);
	EXPECT_EQ(errc_of([&] { Ring::attach(b.span()); }), errc::bad_layout);
	poke64(goipc::wire::offset::capacity, 4096);
	EXPECT_NO_THROW(Ring::attach(b.span()));

	EXPECT_EQ(errc_of([&] { Ring::attach(b.span().first(4000)); }), errc::too_small);
}

TEST(Ring, MaxMessageSize)
{
	testutil::aligned_buffer b(4608);
	auto r = Ring::init(b.span());
	EXPECT_EQ(r.max_message_size(), 2040u);
	std::vector<std::byte> p(2040, std::byte{7});
	EXPECT_TRUE(r.try_write(1, p));
	p.push_back(std::byte{7});
	EXPECT_EQ(errc_of([&] { r.try_write(1, p); }), errc::too_large);
	EXPECT_EQ(errc_of([&] { r.try_claim(1, 2041); }), errc::too_large);
	EXPECT_EQ(errc_of([&] { r.try_write(goipc::wire::type_padding, {}); }), errc::reserved_type);
}

TEST(Ring, WrapInsertsPadding)
{
	testutil::aligned_buffer b(4608);
	auto r = Ring::init(b.span());
	ASSERT_TRUE(r.try_write(10, std::vector<std::byte>(2000)));
	ASSERT_EQ(r.read(1, [](std::uint32_t, std::span<const std::byte>) {}), 1u);
	ASSERT_TRUE(r.try_write(11, std::vector<std::byte>(1500)));
	ASSERT_TRUE(r.try_write(12, std::vector<std::byte>(1000, std::byte{0x33})));
	EXPECT_EQ(type_at(b, 3520), goipc::wire::type_padding);
	EXPECT_EQ(len_at(b, 3520), 576);
	EXPECT_EQ(len_at(b, 0), 1008);
	EXPECT_EQ(type_at(b, 0), 12u);
	EXPECT_EQ(r.tail(), 2008u + 1512u + 576u + 1008u);

	std::vector<std::uint32_t> types;
	r.read(SIZE_MAX, [&](std::uint32_t t, std::span<const std::byte> p) {
		types.push_back(t);
		if (t == 12) {
			EXPECT_EQ(p[999], std::byte{0x33});
		}
	});
	EXPECT_EQ(types, (std::vector<std::uint32_t>{11, 12}));
	EXPECT_TRUE(r.empty());
}

TEST(Ring, ClaimDestructorAborts)
{
	testutil::aligned_buffer b(4608);
	auto r = Ring::init(b.span());
	{
		auto c = r.try_claim(5, 16);
		ASSERT_TRUE(c.has_value());
		EXPECT_EQ(c->bytes().size(), 16u);
		EXPECT_EQ(len_at(b, 0), -24);
	}
	EXPECT_EQ(len_at(b, 0), 24);
	EXPECT_EQ(type_at(b, 0), goipc::wire::type_padding);
	EXPECT_FALSE(r.try_recv().has_value());
	EXPECT_TRUE(r.empty());
}

TEST(Ring, ClaimMoveAndCommit)
{
	testutil::aligned_buffer b(4608);
	auto r = Ring::init(b.span());
	auto c = std::move(*r.try_claim(5, 3));
	goipc::Claim moved = std::move(c);
	EXPECT_FALSE(c);
	EXPECT_TRUE(moved);
	std::memcpy(moved.bytes().data(), "abc", 3);
	moved.commit();
	EXPECT_FALSE(moved);
	EXPECT_EQ(errc_of([&] { moved.commit(); }), errc::invalid);

	auto m = r.try_recv();
	ASSERT_TRUE(m.has_value());
	EXPECT_EQ(m->type, 5u);
	EXPECT_EQ(testutil::text_of(m->payload), "abc");
}

TEST(Ring, ClaimOnFullRing)
{
	testutil::aligned_buffer b(4608);
	auto r = Ring::init(b.span());
	ASSERT_TRUE(r.try_write(1, std::vector<std::byte>(2040)));
	ASSERT_TRUE(r.try_write(1, std::vector<std::byte>(2040)));
	EXPECT_FALSE(r.try_claim(1, 0).has_value());
	EXPECT_FALSE(r.try_write(1, {}));
}

TEST(Ring, RecvIntoSmallBufferLeavesRecordQueued)
{
	testutil::aligned_buffer b(4608);
	auto r = Ring::init(b.span());
	ASSERT_TRUE(r.try_write(9, bytes_of("hello world")));

	std::vector<std::byte> small(4);
	try {
		r.try_recv(small);
		FAIL() << "no error";
	} catch (const goipc::error &e) {
		EXPECT_EQ(e.value(), errc::buffer);
		EXPECT_EQ(e.needed(), 11u);
	}
	EXPECT_FALSE(r.empty());

	std::vector<std::byte> big(64);
	auto got = r.try_recv(big);
	ASSERT_TRUE(got.has_value());
	EXPECT_EQ(got->type, 9u);
	EXPECT_EQ(testutil::text_of(std::span(big).first(got->size)), "hello world");
	EXPECT_FALSE(r.try_recv(big).has_value());
}

TEST(Ring, CorruptLength)
{
	testutil::aligned_buffer b(4608);
	auto r = Ring::init(b.span());
	ASSERT_TRUE(r.try_write(1, bytes_of("abcd")));
	auto set_len = [&](std::int32_t v) { std::memcpy(b.data() + goipc::wire::header_size, &v, 4); };

	set_len(4);
	EXPECT_EQ(errc_of([&] { r.read(1, [](std::uint32_t, std::span<const std::byte>) {}); }), errc::corrupt);
	set_len(100);
	EXPECT_EQ(errc_of([&] { r.read(1, [](std::uint32_t, std::span<const std::byte>) {}); }), errc::corrupt);
	set_len(12);
	EXPECT_EQ(r.read(1, [](std::uint32_t, std::span<const std::byte>) {}), 1u);
}

TEST(Ring, ThrowingCallbackLeavesRecordQueued)
{
	testutil::aligned_buffer b(4608);
	auto r = Ring::init(b.span());
	ASSERT_TRUE(r.try_write(1, bytes_of("a")));
	ASSERT_TRUE(r.try_write(2, bytes_of("b")));
	int calls = 0;
	EXPECT_THROW(r.read(SIZE_MAX,
			    [&](std::uint32_t t, std::span<const std::byte>) {
				    calls++;
				    if (t == 2)
					    throw std::runtime_error("stop");
			    }),
		     std::runtime_error);
	EXPECT_EQ(calls, 2);
	EXPECT_EQ(r.head(), 16u);
	auto m = r.try_recv();
	ASSERT_TRUE(m.has_value());
	EXPECT_EQ(m->type, 2u);
}

TEST(Ring, ReadLimitAndEmptyPayload)
{
	testutil::aligned_buffer b(4608);
	auto r = Ring::init(b.span());
	for (std::uint32_t i = 0; i < 5; i++)
		ASSERT_TRUE(r.try_write(i, {}));
	EXPECT_EQ(r.buffered(), 40u);
	EXPECT_EQ(r.read(0, [](std::uint32_t, std::span<const std::byte>) {}), 0u);
	std::vector<std::uint32_t> seen;
	EXPECT_EQ(r.read(3, [&](std::uint32_t t, std::span<const std::byte> p) {
		EXPECT_TRUE(p.empty());
		seen.push_back(t);
	}),
		  3u);
	EXPECT_EQ(seen, (std::vector<std::uint32_t>{0, 1, 2}));
	EXPECT_EQ(r.buffered(), 16u);
}

// A claim moves tail before it stores its header.
TEST(Ring, UnpublishedClaimOverOldPayloadReadsNothing)
{
	testutil::aligned_buffer b(4608);
	auto r = Ring::init(b.span());
	auto drain = [&] { return r.read(SIZE_MAX, [](std::uint32_t, std::span<const std::byte>) {}); };
	ASSERT_TRUE(r.try_write(1, std::vector<std::byte>(2032, std::byte{0x11})));
	ASSERT_TRUE(r.try_write(2, std::vector<std::byte>(2040, std::byte{0x22})));
	ASSERT_TRUE(r.try_write(3, {}));
	ASSERT_EQ(drain(), 3u);
	ASSERT_TRUE(r.try_write(4, std::vector<std::byte>(8, std::byte{0x44})));
	ASSERT_EQ(drain(), 1u);
	ASSERT_EQ(r.tail(), 4096u + 16u);

	std::uint64_t tail = r.tail() + 16;
	std::memcpy(b.data() + goipc::wire::offset::tail, &tail, 8);
	EXPECT_EQ(drain(), 0u);
	EXPECT_EQ(r.head(), 4096u + 16u);

	// Every byte outside the claimed span is zero.
	for (std::size_t i = 0; i < 4096; i++) {
		if (i >= 16 && i < 32)
			continue;
		ASSERT_EQ(b.data()[goipc::wire::header_size + i], std::byte{0}) << "data byte " << i;
	}
}

TEST(Ring, ManyLaps)
{
	testutil::aligned_buffer b(4608);
	auto r = Ring::init(b.span());
	std::vector<std::byte> buf(2040);
	for (std::uint32_t i = 0; i < 5000; i++) {
		std::size_t n = (i * 37) % 2041;
		std::vector<std::byte> p(n, static_cast<std::byte>(i));
		ASSERT_TRUE(r.try_write(i, p)) << i;
		auto got = r.try_recv(buf);
		ASSERT_TRUE(got.has_value());
		ASSERT_EQ(got->type, i);
		ASSERT_EQ(got->size, n);
		if (n) {
			ASSERT_EQ(buf[n - 1], static_cast<std::byte>(i));
		}
	}
	EXPECT_TRUE(r.empty());
}

} // namespace
