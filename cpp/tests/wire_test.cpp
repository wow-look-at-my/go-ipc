#include <cstring>
#include <map>
#include <string>

#include <sys/stat.h>

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
	EXPECT_EQ(spec::spec_version, 2u);
	EXPECT_EQ(wire::ring_magic, spec::ring_magic);
	EXPECT_EQ(wire::ring_version, spec::ring_version);
	EXPECT_EQ(wire::header_size, spec::header_size);
	EXPECT_EQ(wire::control_size, spec::control_size);
	EXPECT_EQ(wire::offset::slots, spec::slots_offset);
	EXPECT_EQ(wire::claim_slots, spec::slot_count);
	EXPECT_EQ(wire::slot_size, spec::slot_size);
	EXPECT_EQ(wire::no_intent, spec::no_intent);
	EXPECT_EQ(wire::consumer_none, spec::proc_none);
	EXPECT_EQ(wire::consumer_pending, spec::proc_pending);
	EXPECT_EQ(wire::proc_watchable, std::uint64_t(1) << spec::proc_watchable_bit);
	EXPECT_EQ(wire::proc_any, std::uint64_t(1) << spec::proc_always_set_bit);
	EXPECT_EQ(wire::runtime_dir, spec::runtime_dir);
	EXPECT_EQ(spec::file_mode, "0600");
	EXPECT_EQ(wire::incarnation_len, spec::instance_id_hex_digits);
	EXPECT_EQ(spec::life_socket_procid_hex_digits, 16u);
	EXPECT_EQ(spec::life_socket_temp_suffix, ".tmp");
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
	EXPECT_EQ(wire::service_registry_suffix, spec::service_registry_suffix);
	EXPECT_EQ(wire::service_client_prefix, spec::service_client_prefix);
	EXPECT_EQ(wire::incarnation_len, spec::service_client_id_hex_digits);
	EXPECT_EQ(wire::service_reserved_type_min, spec::service_reserved_type_min);
	EXPECT_EQ(wire::service_type_knock, spec::service_type_knock);
	EXPECT_EQ(wire::service_type_hello, spec::service_type_hello);
	EXPECT_EQ(wire::service_type_error, spec::service_type_error);
	EXPECT_EQ(wire::service_sequence_size, spec::service_sequence_size);
	EXPECT_EQ(wire::service_first_sequence, spec::service_first_sequence);
}

TEST(Wire, PathsMatchSpec)
{
	auto expand = [](std::string_view pattern, std::string_view key, std::string_view value,
			 std::string_view key2 = {}, std::string_view value2 = {}) {
		std::string s(pattern);
		s.replace(s.find(key), key.size(), value);
		if (!key2.empty())
			s.replace(s.find(key2), key2.size(), value2);
		return s;
	};
	namespace d = goipc::detail;
	const std::string id = "0123456789abcdef";
	const std::string inst = d::instance_name("q1", id);
	EXPECT_EQ(d::name_path("q1"), expand(spec::name_file, "{name}", "q1"));
	EXPECT_EQ(d::inc_path("q1"), expand(spec::instance_file, "{name}", "q1"));
	EXPECT_EQ(d::segment_path(inst), expand(spec::segment_path, "{name}", "q1", "{id}", id));
	EXPECT_EQ(d::event_path(inst + ".ne"), expand(spec::not_empty_event, "{name}", "q1", "{id}", id));
	EXPECT_EQ(d::event_path(inst + ".nf"), expand(spec::not_full_event, "{name}", "q1", "{id}", id));
	EXPECT_EQ(d::event_path("ev"), expand(spec::event_path, "{event}", "ev"));
	EXPECT_EQ(d::life_path(0xC000000000000abcull), expand(spec::life_socket, "{procid}", "c000000000000abc"));
	EXPECT_EQ(d::life_path(0x42), expand(spec::life_socket, "{procid}", "0000000000000042"));
}

TEST(Wire, SlotFieldsMatchSpec)
{
	const std::map<std::string, std::pair<std::size_t, std::size_t>> ours = {
		{"owner", {wire::offset::slot_owner, 8}},
		{"at", {wire::offset::slot_at, 8}},
		{"size", {wire::offset::slot_claim_size, 8}},
	};
	EXPECT_EQ(std::size(spec::slot_fields), ours.size());
	for (const auto &f : spec::slot_fields) {
		auto it = ours.find(std::string(f.name));
		ASSERT_NE(it, ours.end()) << "spec slot field " << f.name << " is unknown here";
		EXPECT_EQ(it->second.first, f.offset) << f.name;
		EXPECT_EQ(it->second.second, f.size) << f.name;
	}
}

TEST(Wire, ProcIDBits)
{
	std::uint64_t id = goipc::detail::self_id();
	EXPECT_NE(id & wire::proc_any, 0u);
	if (goipc::detail::self_error() == 0) {
		EXPECT_TRUE(goipc::detail::watchable(id));
		EXPECT_TRUE(testutil::file_exists(goipc::detail::life_path(id)));
		EXPECT_FALSE(testutil::file_exists(goipc::detail::life_path(id) + ".tmp"));
		struct stat st;
		ASSERT_EQ(::stat(goipc::detail::life_path(id).c_str(), &st), 0);
		EXPECT_TRUE(S_ISSOCK(st.st_mode));
		EXPECT_EQ(st.st_mode & 0777, 0600u);
	}
}

TEST(Wire, FieldOffsetsMatchSpec)
{
	const std::map<std::string, std::pair<std::size_t, std::size_t>> ours = {
		{"magic", {wire::offset::magic, 8}},
		{"version", {wire::offset::version, 4}},
		{"flags", {wire::offset::flags, 4}},
		{"capacity", {wire::offset::capacity, 8}},
		{"consumer", {wire::offset::consumer, 8}},
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
	testutil::aligned_buffer buf(wire::header_size + 9000);
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
	EXPECT_EQ(field("consumer"), 0u);

	// Every other byte of the cache lines is zero. The claim slots after them
	// are covered by Ring.V2Layout.
	for (std::size_t i = 0; i < wire::control_size; i++) {
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
	static_assert(static_cast<int>(errc::peer_gone) == -18);
	static_assert(static_cast<int>(errc::in_use) == -19);
	static_assert(static_cast<int>(errc::not_consumer) == -20);
	static_assert(static_cast<int>(errc::too_many_claims) == -21);
	static_assert(static_cast<int>(errc::call) == GOIPC_ECALL);
	static_assert(wire::service_reserved_type_min == GOIPC_SERVICE_RESERVED_TYPE_MIN);
	static_assert(wire::service_type_knock == GOIPC_SERVICE_TYPE_KNOCK);
	static_assert(wire::service_type_hello == GOIPC_SERVICE_TYPE_HELLO);
	static_assert(wire::service_type_error == GOIPC_SERVICE_TYPE_ERROR);
	static_assert(wire::service_sequence_size == GOIPC_SERVICE_SEQUENCE_SIZE);
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
