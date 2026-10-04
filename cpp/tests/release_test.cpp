// goipc::release: a process removes its own life socket before it exits. The
// test forks, so the TSan build leaves this file out.
#include <cstdint>
#include <string>

#include <gtest/gtest.h>

#include <goipc/goipc.hpp>

#include "helpers.hpp"

namespace {

using testutil::pipe_pair;

TEST(Release, RemovesTheLifeSocketAndKeepsWatches)
{
	pipe_pair to_child, from_child;
	pid_t pid = testutil::fork_child([&] {
		// release before any procID makes none.
		goipc::release();
		if (goipc::detail::life_state().ready)
			return 2;
		from_child.send_u64(goipc::detail::self_id());
		if (!to_child.wait())
			return 3;
		goipc::release();
		goipc::release();
		from_child.signal();
		return to_child.wait() ? 0 : 4;
	});
	ASSERT_GE(pid, 0);
	std::uint64_t id = from_child.recv_u64();
	ASSERT_TRUE(goipc::detail::watchable(id)) << "the child has no life socket";
	std::string path = goipc::detail::life_path(id);

	// The watch starts before the release.
	int conn = goipc::detail::open_watch(id);
	ASSERT_GE(conn, 0);

	to_child.signal();
	ASSERT_TRUE(from_child.wait()) << "the child did not release";
	EXPECT_FALSE(testutil::file_exists(path)) << path << " is still there after release";
	EXPECT_TRUE(goipc::detail::is_dead(id)) << "a check after release must judge the process gone";
	// The child still lives, so the earlier connection stays open.
	pollfd p{conn, POLLIN, 0};
	EXPECT_EQ(::poll(&p, 1, 100), 0) << "release ended a connection that a peer held";
	int err = 0;
	EXPECT_EQ(goipc::detail::poll_watch(conn, id, err), goipc::detail::watch_state::alive);

	// The exit ends the connection.
	to_child.signal();
	EXPECT_EQ(testutil::exit_code(testutil::wait_child(pid).status), 0);
	p = {conn, POLLIN, 0};
	ASSERT_EQ(::poll(&p, 1, -1), 1);
	EXPECT_EQ(goipc::detail::poll_watch(conn, id, err), goipc::detail::watch_state::exited);
	::close(conn);
}

} // namespace
