#include <spawn.h>
#include <string>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "helpers.hpp"

extern char **environ;

namespace {

struct peer {
	pid_t pid;
	int out;
};

peer spawn_peer(std::vector<std::string> args)
{
	args.insert(args.begin(), GOIPC_PEER_BIN);
	std::vector<char *> argv;
	for (auto &a : args)
		argv.push_back(a.data());
	argv.push_back(nullptr);

	int fds[2];
	if (::pipe2(fds, O_CLOEXEC) != 0)
		throw std::runtime_error("pipe2 failed");
	posix_spawn_file_actions_t fa;
	posix_spawn_file_actions_init(&fa);
	posix_spawn_file_actions_adddup2(&fa, fds[1], STDOUT_FILENO);
	pid_t pid;
	int err = ::posix_spawn(&pid, argv[0], &fa, nullptr, argv.data(), environ);
	posix_spawn_file_actions_destroy(&fa);
	::close(fds[1]);
	if (err != 0) {
		::close(fds[0]);
		throw std::runtime_error("posix_spawn " + args[0] + ": " + std::to_string(err));
	}
	return {pid, fds[0]};
}

// read_line reads one line, or what is left when the peer closes stdout.
std::string read_line(int fd)
{
	std::string line;
	char c;
	while (::read(fd, &c, 1) == 1) {
		if (c == '\n')
			break;
		line += c;
	}
	return line;
}

int finish(peer &p)
{
	::close(p.out);
	return testutil::exit_code(testutil::wait_child(p.pid).status);
}

TEST(Peer, RecvAndSenders)
{
	auto name = testutil::unique_name("peer");
	testutil::queue_files f{name};
	auto recv = spawn_peer({"recv", name, "6000", "4096"});
	ASSERT_EQ(read_line(recv.out), "ready");
	std::vector<peer> senders;
	for (const char *id : {"a", "b", "c"})
		senders.push_back(spawn_peer({"send", name, id, "2000"}));
	for (auto &s : senders)
		EXPECT_EQ(finish(s), 0);
	EXPECT_EQ(read_line(recv.out), "ok 6000");
	EXPECT_EQ(finish(recv), 0);
	EXPECT_FALSE(testutil::file_exists("/dev/shm/go-ipc-" + name + ".name"));
}

TEST(Peer, ListenEchoAndDialCheck)
{
	auto name = testutil::unique_name("peer");
	testutil::channel_files f{name};
	auto listen = spawn_peer({"listen-echo", name, "4096"});
	ASSERT_EQ(read_line(listen.out), "ready");
	auto dial = spawn_peer({"dial-check", name, "300000"});
	EXPECT_EQ(finish(dial), 0);
	EXPECT_EQ(finish(listen), 0);
	EXPECT_FALSE(testutil::file_exists("/dev/shm/go-ipc-" + name + ".c2o.name"));
}

TEST(Peer, TypedSendAndRecv)
{
	auto name = testutil::unique_name("peer");
	testutil::queue_files f{name};
	auto recv = spawn_peer({"typed-recv", name, "4096"});
	ASSERT_EQ(read_line(recv.out), "ready");
	auto send = spawn_peer({"typed-send", name});
	EXPECT_EQ(finish(send), 0);
	std::string done = read_line(recv.out);
	EXPECT_TRUE(done.starts_with("ok ") && std::stoi(done.substr(3)) > 0) << done;
	EXPECT_EQ(finish(recv), 0);
	EXPECT_FALSE(testutil::file_exists("/dev/shm/go-ipc-" + name + ".name"));
}

// typed-recv must reject a record whose type is not the next entry's type ID.
TEST(Peer, TypedRecvRejectsAWrongType)
{
	auto name = testutil::unique_name("peer");
	testutil::queue_files f{name};
	auto recv = spawn_peer({"typed-recv", name, "4096"});
	ASSERT_EQ(read_line(recv.out), "ready");
	auto send = spawn_peer({"send", name, "a", "1"});
	EXPECT_EQ(finish(send), 0);
	EXPECT_EQ(finish(recv), 1);
	EXPECT_FALSE(testutil::file_exists("/dev/shm/go-ipc-" + name + ".name"));
}

TEST(Peer, BadArgumentsFail)
{
	auto p = spawn_peer({"recv", "x"});
	EXPECT_EQ(finish(p), 1);
	auto q = spawn_peer({"nope"});
	EXPECT_EQ(finish(q), 1);
}

} // namespace
