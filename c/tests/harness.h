/* A minimal test harness. TEST registers a function at load time. REQUIRE
 * ends the test on the main thread. CHECK records a failure and continues, so
 * worker threads use it. */
#ifndef GOIPC_HARNESS_H
#define GOIPC_HARNESS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "goipc.h"

enum {
	/* The test runs threads and belongs in the ThreadSanitizer build. */
	T_THREADS = 1,
	/* The test forks, which the ThreadSanitizer build does not run. */
	T_FORK = 2,
};

typedef void (*test_fn)(void);
void harness_register(const char *name, test_fn fn, int flags);
void harness_fail(const char *file, int line, const char *fmt, ...) __attribute__((format(printf, 3, 4)));
void harness_fail_cond(const char *file, int line, const char *cond, const char *fmt, ...) __attribute__((format(printf, 4, 5)));
void harness_abort_test(void) __attribute__((noreturn));

#define TEST(name, flags) \
	static void name(void); \
	__attribute__((constructor)) static void register_##name(void) { harness_register(#name, name, flags); } \
	static void name(void)

#define CHECK(cond, ...) \
	do { \
		if (!(cond)) \
			harness_fail_cond(__FILE__, __LINE__, #cond, " " __VA_ARGS__); \
	} while (0)

#define REQUIRE(cond, ...) \
	do { \
		if (!(cond)) { \
			harness_fail_cond(__FILE__, __LINE__, #cond, " " __VA_ARGS__); \
			harness_abort_test(); \
		} \
	} while (0)

/* REQUIRE_RC compares goipc results and prints both by name. */
#define REQUIRE_RC(got, want) \
	do { \
		int got_ = (got), want_ = (want); \
		if (got_ != want_) { \
			harness_fail(__FILE__, __LINE__, "%s = %d (%s), want %d (%s)", #got, got_, goipc_strerror(got_), want_, goipc_strerror(want_)); \
			harness_abort_test(); \
		} \
	} while (0)

#define CHECK_RC(got, want) \
	do { \
		int got_ = (got), want_ = (want); \
		if (got_ != want_) \
			harness_fail(__FILE__, __LINE__, "%s = %d (%s), want %d (%s)", #got, got_, goipc_strerror(got_), want_, goipc_strerror(want_)); \
	} while (0)

/* unique_name fills buf with a name no other test or run uses. */
void unique_name(char *buf, size_t len, const char *what);
/* runtime_file fills buf with the path of a file named by fmt under
 * goipc_runtime_dir(). */
void runtime_file(char *buf, size_t len, const char *fmt, ...) __attribute__((format(printf, 3, 4)));
bool path_exists(const char *path);
int64_t now_ns(void);
/* aligned_buffer returns zeroed memory on an 8-byte boundary. */
void *aligned_buffer(size_t len);

#define MS(n) ((int64_t)(n) * 1000000)

#endif
