#define _GNU_SOURCE
#include "harness.h"

#include <glob.h>
#include <pthread.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define MAX_TESTS 256

struct test {
	const char *name;
	test_fn fn;
	int flags;
};

static struct test tests[MAX_TESTS];
static int ntests;
static atomic_int failures;
static jmp_buf abort_jmp;
static pthread_t main_thread;
static atomic_int name_seq;

void harness_register(const char *name, test_fn fn, int flags)
{
	if (ntests == MAX_TESTS) {
		fprintf(stderr, "harness: more than %d tests\n", MAX_TESTS);
		exit(2);
	}
	tests[ntests++] = (struct test){name, fn, flags};
}

void harness_fail(const char *file, int line, const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	fprintf(stderr, "    %s:%d: ", file, line);
	vfprintf(stderr, fmt, ap);
	fputc('\n', stderr);
	va_end(ap);
	atomic_fetch_add(&failures, 1);
}

void harness_fail_cond(const char *file, int line, const char *cond, const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	fprintf(stderr, "    %s:%d: failed: %s: ", file, line, cond);
	vfprintf(stderr, fmt, ap);
	fputc('\n', stderr);
	va_end(ap);
	atomic_fetch_add(&failures, 1);
}

void harness_abort_test(void)
{
	if (!pthread_equal(pthread_self(), main_thread)) {
		fprintf(stderr, "    REQUIRE failed off the main thread; use CHECK there\n");
		abort();
	}
	longjmp(abort_jmp, 1);
}

void unique_name(char *buf, size_t len, const char *what)
{
	snprintf(buf, len, "goipc-ctest-%d-%s-%d", (int)getpid(), what, atomic_fetch_add(&name_seq, 1));
}

bool path_exists(const char *path)
{
	struct stat st;
	return lstat(path, &st) == 0;
}

int64_t now_ns(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (int64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
}

void *aligned_buffer(size_t len)
{
	void *p = NULL;
	if (posix_memalign(&p, 8, len == 0 ? 8 : len) != 0)
		return NULL;
	memset(p, 0, len);
	return p;
}

/* A failed test can leave its files behind. This removes the ones this run
 * created. */
static void remove_leftovers(void)
{
	char pattern[128];
	snprintf(pattern, sizeof pattern, "/dev/shm/go-*goipc-ctest-%d-*", (int)getpid());
	glob_t g;
	if (glob(pattern, 0, NULL, &g) != 0)
		return;
	for (size_t i = 0; i < g.gl_pathc; i++) {
		fprintf(stderr, "harness: removing leftover %s\n", g.gl_pathv[i]);
		unlink(g.gl_pathv[i]);
	}
	globfree(&g);
}

static void run_one(test_fn fn)
{
	if (setjmp(abort_jmp) == 0)
		fn();
}

int main(int argc, char **argv)
{
	int need = 0;
	const char *only = NULL;
	for (int i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--threads-only") == 0)
			need = T_THREADS;
		else
			only = argv[i];
	}

	main_thread = pthread_self();
	int ran = 0, failed = 0;
	for (int i = 0; i < ntests; i++) {
		struct test *t = &tests[i];
		if (need != 0 && ((t->flags & need) == 0 || (t->flags & T_FORK) != 0))
			continue;
		if (only != NULL && strcmp(only, t->name) != 0)
			continue;
		int before = atomic_load(&failures);
		int64_t start = now_ns();
		run_one(t->fn);
		bool ok = atomic_load(&failures) == before;
		double ms = (double)(now_ns() - start) / 1e6;
		printf("%s %s (%.1f ms)\n", ok ? "PASS" : "FAIL", t->name, ms);
		fflush(stdout);
		ran++;
		if (!ok)
			failed++;
	}
	remove_leftovers();
	printf("%d passed, %d failed\n", ran - failed, failed);
	if (ran == 0) {
		printf("no test matched\n");
		return 1;
	}
	return failed == 0 ? 0 : 1;
}
