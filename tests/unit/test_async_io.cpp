#include "../../src/common/include/dfg/thread_pool.hpp"
#include <chrono>
#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include <stdexcept>

TEST(ThreadPoolTest, SubmitAndReturnValue) {
  dfg::ThreadPool pool(2);
  auto fut = pool.submit([] { return 42; });

  EXPECT_EQ(fut.get(), 42);
}

TEST(ThreadPoolTest, SubmitAndExecuteConcurrently) {
  dfg::ThreadPool pool(4);
  std::promise<void> release;
  auto gate = release.get_future().share();
  std::atomic<int> started{0};
  std::vector<std::future<int>> futures;
  for (int i = 0; i < 4; ++i) {
    futures.push_back(pool.submit([&, i] {
      ++started;
      gate.wait();
      return i;
    }));
  }
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (started < 4 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  EXPECT_EQ(started.load(), 4);
  // Always release workers, even on failure, so teardown cannot deadlock.
  release.set_value();
  for (int i = 0; i < 4; ++i) EXPECT_EQ(futures[i].get(), i);
}

TEST(ThreadPoolTest, PropagatesExceptionsAndKeepsWorkerAlive) {
  dfg::ThreadPool pool(1);
  auto failure = pool.submit([]() -> int { throw std::runtime_error("failed job"); });
  EXPECT_THROW(failure.get(), std::runtime_error);
  EXPECT_EQ(pool.submit([] { return 42; }).get(), 42);
}

TEST(ThreadPoolTest, DestructorDrainsQueuedJobs) {
  std::atomic<int> completed{0};
  {
    dfg::ThreadPool pool(2);
    for (int i = 0; i < 100; ++i) pool.submit([&] { ++completed; });
  }
  EXPECT_EQ(completed.load(), 100);
}

TEST(ThreadPoolTest, SupportsMoveOnlyTasks) {
  dfg::ThreadPool pool(1);
  auto result = pool.submit([value = std::make_unique<int>(17)] { return *value; });
  EXPECT_EQ(result.get(), 17);
}

TEST(ThreadPoolTest, SubmitsManyJobs) {
  dfg::ThreadPool pool(2);
  std::vector<std::future<int>> futures;

  for (int i = 0; i < 100; ++i) {
    futures.push_back(pool.submit([i] { return i * 2; }));
  }

  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(futures[i].get(), i * 2);
  }
}
