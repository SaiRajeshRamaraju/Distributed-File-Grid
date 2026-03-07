#include <gtest/gtest.h>
#include "../src/include/io_thread_pool.hpp"
#include <chrono>
#include <thread>
#include <vector>

TEST(IOThreadPoolTest, SubmitAndReturnValue) {
    IOThreadPool pool(2);
    auto fut = pool.submit([] { return 42; });
    
    EXPECT_EQ(fut.get(), 42);
}

TEST(IOThreadPoolTest, SubmitAndExecuteConcurrently) {
    IOThreadPool pool(4);
    auto start = std::chrono::steady_clock::now();
    
    auto fut1 = pool.submit([] { std::this_thread::sleep_for(std::chrono::milliseconds(100)); return 1; });
    auto fut2 = pool.submit([] { std::this_thread::sleep_for(std::chrono::milliseconds(100)); return 2; });
    auto fut3 = pool.submit([] { std::this_thread::sleep_for(std::chrono::milliseconds(100)); return 3; });
    auto fut4 = pool.submit([] { std::this_thread::sleep_for(std::chrono::milliseconds(100)); return 4; });
    
    EXPECT_EQ(fut1.get(), 1);
    EXPECT_EQ(fut2.get(), 2);
    EXPECT_EQ(fut3.get(), 3);
    EXPECT_EQ(fut4.get(), 4);
    
    auto end = std::chrono::steady_clock::now();
    auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    
    // With 4 threads processing 4 sleep(100ms) commands simultaneously,
    // the total duration should be around 100-200ms, definitely less than 300ms.
    EXPECT_LT(dur, 300);
}

TEST(IOThreadPoolTest, SubmitsManyJobs) {
    IOThreadPool pool(2);
    std::vector<std::future<int>> futures;
    
    for (int i = 0; i < 100; ++i) {
        futures.push_back(pool.submit([i] { return i * 2; }));
    }
    
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(futures[i].get(), i * 2);
    }
}
