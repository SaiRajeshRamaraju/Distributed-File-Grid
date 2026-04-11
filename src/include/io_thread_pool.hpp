#pragma once

#include "heart_beat_signal.hpp"
#include <vector>
#include <thread>
#include <mutex>
#include <future>
#include <queue>
#include <condition_variable>
#include <functional>
#include <chrono>
#include <memory>

// ─────────────────────── I/O Thread Pool ───────────────────────
// Offloads blocking file system operations so the epoll reactor is never stalled.
class IOThreadPool {
public:
    explicit IOThreadPool(size_t num_threads = 4) : stop_(false) {
        for (size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this] { worker_loop(); });
        }
    }

    ~IOThreadPool() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_ = true;
        }
        cv_.notify_all();
        for (auto& t : workers_) {
            if (t.joinable()) t.join();
        }
    }

    // Submit a callable; returns a future for the result.
    template <typename F>
    auto submit(F&& fn) -> std::future<decltype(fn())> {
        using Ret = decltype(fn());
        auto task = std::make_shared<std::packaged_task<Ret()>>(std::forward<F>(fn));
        auto fut = task->get_future();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            jobs_.push([task]() { (*task)(); });
        }
        cv_.notify_one();
        return fut;
    }

private:
    void worker_loop() {
        while (true) {
            std::function<void()> job;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this] { return stop_ || !jobs_.empty(); });
                if (stop_ && jobs_.empty()) return;
                job = std::move(jobs_.front());
                jobs_.pop();
            }
            job();
        }
    }

    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> jobs_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stop_;
};

// Global I/O thread pool (shared by all storage operations)
inline IOThreadPool& io_pool() {
    static IOThreadPool pool(4);
    return pool;
}

// ─────────────────── Awaiter: bridge future → coroutine ───────────────────
// Suspends the coroutine, polls the future on reactor ticks via a timerfd,
// and resumes once the result is ready.
template <typename T>
struct FutureAwaiter {
    std::shared_future<T> fut;
    async_hb::Reactor* reactor;

    bool await_ready() const noexcept {
        return fut.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
    }

    void await_suspend(std::coroutine_handle<> h) {
        auto r = reactor;
        auto shared_h = std::make_shared<std::coroutine_handle<>>(h);
        
        auto poll = [r, f = fut, shared_h](async_hb::Reactor& rx) -> async_hb::task {
            while (f.wait_for(std::chrono::microseconds(0)) != std::future_status::ready) {
                co_await rx.sleep_for(std::chrono::milliseconds(1));
            }
            if (*shared_h && !shared_h->done()) {
                shared_h->resume();
            }
            co_return;
        };

        reactor->spawn(poll(*reactor));
    }

    T await_resume() {
        return fut.get();
    }
};

template <typename T>
FutureAwaiter<T> await_future(async_hb::Reactor& r, std::future<T> f) {
    return FutureAwaiter<T>{f.share(), &r};
}
