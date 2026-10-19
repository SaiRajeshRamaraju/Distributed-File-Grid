#pragma once

#include <unistd.h>
#include <utility>

namespace dfg {

/// Move-only owner of a POSIX file descriptor; -1 represents no resource.
class UniqueFd {
public:
  explicit UniqueFd(int fd = -1) noexcept : fd_(fd) {}
  ~UniqueFd() { reset(); }
  UniqueFd(const UniqueFd &) = delete;
  UniqueFd &operator=(const UniqueFd &) = delete;
  UniqueFd(UniqueFd &&other) noexcept : fd_(other.release()) {}
  UniqueFd &operator=(UniqueFd &&other) noexcept {
    if (this != &other)
      reset(other.release());
    return *this;
  }

  int get() const noexcept { return fd_; }
  int release() noexcept { return std::exchange(fd_, -1); }
  void reset(int fd = -1) noexcept {
    if (fd_ >= 0)
      ::close(fd_);
    fd_ = fd;
  }

private:
  int fd_;
};

} // namespace dfg
