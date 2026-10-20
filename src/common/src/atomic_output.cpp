#include <cerrno>
#include <cstdlib>
#include <dfg/atomic_output.hpp>
#include <dfg/unique_fd.hpp>
#include <fcntl.h>
#include <stdexcept>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace dfg {

AtomicOutput::AtomicOutput(const std::filesystem::path &destination)
    : destination_(destination) {
  std::string pattern = destination.string() + ".dfg-XXXXXX";
  std::vector<char> name(pattern.begin(), pattern.end());
  name.push_back('\0');
  UniqueFd descriptor(::mkstemp(name.data()));
  if (descriptor.get() < 0)
    throw std::system_error(errno, std::generic_category(),
                            "Create output file");
  temporary_ = name.data();
  output_.open(temporary_, std::ios::binary | std::ios::trunc);
  if (!output_) {
    std::error_code ignored;
    std::filesystem::remove(temporary_, ignored);
    throw std::runtime_error("Cannot open temporary output file");
  }
}

AtomicOutput::~AtomicOutput() {
  output_.close();
  if (!temporary_.empty()) {
    std::error_code ignored;
    std::filesystem::remove(temporary_, ignored);
  }
}

void AtomicOutput::commit() {
  output_.close();
  if (!output_)
    throw std::runtime_error("Failed to write output file");
  UniqueFd descriptor(::open(temporary_.c_str(), O_RDONLY | O_CLOEXEC));
  if (descriptor.get() < 0 || ::fsync(descriptor.get()) != 0)
    throw std::system_error(errno, std::generic_category(),
                            "Flush output file");
  std::filesystem::rename(temporary_, destination_);
  temporary_.clear();
}

} // namespace dfg
