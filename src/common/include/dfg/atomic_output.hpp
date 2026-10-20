#pragma once

#include <filesystem>
#include <fstream>

namespace dfg {

// Write beside the destination and replace it only after a successful close.
// A failed or abandoned write leaves the previous destination intact.
class AtomicOutput {
public:
  explicit AtomicOutput(const std::filesystem::path &destination);
  ~AtomicOutput();
  AtomicOutput(const AtomicOutput &) = delete;
  AtomicOutput &operator=(const AtomicOutput &) = delete;

  std::ostream &stream() { return output_; }
  void commit();

private:
  std::filesystem::path destination_;
  std::filesystem::path temporary_;
  std::ofstream output_;
};

} // namespace dfg
