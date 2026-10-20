#pragma once

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <string_view>
#include <system_error>

namespace dfg::protocol {

// Bound allocations before reading data supplied by a network peer.
inline constexpr uint64_t max_chunk_bytes = 256ULL * 1024 * 1024;
inline constexpr uint64_t max_message_bytes = max_chunk_bytes + 1024 * 1024;

inline bool parse_size(std::string_view text, uint64_t &value) {
  if (text.empty())
    return false;
  auto result = std::from_chars(text.data(), text.data() + text.size(), value);
  return result.ec == std::errc{} && result.ptr == text.data() + text.size();
}

inline bool valid_digest(std::string_view text) {
  return text.size() == 64 && std::all_of(text.begin(), text.end(), [](char c) {
           return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
         });
}

// The line protocol and metadata store use whitespace and '|' as separators.
// Remote names are single names, never filesystem paths.
inline bool valid_name(std::string_view name, size_t max_length = 200) {
  return !name.empty() && name.size() <= max_length && name != "." &&
         name != ".." &&
         std::all_of(name.begin(), name.end(), [](unsigned char c) {
           return c > 32 && c != 127 && c != '/' && c != '\\' && c != '|';
         });
}

} // namespace dfg::protocol
