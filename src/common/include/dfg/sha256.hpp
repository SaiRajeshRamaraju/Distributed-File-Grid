#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <algorithm>

namespace dfg {
namespace hash {

class SHA256 {
public:
    SHA256() { reset(); }

    void update(const uint8_t* data, size_t length) {
        if (!data || length == 0) return;

        size_t offset = 0;

        // If we have partial data in the buffer, fill it first
        if (datalen_ > 0) {
            size_t to_fill = 64 - datalen_;
            if (length < to_fill) {
                std::memcpy(data_ + datalen_, data, length);
                datalen_ += static_cast<uint32_t>(length);
                return;
            }
            std::memcpy(data_ + datalen_, data, to_fill);
            transform(data_);
            bitlen_ += 512;
            datalen_ = 0;
            offset += to_fill;
        }

        // Process complete 64-byte blocks directly from the input buffer
        while (offset + 64 <= length) {
            transform(data + offset);
            bitlen_ += 512;
            offset += 64;
        }

        // Buffer remaining bytes (< 64)
        if (offset < length) {
            datalen_ = static_cast<uint32_t>(length - offset);
            std::memcpy(data_, data + offset, datalen_);
        }
    }

    void update(const char* data, size_t length) {
        update(reinterpret_cast<const uint8_t*>(data), length);
    }

    void update(const std::vector<char>& data) {
        if (!data.empty()) {
            update(reinterpret_cast<const uint8_t*>(data.data()), data.size());
        }
    }

    // Computes digest without mutating this instance's ongoing state
    std::string digest() const {
        SHA256 copy = *this;
        return copy.finalize();
    }

    void reset() {
        datalen_ = 0;
        bitlen_ = 0;
        state_[0] = 0x6a09e667; state_[1] = 0xbb67ae85;
        state_[2] = 0x3c6ef372; state_[3] = 0xa54ff53a;
        state_[4] = 0x510e527f; state_[5] = 0x9b05688c;
        state_[6] = 0x1f83d9ab; state_[7] = 0x5be0cd19;
    }

private:
    uint8_t  data_[64];
    uint32_t datalen_;
    uint64_t bitlen_;
    uint32_t state_[8];

    constexpr static uint32_t k[64] = {
        0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
        0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
        0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
        0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
        0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
        0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
        0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
        0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
    };

    // Safe circular right shift (guards against n % 32 == 0 undefined behavior in C++)
    static inline uint32_t rotr(uint32_t x, uint32_t n) {
        return (n == 0) ? x : ((x >> n) | (x << (32 - n)));
    }

    // NIST FIPS 180-4 standard functions
    static inline uint32_t ch(uint32_t x, uint32_t y, uint32_t z) { return (x & y) ^ (~x & z); }
    static inline uint32_t maj(uint32_t x, uint32_t y, uint32_t z) { return (x & y) ^ (x & z) ^ (y & z); }

    // Upper Sigma: Σ0 and Σ1
    static inline uint32_t SIGMA0(uint32_t x) { return rotr(x, 2) ^ rotr(x, 13) ^ rotr(x, 22); }
    static inline uint32_t SIGMA1(uint32_t x) { return rotr(x, 6) ^ rotr(x, 11) ^ rotr(x, 25); }

    // Lower Sigma: σ0 and σ1
    static inline uint32_t sigma0(uint32_t x) { return rotr(x, 7) ^ rotr(x, 18) ^ (x >> 3); }
    static inline uint32_t sigma1(uint32_t x) { return rotr(x, 17) ^ rotr(x, 19) ^ (x >> 10); }

    void transform(const uint8_t* chunk) {
        uint32_t a, b, c, d, e, f, g, h, i, j, t1, t2, m[64];

        for (i = 0, j = 0; i < 16; ++i, j += 4)
            m[i] = (static_cast<uint32_t>(chunk[j]) << 24) |
                   (static_cast<uint32_t>(chunk[j + 1]) << 16) |
                   (static_cast<uint32_t>(chunk[j + 2]) << 8) |
                   (static_cast<uint32_t>(chunk[j + 3]));
        for ( ; i < 64; ++i)
            m[i] = sigma1(m[i - 2]) + m[i - 7] + sigma0(m[i - 15]) + m[i - 16];

        a = state_[0]; b = state_[1]; c = state_[2]; d = state_[3];
        e = state_[4]; f = state_[5]; g = state_[6]; h = state_[7];

        for (i = 0; i < 64; ++i) {
            t1 = h + SIGMA1(e) + ch(e, f, g) + k[i] + m[i];
            t2 = SIGMA0(a) + maj(a, b, c);
            h = g; g = f; f = e; e = d + t1;
            d = c; c = b; b = a; a = t1 + t2;
        }

        state_[0] += a; state_[1] += b; state_[2] += c; state_[3] += d;
        state_[4] += e; state_[5] += f; state_[6] += g; state_[7] += h;
    }

    std::string finalize() {
        uint8_t hash[32];
        uint32_t i = datalen_;

        if (datalen_ < 56) {
            data_[i++] = 0x80;
            while (i < 56) data_[i++] = 0x00;
        } else {
            data_[i++] = 0x80;
            while (i < 64) data_[i++] = 0x00;
            transform(data_);
            for (i = 0; i < 56; ++i) data_[i] = 0;
        }

        bitlen_ += static_cast<uint64_t>(datalen_) * 8;
        data_[63] = static_cast<uint8_t>(bitlen_);
        data_[62] = static_cast<uint8_t>(bitlen_ >> 8);
        data_[61] = static_cast<uint8_t>(bitlen_ >> 16);
        data_[60] = static_cast<uint8_t>(bitlen_ >> 24);
        data_[59] = static_cast<uint8_t>(bitlen_ >> 32);
        data_[58] = static_cast<uint8_t>(bitlen_ >> 40);
        data_[57] = static_cast<uint8_t>(bitlen_ >> 48);
        data_[56] = static_cast<uint8_t>(bitlen_ >> 56);
        transform(data_);

        for (i = 0; i < 4; ++i) {
            hash[i]      = (state_[0] >> (24 - i * 8)) & 0x000000ff;
            hash[i + 4]  = (state_[1] >> (24 - i * 8)) & 0x000000ff;
            hash[i + 8]  = (state_[2] >> (24 - i * 8)) & 0x000000ff;
            hash[i + 12] = (state_[3] >> (24 - i * 8)) & 0x000000ff;
            hash[i + 16] = (state_[4] >> (24 - i * 8)) & 0x000000ff;
            hash[i + 20] = (state_[5] >> (24 - i * 8)) & 0x000000ff;
            hash[i + 24] = (state_[6] >> (24 - i * 8)) & 0x000000ff;
            hash[i + 28] = (state_[7] >> (24 - i * 8)) & 0x000000ff;
        }

        std::stringstream ss;
        ss << std::hex << std::setfill('0');
        for (i = 0; i < 32; i++) {
            ss << std::setw(2) << static_cast<int>(hash[i]);
        }
        return ss.str();
    }
};

inline std::string sha256(const std::vector<char>& data) {
    SHA256 hash;
    hash.update(data);
    return hash.digest();
}

inline std::string sha256(const std::string& data) {
    SHA256 hash;
    hash.update(data.c_str(), data.size());
    return hash.digest();
}

} // namespace hash
} // namespace dfg
