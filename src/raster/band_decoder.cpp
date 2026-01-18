#include "band_decoder.hpp"
#include <zlib.h>
#include <stdexcept>
#include <cstring>
#include <vector>
#include <string>

namespace duckdb {
namespace raquet {

std::vector<uint8_t> decompress_gzip(const uint8_t *data, size_t size) {
    if (size == 0) {
        return {};
    }

    if (data == nullptr) {
        throw std::runtime_error("Null data pointer for decompression");
    }

    // For gzip: estimate decompressed size (typical ratio 10:1 to 100:1)
    // A 256x256 uint8 tile = 65536 bytes
    size_t estimated_size = 256 * 256;  // Default tile size
    if (size > 100) {
        estimated_size = size * 50;  // Conservative estimate
    }

    std::vector<uint8_t> result(estimated_size);

    // Copy input to ensure it's contiguous
    std::vector<uint8_t> input(data, data + size);

    z_stream strm;
    memset(&strm, 0, sizeof(strm));
    strm.next_in = input.data();
    strm.avail_in = static_cast<uInt>(input.size());
    strm.next_out = result.data();
    strm.avail_out = static_cast<uInt>(result.size());

    // 15 + 16 = gzip decoding only
    int ret = inflateInit2(&strm, 15 + 16);
    if (ret != Z_OK) {
        throw std::runtime_error("inflateInit2 failed: " + std::to_string(ret));
    }

    ret = inflate(&strm, Z_FINISH);

    if (ret == Z_BUF_ERROR && strm.avail_out == 0) {
        // Need more output space - resize and retry
        inflateEnd(&strm);

        result.resize(result.size() * 4);
        std::vector<uint8_t> input2(data, data + size);

        z_stream strm2;
        memset(&strm2, 0, sizeof(strm2));
        strm2.next_in = input2.data();
        strm2.avail_in = static_cast<uInt>(input2.size());
        strm2.next_out = result.data();
        strm2.avail_out = static_cast<uInt>(result.size());

        ret = inflateInit2(&strm2, 15 + 16);
        if (ret != Z_OK) {
            throw std::runtime_error("inflateInit2 retry failed");
        }

        ret = inflate(&strm2, Z_FINISH);
        size_t decompressed_size = result.size() - strm2.avail_out;
        inflateEnd(&strm2);

        if (ret != Z_STREAM_END) {
            throw std::runtime_error("inflate retry failed: " + std::to_string(ret));
        }

        result.resize(decompressed_size);
        return result;
    }

    if (ret != Z_STREAM_END) {
        inflateEnd(&strm);
        throw std::runtime_error("inflate failed: " + std::to_string(ret));
    }

    size_t decompressed_size = result.size() - strm.avail_out;
    inflateEnd(&strm);

    result.resize(decompressed_size);
    return result;
}

double decode_pixel(const uint8_t *band_data, size_t band_size,
                    const std::string &dtype_str,
                    int pixel_x, int pixel_y, int width,
                    bool compressed) {
    BandDataType dtype = parse_dtype(dtype_str);

    const uint8_t *data;
    std::vector<uint8_t> decompressed;

    if (compressed) {
        decompressed = decompress_gzip(band_data, band_size);
        data = decompressed.data();
    } else {
        data = band_data;
    }

    // Row-major order: offset = y * width + x
    size_t offset = static_cast<size_t>(pixel_y) * width + pixel_x;

    return get_pixel_value(data, offset, dtype);
}

std::vector<double> decode_band(const uint8_t *band_data, size_t band_size,
                                 const std::string &dtype_str,
                                 int width, int height,
                                 bool compressed) {
    BandDataType dtype = parse_dtype(dtype_str);

    const uint8_t *data;
    std::vector<uint8_t> decompressed;

    if (compressed) {
        decompressed = decompress_gzip(band_data, band_size);
        data = decompressed.data();
    } else {
        data = band_data;
    }

    size_t pixel_count = static_cast<size_t>(width) * height;
    std::vector<double> result(pixel_count);

    for (size_t i = 0; i < pixel_count; i++) {
        result[i] = get_pixel_value(data, i, dtype);
    }

    return result;
}

} // namespace raquet
} // namespace duckdb
