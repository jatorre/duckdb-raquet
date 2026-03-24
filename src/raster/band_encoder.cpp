#include "band_encoder.hpp"
#include <stdexcept>
#include <zlib.h>

#ifdef RAQUET_HAS_JPEG
#include <jpeglib.h>
#endif

#ifdef RAQUET_HAS_WEBP
#include <webp/encode.h>
#endif

namespace duckdb {
namespace raquet {

std::vector<uint8_t> compress_gzip(const uint8_t *data, size_t size) {
    // Estimate compressed size (zlib recommends compressBound)
    uLongf compressed_size = compressBound(static_cast<uLong>(size));
    std::vector<uint8_t> compressed(compressed_size);

    int ret = compress2(compressed.data(), &compressed_size,
                        data, static_cast<uLong>(size), Z_DEFAULT_COMPRESSION);
    if (ret != Z_OK) {
        throw std::runtime_error("gzip compression failed with error code " + std::to_string(ret));
    }

    compressed.resize(compressed_size);
    return compressed;
}

std::vector<uint8_t> encode_jpeg(const uint8_t *data, int width, int height,
                                  int channels, int quality) {
#ifdef RAQUET_HAS_JPEG
    struct jpeg_compress_struct cinfo;
    struct jpeg_error_mgr jerr;

    cinfo.err = jpeg_std_error(&jerr);
    jpeg_create_compress(&cinfo);

    // Write to memory buffer
    unsigned char *outbuffer = nullptr;
    unsigned long outsize = 0;
    jpeg_mem_dest(&cinfo, &outbuffer, &outsize);

    cinfo.image_width = width;
    cinfo.image_height = height;
    cinfo.input_components = channels;

    if (channels == 1) {
        cinfo.in_color_space = JCS_GRAYSCALE;
    } else if (channels == 3) {
        cinfo.in_color_space = JCS_RGB;
    } else {
        jpeg_destroy_compress(&cinfo);
        throw std::invalid_argument("JPEG encoding supports 1 or 3 channels, got " +
                                     std::to_string(channels));
    }

    jpeg_set_defaults(&cinfo);
    jpeg_set_quality(&cinfo, quality, TRUE);
    jpeg_start_compress(&cinfo, TRUE);

    int row_stride = width * channels;
    while (cinfo.next_scanline < cinfo.image_height) {
        const uint8_t *row_ptr = data + cinfo.next_scanline * row_stride;
        JSAMPROW row = const_cast<JSAMPROW>(row_ptr);
        jpeg_write_scanlines(&cinfo, &row, 1);
    }

    jpeg_finish_compress(&cinfo);

    std::vector<uint8_t> result(outbuffer, outbuffer + outsize);

    jpeg_destroy_compress(&cinfo);
    free(outbuffer);

    return result;
#else
    throw std::runtime_error("JPEG encoding not available (libjpeg not linked)");
#endif
}

std::vector<uint8_t> encode_webp(const uint8_t *data, int width, int height,
                                  int channels, int quality) {
#ifdef RAQUET_HAS_WEBP
    uint8_t *output = nullptr;
    size_t output_size = 0;

    if (channels == 3) {
        output_size = WebPEncodeRGB(data, width, height, width * 3,
                                     static_cast<float>(quality), &output);
    } else if (channels == 4) {
        output_size = WebPEncodeRGBA(data, width, height, width * 4,
                                      static_cast<float>(quality), &output);
    } else {
        throw std::invalid_argument("WebP encoding supports 3 or 4 channels, got " +
                                     std::to_string(channels));
    }

    if (output_size == 0 || output == nullptr) {
        if (output) WebPFree(output);
        throw std::runtime_error("WebP encoding failed");
    }

    std::vector<uint8_t> result(output, output + output_size);
    WebPFree(output);
    return result;
#else
    throw std::runtime_error("WebP encoding not available (libwebp not linked)");
#endif
}

std::vector<uint8_t> interleave_bands(const std::vector<std::vector<uint8_t>> &bands,
                                       int width, int height, size_t dtype_size) {
    size_t num_bands = bands.size();
    size_t num_pixels = static_cast<size_t>(width) * height;
    size_t total_size = num_pixels * num_bands * dtype_size;

    std::vector<uint8_t> interleaved(total_size);

    for (size_t pixel = 0; pixel < num_pixels; pixel++) {
        for (size_t band = 0; band < num_bands; band++) {
            size_t src_offset = pixel * dtype_size;
            size_t dst_offset = (pixel * num_bands + band) * dtype_size;
            std::memcpy(interleaved.data() + dst_offset,
                        bands[band].data() + src_offset,
                        dtype_size);
        }
    }

    return interleaved;
}

} // namespace raquet
} // namespace duckdb
