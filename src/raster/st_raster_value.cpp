#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "band_decoder.hpp"
#include "quadbin.hpp"
#include "raquet_metadata.hpp"
#include <cstring>

namespace duckdb {

// ============================================================================
// Geometry helpers - extract coordinates from DuckDB's GEOMETRY type
// ============================================================================
//
// DuckDB's native GEOMETRY type stores data as standard WKB (Well-Known Binary)
// when created from WKT like 'POINT(x y)'::GEOMETRY.
//
// WKB POINT format (21 bytes for 2D):
// - 1 byte: byte order (01 = little-endian, 00 = big-endian)
// - 4 bytes: geometry type (1 = Point)
// - 8 bytes: X coordinate (double)
// - 8 bytes: Y coordinate (double)

// Extract X and Y from a GEOMETRY value (assumed to be a POINT)
// Returns true on success, false if not a valid point
static bool ExtractPointCoordinates(const string_t &geom, double &x, double &y) {
    const uint8_t *data = reinterpret_cast<const uint8_t*>(geom.GetData());
    idx_t size = geom.GetSize();

    // Standard WKB format (21 bytes for 2D point)
    if (size >= 21) {
        uint8_t byte_order = data[0];
        // Valid byte orders are 0 (big-endian) or 1 (little-endian)
        if (byte_order > 1) {
            return false;
        }
        bool little_endian = (byte_order == 1);

        // Read geometry type
        uint32_t geom_type;
        if (little_endian) {
            memcpy(&geom_type, data + 1, 4);
        } else {
            geom_type = (static_cast<uint32_t>(data[1]) << 24) |
                        (static_cast<uint32_t>(data[2]) << 16) |
                        (static_cast<uint32_t>(data[3]) << 8) |
                        static_cast<uint32_t>(data[4]);
        }

        // Type 1 = Point, also check for SRID variants
        uint32_t base_type = geom_type & 0xFF;
        if (base_type == 1) {
            idx_t coord_offset = 5;
            // Check for SRID flag (0x20000000 in EWKB)
            if (geom_type & 0x20000000) {
                coord_offset += 4;  // Skip 4-byte SRID
                if (size < coord_offset + 16) {
                    return false;
                }
            }

            if (little_endian) {
                memcpy(&x, data + coord_offset, 8);
                memcpy(&y, data + coord_offset + 8, 8);
            } else {
                uint64_t x_bits, y_bits;
                for (int i = 0; i < 8; i++) {
                    reinterpret_cast<uint8_t*>(&x_bits)[7-i] = data[coord_offset + i];
                    reinterpret_cast<uint8_t*>(&y_bits)[7-i] = data[coord_offset + 8 + i];
                }
                memcpy(&x, &x_bits, 8);
                memcpy(&y, &y_bits, 8);
            }
            return true;
        }
    }

    return false;
}

// ============================================================================
// Pixel coordinate functions (not lon/lat based)
// ============================================================================

// raquet_pixel(band BLOB, dtype VARCHAR, x INT, y INT, width INT, compression VARCHAR) -> DOUBLE
// Get a single pixel value from band data by x,y coordinates within the tile
static void RaquetPixelFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    // Flatten all vectors to ensure consistent access
    args.data[0].Flatten(args.size());
    args.data[1].Flatten(args.size());
    args.data[2].Flatten(args.size());
    args.data[3].Flatten(args.size());
    args.data[4].Flatten(args.size());
    args.data[5].Flatten(args.size());

    auto band_data = FlatVector::GetData<string_t>(args.data[0]);
    auto dtype_data = FlatVector::GetData<string_t>(args.data[1]);
    auto x_data = FlatVector::GetData<int32_t>(args.data[2]);
    auto y_data = FlatVector::GetData<int32_t>(args.data[3]);
    auto width_data = FlatVector::GetData<int32_t>(args.data[4]);
    auto compression_data = FlatVector::GetData<string_t>(args.data[5]);
    auto result_data = FlatVector::GetData<double>(result);
    auto &result_mask = FlatVector::Validity(result);

    for (idx_t i = 0; i < args.size(); i++) {
        auto band = band_data[i];
        auto dtype = dtype_data[i].GetString();
        auto x = x_data[i];
        auto y = y_data[i];
        auto width = width_data[i];
        auto compression = compression_data[i].GetString();

        bool compressed = (compression == "gzip");

        const char* band_ptr = band.GetData();
        idx_t band_size = band.GetSize();

        // Return NULL for empty/invalid band data
        if (band_ptr == nullptr || band_size == 0) {
            result_mask.SetInvalid(i);
            continue;
        }
        if (x < 0 || y < 0 || width <= 0) {
            result_mask.SetInvalid(i);
            continue;
        }

        try {
            result_data[i] = raquet::decode_pixel(
                reinterpret_cast<const uint8_t*>(band_ptr),
                static_cast<size_t>(band_size),
                dtype, x, y, width, compressed
            );
        } catch (const std::exception &e) {
            throw InvalidInputException("raquet_pixel error: %s", e.what());
        }
    }
}

// raquet_decode_band(band BLOB, dtype VARCHAR, width INT, height INT, compression VARCHAR) -> DOUBLE[]
// Decode entire band to array of doubles
static void RaquetDecodeBandFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    // Flatten all vectors to ensure consistent access
    args.data[0].Flatten(args.size());
    args.data[1].Flatten(args.size());
    args.data[2].Flatten(args.size());
    args.data[3].Flatten(args.size());
    args.data[4].Flatten(args.size());

    auto band_data = FlatVector::GetData<string_t>(args.data[0]);
    auto dtype_data = FlatVector::GetData<string_t>(args.data[1]);
    auto width_data = FlatVector::GetData<int32_t>(args.data[2]);
    auto height_data = FlatVector::GetData<int32_t>(args.data[3]);
    auto compression_data = FlatVector::GetData<string_t>(args.data[4]);

    auto list_data = ListVector::GetData(result);
    auto &list_child = ListVector::GetEntry(result);
    auto child_data = FlatVector::GetData<double>(list_child);

    idx_t total_list_size = 0;

    for (idx_t i = 0; i < args.size(); i++) {
        auto band = band_data[i];
        auto dtype = dtype_data[i].GetString();
        auto width = width_data[i];
        auto height = height_data[i];
        auto compression = compression_data[i].GetString();

        bool compressed = (compression == "gzip");

        // Check for empty band data
        if (band.GetSize() == 0) {
            list_data[i].offset = total_list_size;
            list_data[i].length = 0;
            FlatVector::Validity(result).SetInvalid(i);
            continue;
        }

        try {
            auto values = raquet::decode_band(
                reinterpret_cast<const uint8_t*>(band.GetData()),
                band.GetSize(),
                dtype, width, height, compressed
            );

            // Set list entry
            list_data[i].offset = total_list_size;
            list_data[i].length = values.size();

            // Reserve space if needed
            ListVector::Reserve(result, total_list_size + values.size());
            child_data = FlatVector::GetData<double>(ListVector::GetEntry(result));

            // Copy values
            for (size_t j = 0; j < values.size(); j++) {
                child_data[total_list_size + j] = values[j];
            }

            total_list_size += values.size();
        } catch (const std::exception &e) {
            list_data[i].offset = total_list_size;
            list_data[i].length = 0;
            FlatVector::Validity(result).SetInvalid(i);
        }
    }

    ListVector::SetListSize(result, total_list_size);
}

// ============================================================================
// Metadata-aware pixel functions
// ============================================================================

// raquet_pixel(band BLOB, metadata VARCHAR, x INT, y INT) -> DOUBLE
static void RaquetPixelWithMetadataFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    args.data[0].Flatten(args.size());
    args.data[1].Flatten(args.size());
    args.data[2].Flatten(args.size());
    args.data[3].Flatten(args.size());

    auto band_data = FlatVector::GetData<string_t>(args.data[0]);
    auto metadata_data = FlatVector::GetData<string_t>(args.data[1]);
    auto x_data = FlatVector::GetData<int32_t>(args.data[2]);
    auto y_data = FlatVector::GetData<int32_t>(args.data[3]);
    auto result_data = FlatVector::GetData<double>(result);
    auto &result_mask = FlatVector::Validity(result);

    for (idx_t i = 0; i < args.size(); i++) {
        auto band = band_data[i];
        auto metadata_str = metadata_data[i].GetString();
        auto x = x_data[i];
        auto y = y_data[i];

        const char* band_ptr = band.GetData();
        idx_t band_size = band.GetSize();

        if (band_ptr == nullptr || band_size == 0) {
            result_mask.SetInvalid(i);
            continue;
        }
        if (x < 0 || y < 0) {
            result_mask.SetInvalid(i);
            continue;
        }

        try {
            auto meta = raquet::parse_metadata(metadata_str);
            std::string dtype = meta.bands.empty() ? "uint8" : meta.bands[0].second;
            bool compressed = (meta.compression == "gzip");

            result_data[i] = raquet::decode_pixel(
                reinterpret_cast<const uint8_t*>(band_ptr),
                static_cast<size_t>(band_size),
                dtype, x, y, meta.block_width, compressed
            );
        } catch (const std::exception &e) {
            throw InvalidInputException("raquet_pixel error: %s", e.what());
        }
    }
}

// raquet_pixel(band BLOB, metadata VARCHAR, band_index INT, x INT, y INT) -> DOUBLE
static void RaquetPixelWithMetadataAndBandFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    args.data[0].Flatten(args.size());
    args.data[1].Flatten(args.size());
    args.data[2].Flatten(args.size());
    args.data[3].Flatten(args.size());
    args.data[4].Flatten(args.size());

    auto band_data = FlatVector::GetData<string_t>(args.data[0]);
    auto metadata_data = FlatVector::GetData<string_t>(args.data[1]);
    auto band_idx_data = FlatVector::GetData<int32_t>(args.data[2]);
    auto x_data = FlatVector::GetData<int32_t>(args.data[3]);
    auto y_data = FlatVector::GetData<int32_t>(args.data[4]);
    auto result_data = FlatVector::GetData<double>(result);
    auto &result_mask = FlatVector::Validity(result);

    for (idx_t i = 0; i < args.size(); i++) {
        auto band = band_data[i];
        auto metadata_str = metadata_data[i].GetString();
        auto band_idx = band_idx_data[i];
        auto x = x_data[i];
        auto y = y_data[i];

        const char* band_ptr = band.GetData();
        idx_t band_size = band.GetSize();

        if (band_ptr == nullptr || band_size == 0) {
            result_mask.SetInvalid(i);
            continue;
        }
        if (x < 0 || y < 0 || band_idx < 0) {
            result_mask.SetInvalid(i);
            continue;
        }

        try {
            auto meta = raquet::parse_metadata(metadata_str);
            std::string dtype = meta.get_band_type(band_idx);
            bool compressed = (meta.compression == "gzip");

            result_data[i] = raquet::decode_pixel(
                reinterpret_cast<const uint8_t*>(band_ptr),
                static_cast<size_t>(band_size),
                dtype, x, y, meta.block_width, compressed
            );
        } catch (const std::exception &e) {
            throw InvalidInputException("raquet_pixel error: %s", e.what());
        }
    }
}

// ============================================================================
// GEOMETRY-based ST_RasterValue functions (uses DuckDB 1.5+ native GEOMETRY)
// ============================================================================
//
// ST_RasterValue(block, band, geometry, metadata) -> DOUBLE
//
// PostGIS-like API for extracting raster pixel values at a point.
//
// Semantics:
//   - Input geometry must be a POINT in EPSG:4326 (WGS84 lon/lat)
//   - Internally transforms to EPSG:3857 for pixel lookup (rasters are WebMercator)
//   - Returns NULL if:
//     * Point falls outside the block's spatial extent
//     * Sampled pixel equals the band's NODATA value
//     * Band data is empty
//   - Resampling: nearest neighbor (default, bilinear can be added later)
//
// Execution model:
//   - Use ST_RasterIntersects(block, geom) for block pruning in WHERE clause
//   - ST_RasterValue is only evaluated after pruning (blocks are not decoded unnecessarily)
//
// Example:
//   SELECT ST_RasterValue(block, band_1, ST_Point(-3.7, 40.4), metadata)
//   FROM read_raquet('file.parquet')
//   WHERE ST_RasterIntersects(block, ST_Point(-3.7, 40.4));
// ============================================================================

// ST_RasterValue(block UBIGINT, band BLOB, point GEOMETRY, metadata VARCHAR) -> DOUBLE
// Get pixel value at point geometry location
static void STRasterValueWithGeometryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    args.data[0].Flatten(args.size());
    args.data[1].Flatten(args.size());
    args.data[2].Flatten(args.size());
    args.data[3].Flatten(args.size());

    auto block_data = FlatVector::GetData<uint64_t>(args.data[0]);
    auto band_data = FlatVector::GetData<string_t>(args.data[1]);
    auto geom_data = FlatVector::GetData<string_t>(args.data[2]);  // GEOMETRY is stored as string_t (WKB)
    auto metadata_data = FlatVector::GetData<string_t>(args.data[3]);
    auto result_data = FlatVector::GetData<double>(result);
    auto &result_mask = FlatVector::Validity(result);

    for (idx_t i = 0; i < args.size(); i++) {
        auto block = block_data[i];
        auto band = band_data[i];
        auto geom = geom_data[i];
        auto metadata_str = metadata_data[i].GetString();

        if (band.GetSize() == 0) {
            result_mask.SetInvalid(i);
            continue;
        }

        // Extract lon/lat from point geometry (assumed EPSG:4326)
        double lon, lat;
        if (!ExtractPointCoordinates(geom, lon, lat)) {
            throw InvalidInputException("ST_RasterValue: geometry must be a POINT in EPSG:4326");
        }

        try {
            auto meta = raquet::parse_metadata(metadata_str);
            std::string dtype = meta.bands.empty() ? "uint8" : meta.bands[0].second;
            int tile_size = meta.block_width;

            int resolution = quadbin::cell_to_resolution(block);
            int tile_x, tile_y, z;
            quadbin::cell_to_tile(block, tile_x, tile_y, z);

            int pixel_x, pixel_y, calc_tile_x, calc_tile_y;
            quadbin::lonlat_to_pixel(lon, lat, resolution, tile_size, pixel_x, pixel_y, calc_tile_x, calc_tile_y);

            // Return NULL if point falls outside this block
            if (calc_tile_x != tile_x || calc_tile_y != tile_y) {
                result_mask.SetInvalid(i);
                continue;
            }

            double value;
            if (meta.is_interleaved()) {
                // v0.4.0 interleaved layout: all bands in single column
                value = raquet::decode_pixel_interleaved(
                    reinterpret_cast<const uint8_t*>(band.GetData()),
                    band.GetSize(),
                    dtype, pixel_x, pixel_y, tile_size,
                    0, meta.num_bands(),
                    meta.compression
                );
            } else {
                // Standard sequential layout
                bool compressed = (meta.compression == "gzip");
                value = raquet::decode_pixel(
                    reinterpret_cast<const uint8_t*>(band.GetData()),
                    band.GetSize(),
                    dtype, pixel_x, pixel_y, tile_size, compressed
                );
            }

            // Check for NODATA value and return NULL if matched
            if (!meta.band_info.empty() && meta.is_nodata(0, value)) {
                result_mask.SetInvalid(i);
                continue;
            }

            result_data[i] = value;
        } catch (const std::exception &e) {
            throw InvalidInputException("ST_RasterValue error: %s", e.what());
        }
    }
}

// ST_RasterValue(block UBIGINT, band BLOB, point GEOMETRY, metadata VARCHAR, band_index INT) -> DOUBLE
// Get pixel value at point geometry with explicit band index (for multi-band rasters)
// Auto-detects interleaved vs sequential layout from metadata
static void STRasterValueWithGeometryAndBandFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    args.data[0].Flatten(args.size());
    args.data[1].Flatten(args.size());
    args.data[2].Flatten(args.size());
    args.data[3].Flatten(args.size());
    args.data[4].Flatten(args.size());

    auto block_data = FlatVector::GetData<uint64_t>(args.data[0]);
    auto band_data = FlatVector::GetData<string_t>(args.data[1]);
    auto geom_data = FlatVector::GetData<string_t>(args.data[2]);
    auto metadata_data = FlatVector::GetData<string_t>(args.data[3]);
    auto band_idx_data = FlatVector::GetData<int32_t>(args.data[4]);
    auto result_data = FlatVector::GetData<double>(result);
    auto &result_mask = FlatVector::Validity(result);

    for (idx_t i = 0; i < args.size(); i++) {
        auto block = block_data[i];
        auto band = band_data[i];
        auto geom = geom_data[i];
        auto metadata_str = metadata_data[i].GetString();
        auto band_idx = band_idx_data[i];

        if (band.GetSize() == 0 || band_idx < 0) {
            result_mask.SetInvalid(i);
            continue;
        }

        // Extract lon/lat from point geometry (assumed EPSG:4326)
        double lon, lat;
        if (!ExtractPointCoordinates(geom, lon, lat)) {
            throw InvalidInputException("ST_RasterValue: geometry must be a POINT in EPSG:4326");
        }

        try {
            auto meta = raquet::parse_metadata(metadata_str);
            std::string dtype = meta.get_band_type(band_idx);
            int tile_size = meta.block_width;

            int resolution = quadbin::cell_to_resolution(block);
            int tile_x, tile_y, z;
            quadbin::cell_to_tile(block, tile_x, tile_y, z);

            int pixel_x, pixel_y, calc_tile_x, calc_tile_y;
            quadbin::lonlat_to_pixel(lon, lat, resolution, tile_size, pixel_x, pixel_y, calc_tile_x, calc_tile_y);

            // Return NULL if point falls outside this block
            if (calc_tile_x != tile_x || calc_tile_y != tile_y) {
                result_mask.SetInvalid(i);
                continue;
            }

            double value;
            if (meta.is_interleaved()) {
                // v0.4.0 interleaved layout: all bands in single column
                value = raquet::decode_pixel_interleaved(
                    reinterpret_cast<const uint8_t*>(band.GetData()),
                    band.GetSize(),
                    dtype, pixel_x, pixel_y, tile_size,
                    band_idx, meta.num_bands(),
                    meta.compression
                );
            } else {
                // Standard sequential layout
                bool compressed = (meta.compression == "gzip");
                value = raquet::decode_pixel(
                    reinterpret_cast<const uint8_t*>(band.GetData()),
                    band.GetSize(),
                    dtype, pixel_x, pixel_y, tile_size, compressed
                );
            }

            // Check for NODATA value and return NULL if matched
            if (band_idx < static_cast<int32_t>(meta.band_info.size()) && meta.is_nodata(band_idx, value)) {
                result_mask.SetInvalid(i);
                continue;
            }

            result_data[i] = value;
        } catch (const std::exception &e) {
            throw InvalidInputException("ST_RasterValue error: %s", e.what());
        }
    }
}

// ============================================================================
// v0.4.0: Interleaved layout low-level pixel access
// ============================================================================

// raquet_pixel_interleaved(pixels BLOB, metadata VARCHAR, band_index INT, x INT, y INT) -> DOUBLE
// Get a pixel value from interleaved (BIP) layout data
static void RaquetPixelInterleavedFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    args.data[0].Flatten(args.size());
    args.data[1].Flatten(args.size());
    args.data[2].Flatten(args.size());
    args.data[3].Flatten(args.size());
    args.data[4].Flatten(args.size());

    auto pixels_data = FlatVector::GetData<string_t>(args.data[0]);
    auto metadata_data = FlatVector::GetData<string_t>(args.data[1]);
    auto band_idx_data = FlatVector::GetData<int32_t>(args.data[2]);
    auto x_data = FlatVector::GetData<int32_t>(args.data[3]);
    auto y_data = FlatVector::GetData<int32_t>(args.data[4]);
    auto result_data = FlatVector::GetData<double>(result);
    auto &result_mask = FlatVector::Validity(result);

    for (idx_t i = 0; i < args.size(); i++) {
        auto pixels = pixels_data[i];
        auto metadata_str = metadata_data[i].GetString();
        auto band_idx = band_idx_data[i];
        auto x = x_data[i];
        auto y = y_data[i];

        const char* pixels_ptr = pixels.GetData();
        idx_t pixels_size = pixels.GetSize();

        if (pixels_ptr == nullptr || pixels_size == 0) {
            result_mask.SetInvalid(i);
            continue;
        }
        if (x < 0 || y < 0 || band_idx < 0) {
            result_mask.SetInvalid(i);
            continue;
        }

        try {
            auto meta = raquet::parse_metadata(metadata_str);
            std::string dtype = meta.get_band_type(band_idx);
            int num_bands = meta.num_bands();

            double value = raquet::decode_pixel_interleaved(
                reinterpret_cast<const uint8_t*>(pixels_ptr),
                static_cast<size_t>(pixels_size),
                dtype, x, y, meta.block_width,
                band_idx, num_bands,
                meta.compression
            );

            if (meta.is_nodata(band_idx, value)) {
                result_mask.SetInvalid(i);
                continue;
            }

            result_data[i] = value;
        } catch (const std::exception &e) {
            throw InvalidInputException("raquet_pixel_interleaved error: %s", e.what());
        }
    }
}

// ============================================================================
// Function registration
// ============================================================================

void RegisterRasterValueFunctions(ExtensionLoader &loader) {
    // raquet_pixel(band, dtype, x, y, width, compression) -> DOUBLE
    ScalarFunction pixel_fn("raquet_pixel",
        {LogicalType::BLOB, LogicalType::VARCHAR, LogicalType::INTEGER,
         LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::VARCHAR},
        LogicalType::DOUBLE,
        RaquetPixelFunction);
    loader.RegisterFunction(pixel_fn);

    // raquet_decode_band(band, dtype, width, height, compression) -> DOUBLE[]
    ScalarFunction decode_band_fn("raquet_decode_band",
        {LogicalType::BLOB, LogicalType::VARCHAR, LogicalType::INTEGER,
         LogicalType::INTEGER, LogicalType::VARCHAR},
        LogicalType::LIST(LogicalType::DOUBLE),
        RaquetDecodeBandFunction);
    loader.RegisterFunction(decode_band_fn);

    // raquet_pixel(band, metadata, x, y) -> DOUBLE
    ScalarFunction pixel_meta_fn("raquet_pixel",
        {LogicalType::BLOB, LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::INTEGER},
        LogicalType::DOUBLE,
        RaquetPixelWithMetadataFunction);
    loader.RegisterFunction(pixel_meta_fn);

    // raquet_pixel(band, metadata, band_index, x, y) -> DOUBLE
    ScalarFunction pixel_meta_band_fn("raquet_pixel",
        {LogicalType::BLOB, LogicalType::VARCHAR, LogicalType::INTEGER,
         LogicalType::INTEGER, LogicalType::INTEGER},
        LogicalType::DOUBLE,
        RaquetPixelWithMetadataAndBandFunction);
    loader.RegisterFunction(pixel_meta_band_fn);

    // ========================================================================
    // GEOMETRY type functions (uses DuckDB 1.5+ native GEOMETRY)
    // ========================================================================

    // ST_RasterValue(block, band, point_geometry, metadata) -> DOUBLE
    ScalarFunction raster_value_geom_fn("ST_RasterValue",
        {LogicalType::UBIGINT, LogicalType::BLOB, LogicalType::GEOMETRY(),
         LogicalType::VARCHAR},
        LogicalType::DOUBLE,
        STRasterValueWithGeometryFunction);
    loader.RegisterFunction(raster_value_geom_fn);

    // ST_RasterValue(block, band, point_geometry, metadata, band_index) -> DOUBLE
    ScalarFunction raster_value_geom_band_fn("ST_RasterValue",
        {LogicalType::UBIGINT, LogicalType::BLOB, LogicalType::GEOMETRY(),
         LogicalType::VARCHAR, LogicalType::INTEGER},
        LogicalType::DOUBLE,
        STRasterValueWithGeometryAndBandFunction);
    loader.RegisterFunction(raster_value_geom_band_fn);

    // ========================================================================
    // v0.4.0: Interleaved layout low-level function
    // ========================================================================

    // raquet_pixel_interleaved(pixels, metadata, band_index, x, y) -> DOUBLE
    ScalarFunction pixel_interleaved_fn("raquet_pixel_interleaved",
        {LogicalType::BLOB, LogicalType::VARCHAR, LogicalType::INTEGER,
         LogicalType::INTEGER, LogicalType::INTEGER},
        LogicalType::DOUBLE,
        RaquetPixelInterleavedFunction);
    loader.RegisterFunction(pixel_interleaved_fn);
}

} // namespace duckdb
