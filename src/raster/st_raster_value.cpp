#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "band_decoder.hpp"
#include "quadbin.hpp"

namespace duckdb {

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

// ST_RasterValue(block UBIGINT, band BLOB, lon DOUBLE, lat DOUBLE, dtype VARCHAR, width INT, compression VARCHAR) -> DOUBLE
// Get pixel value at lon/lat from a raquet tile
static void STRasterValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    // Flatten all vectors to ensure consistent access
    args.data[0].Flatten(args.size());
    args.data[1].Flatten(args.size());
    args.data[2].Flatten(args.size());
    args.data[3].Flatten(args.size());
    args.data[4].Flatten(args.size());
    args.data[5].Flatten(args.size());
    args.data[6].Flatten(args.size());

    auto block_data = FlatVector::GetData<uint64_t>(args.data[0]);
    auto band_data = FlatVector::GetData<string_t>(args.data[1]);
    auto lon_data = FlatVector::GetData<double>(args.data[2]);
    auto lat_data = FlatVector::GetData<double>(args.data[3]);
    auto dtype_data = FlatVector::GetData<string_t>(args.data[4]);
    auto width_data = FlatVector::GetData<int32_t>(args.data[5]);
    auto compression_data = FlatVector::GetData<string_t>(args.data[6]);
    auto result_data = FlatVector::GetData<double>(result);
    auto &result_mask = FlatVector::Validity(result);

    for (idx_t i = 0; i < args.size(); i++) {
        auto block = block_data[i];
        auto band = band_data[i];
        auto lon = lon_data[i];
        auto lat = lat_data[i];
        auto dtype = dtype_data[i].GetString();
        auto tile_size = width_data[i];  // Assuming square tiles
        auto compression = compression_data[i].GetString();

        // Get resolution from the block
        int resolution = quadbin::cell_to_resolution(block);

        // Get tile coordinates from block
        int tile_x, tile_y, z;
        quadbin::cell_to_tile(block, tile_x, tile_y, z);

        // Calculate pixel coordinates within the tile
        int pixel_x, pixel_y, calc_tile_x, calc_tile_y;
        quadbin::lonlat_to_pixel(lon, lat, resolution, tile_size, pixel_x, pixel_y, calc_tile_x, calc_tile_y);

        // Verify the point falls within this tile
        if (calc_tile_x != tile_x || calc_tile_y != tile_y) {
            // Point is not in this tile, return NULL
            result_mask.SetInvalid(i);
            continue;
        }

        // Check for empty band data
        if (band.GetSize() == 0) {
            result_mask.SetInvalid(i);
            continue;
        }

        bool compressed = (compression == "gzip");

        result_data[i] = raquet::decode_pixel(
            reinterpret_cast<const uint8_t*>(band.GetData()),
            band.GetSize(),
            dtype, pixel_x, pixel_y, tile_size, compressed
        );
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
    }

    ListVector::SetListSize(result, total_list_size);
}

void RegisterRasterValueFunctions(ExtensionLoader &loader) {
    // raquet_pixel(band, dtype, x, y, width, compression) -> DOUBLE
    ScalarFunction pixel_fn("raquet_pixel",
        {LogicalType::BLOB, LogicalType::VARCHAR, LogicalType::INTEGER,
         LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::VARCHAR},
        LogicalType::DOUBLE,
        RaquetPixelFunction);
    loader.RegisterFunction(pixel_fn);

    // ST_RasterValue(block, band, lon, lat, dtype, width, compression) -> DOUBLE
    ScalarFunction raster_value_fn("ST_RasterValue",
        {LogicalType::UBIGINT, LogicalType::BLOB, LogicalType::DOUBLE, LogicalType::DOUBLE,
         LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR},
        LogicalType::DOUBLE,
        STRasterValueFunction);
    loader.RegisterFunction(raster_value_fn);

    // raquet_decode_band(band, dtype, width, height, compression) -> DOUBLE[]
    ScalarFunction decode_band_fn("raquet_decode_band",
        {LogicalType::BLOB, LogicalType::VARCHAR, LogicalType::INTEGER,
         LogicalType::INTEGER, LogicalType::VARCHAR},
        LogicalType::LIST(LogicalType::DOUBLE),
        RaquetDecodeBandFunction);
    loader.RegisterFunction(decode_band_fn);
}

} // namespace duckdb
