#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "band_decoder.hpp"
#include "raquet_metadata.hpp"
#include <cmath>
#include <limits>

namespace duckdb {

// ============================================================================
// Band Math Functions
// These functions perform pixel-by-pixel operations on raster bands
// ============================================================================

// Helper: Decode band data and return raw pixel pointer
static const uint8_t* DecodeBandData(const string_t &band, bool compressed,
                                      std::vector<uint8_t> &decompressed_buffer) {
    if (compressed) {
        decompressed_buffer = raquet::decompress_gzip(
            reinterpret_cast<const uint8_t*>(band.GetData()),
            band.GetSize()
        );
        return decompressed_buffer.data();
    }
    return reinterpret_cast<const uint8_t*>(band.GetData());
}

// ============================================================================
// ST_NormalizedDifference(band1, band2, metadata) -> DOUBLE[]
// Computes (band1 - band2) / (band1 + band2) for each pixel
// Common use cases: NDVI (NIR, Red), NDWI (Green, NIR), NDBI (SWIR, NIR)
// ============================================================================

static void STNormalizedDifferenceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    args.data[0].Flatten(args.size());
    args.data[1].Flatten(args.size());
    args.data[2].Flatten(args.size());

    auto band1_data = FlatVector::GetData<string_t>(args.data[0]);
    auto band2_data = FlatVector::GetData<string_t>(args.data[1]);
    auto metadata_data = FlatVector::GetData<string_t>(args.data[2]);

    auto &band1_validity = FlatVector::Validity(args.data[0]);
    auto &band2_validity = FlatVector::Validity(args.data[1]);

    auto list_data = ListVector::GetData(result);
    auto &list_child = ListVector::GetEntry(result);
    auto child_data = FlatVector::GetData<double>(list_child);

    idx_t total_list_size = 0;

    for (idx_t i = 0; i < args.size(); i++) {
        if (!band1_validity.RowIsValid(i) || !band2_validity.RowIsValid(i)) {
            list_data[i].offset = total_list_size;
            list_data[i].length = 0;
            FlatVector::Validity(result).SetInvalid(i);
            continue;
        }

        auto band1 = band1_data[i];
        auto band2 = band2_data[i];
        auto metadata_str = metadata_data[i].GetString();

        if (band1.GetSize() == 0 || band2.GetSize() == 0) {
            list_data[i].offset = total_list_size;
            list_data[i].length = 0;
            FlatVector::Validity(result).SetInvalid(i);
            continue;
        }

        try {
            auto meta = raquet::parse_metadata(metadata_str);
            bool compressed = (meta.compression == "gzip");
            int width = meta.block_width;
            int height = meta.block_height;
            size_t num_pixels = static_cast<size_t>(width) * height;

            // Get band data types (use first two bands' types, or default to same type)
            std::string dtype1 = meta.bands.size() > 0 ? meta.bands[0].second : "float32";
            std::string dtype2 = meta.bands.size() > 1 ? meta.bands[1].second : dtype1;

            auto band1_dtype = raquet::parse_dtype(dtype1);
            auto band2_dtype = raquet::parse_dtype(dtype2);

            // Decompress band data
            std::vector<uint8_t> decompressed1, decompressed2;
            const uint8_t *raw1 = DecodeBandData(band1, compressed, decompressed1);
            const uint8_t *raw2 = DecodeBandData(band2, compressed, decompressed2);

            // Reserve space in result list
            ListVector::Reserve(result, total_list_size + num_pixels);
            child_data = FlatVector::GetData<double>(ListVector::GetEntry(result));

            // Compute normalized difference for each pixel
            for (size_t p = 0; p < num_pixels; p++) {
                double val1 = raquet::get_pixel_value(raw1, p, band1_dtype);
                double val2 = raquet::get_pixel_value(raw2, p, band2_dtype);

                double sum = val1 + val2;
                double nd;
                if (sum == 0.0) {
                    nd = 0.0;  // Avoid division by zero
                } else {
                    nd = (val1 - val2) / sum;
                }

                child_data[total_list_size + p] = nd;
            }

            list_data[i].offset = total_list_size;
            list_data[i].length = num_pixels;
            total_list_size += num_pixels;

        } catch (const std::exception &e) {
            list_data[i].offset = total_list_size;
            list_data[i].length = 0;
            FlatVector::Validity(result).SetInvalid(i);
        }
    }

    ListVector::SetListSize(result, total_list_size);
}

// ============================================================================
// ST_NormalizedDifference with nodata handling
// ST_NormalizedDifference(band1, band2, metadata, nodata) -> DOUBLE[]
// ============================================================================

static void STNormalizedDifferenceNodataFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    args.data[0].Flatten(args.size());
    args.data[1].Flatten(args.size());
    args.data[2].Flatten(args.size());
    args.data[3].Flatten(args.size());

    auto band1_data = FlatVector::GetData<string_t>(args.data[0]);
    auto band2_data = FlatVector::GetData<string_t>(args.data[1]);
    auto metadata_data = FlatVector::GetData<string_t>(args.data[2]);
    auto nodata_data = FlatVector::GetData<double>(args.data[3]);

    auto &band1_validity = FlatVector::Validity(args.data[0]);
    auto &band2_validity = FlatVector::Validity(args.data[1]);

    auto list_data = ListVector::GetData(result);
    auto &list_child = ListVector::GetEntry(result);
    auto child_data = FlatVector::GetData<double>(list_child);

    idx_t total_list_size = 0;

    for (idx_t i = 0; i < args.size(); i++) {
        if (!band1_validity.RowIsValid(i) || !band2_validity.RowIsValid(i)) {
            list_data[i].offset = total_list_size;
            list_data[i].length = 0;
            FlatVector::Validity(result).SetInvalid(i);
            continue;
        }

        auto band1 = band1_data[i];
        auto band2 = band2_data[i];
        auto metadata_str = metadata_data[i].GetString();
        double nodata = nodata_data[i];

        if (band1.GetSize() == 0 || band2.GetSize() == 0) {
            list_data[i].offset = total_list_size;
            list_data[i].length = 0;
            FlatVector::Validity(result).SetInvalid(i);
            continue;
        }

        try {
            auto meta = raquet::parse_metadata(metadata_str);
            bool compressed = (meta.compression == "gzip");
            int width = meta.block_width;
            int height = meta.block_height;
            size_t num_pixels = static_cast<size_t>(width) * height;

            std::string dtype1 = meta.bands.size() > 0 ? meta.bands[0].second : "float32";
            std::string dtype2 = meta.bands.size() > 1 ? meta.bands[1].second : dtype1;

            auto band1_dtype = raquet::parse_dtype(dtype1);
            auto band2_dtype = raquet::parse_dtype(dtype2);

            std::vector<uint8_t> decompressed1, decompressed2;
            const uint8_t *raw1 = DecodeBandData(band1, compressed, decompressed1);
            const uint8_t *raw2 = DecodeBandData(band2, compressed, decompressed2);

            ListVector::Reserve(result, total_list_size + num_pixels);
            child_data = FlatVector::GetData<double>(ListVector::GetEntry(result));

            for (size_t p = 0; p < num_pixels; p++) {
                double val1 = raquet::get_pixel_value(raw1, p, band1_dtype);
                double val2 = raquet::get_pixel_value(raw2, p, band2_dtype);

                // Check for nodata
                if (val1 == nodata || val2 == nodata) {
                    child_data[total_list_size + p] = nodata;
                    continue;
                }

                double sum = val1 + val2;
                double nd;
                if (sum == 0.0) {
                    nd = 0.0;
                } else {
                    nd = (val1 - val2) / sum;
                }

                child_data[total_list_size + p] = nd;
            }

            list_data[i].offset = total_list_size;
            list_data[i].length = num_pixels;
            total_list_size += num_pixels;

        } catch (const std::exception &e) {
            list_data[i].offset = total_list_size;
            list_data[i].length = 0;
            FlatVector::Validity(result).SetInvalid(i);
        }
    }

    ListVector::SetListSize(result, total_list_size);
}

// ============================================================================
// ST_BandMath(band1, band2, operation, metadata) -> DOUBLE[]
// Generic band math operations: 'add', 'subtract', 'multiply', 'divide', 'ndiff'
// ============================================================================

static void STBandMathFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    args.data[0].Flatten(args.size());
    args.data[1].Flatten(args.size());
    args.data[2].Flatten(args.size());
    args.data[3].Flatten(args.size());

    auto band1_data = FlatVector::GetData<string_t>(args.data[0]);
    auto band2_data = FlatVector::GetData<string_t>(args.data[1]);
    auto op_data = FlatVector::GetData<string_t>(args.data[2]);
    auto metadata_data = FlatVector::GetData<string_t>(args.data[3]);

    auto &band1_validity = FlatVector::Validity(args.data[0]);
    auto &band2_validity = FlatVector::Validity(args.data[1]);

    auto list_data = ListVector::GetData(result);
    auto &list_child = ListVector::GetEntry(result);
    auto child_data = FlatVector::GetData<double>(list_child);

    idx_t total_list_size = 0;

    for (idx_t i = 0; i < args.size(); i++) {
        if (!band1_validity.RowIsValid(i) || !band2_validity.RowIsValid(i)) {
            list_data[i].offset = total_list_size;
            list_data[i].length = 0;
            FlatVector::Validity(result).SetInvalid(i);
            continue;
        }

        auto band1 = band1_data[i];
        auto band2 = band2_data[i];
        auto op = op_data[i].GetString();
        auto metadata_str = metadata_data[i].GetString();

        if (band1.GetSize() == 0 || band2.GetSize() == 0) {
            list_data[i].offset = total_list_size;
            list_data[i].length = 0;
            FlatVector::Validity(result).SetInvalid(i);
            continue;
        }

        try {
            auto meta = raquet::parse_metadata(metadata_str);
            bool compressed = (meta.compression == "gzip");
            int width = meta.block_width;
            int height = meta.block_height;
            size_t num_pixels = static_cast<size_t>(width) * height;

            std::string dtype1 = meta.bands.size() > 0 ? meta.bands[0].second : "float32";
            std::string dtype2 = meta.bands.size() > 1 ? meta.bands[1].second : dtype1;

            auto band1_dtype = raquet::parse_dtype(dtype1);
            auto band2_dtype = raquet::parse_dtype(dtype2);

            std::vector<uint8_t> decompressed1, decompressed2;
            const uint8_t *raw1 = DecodeBandData(band1, compressed, decompressed1);
            const uint8_t *raw2 = DecodeBandData(band2, compressed, decompressed2);

            ListVector::Reserve(result, total_list_size + num_pixels);
            child_data = FlatVector::GetData<double>(ListVector::GetEntry(result));

            // Determine operation
            enum class Op { ADD, SUBTRACT, MULTIPLY, DIVIDE, NDIFF };
            Op operation;
            if (op == "add" || op == "+") {
                operation = Op::ADD;
            } else if (op == "subtract" || op == "-") {
                operation = Op::SUBTRACT;
            } else if (op == "multiply" || op == "*") {
                operation = Op::MULTIPLY;
            } else if (op == "divide" || op == "/") {
                operation = Op::DIVIDE;
            } else if (op == "ndiff" || op == "normalized_difference") {
                operation = Op::NDIFF;
            } else {
                throw InvalidInputException("ST_BandMath: Unknown operation '%s'. Use: add, subtract, multiply, divide, ndiff", op.c_str());
            }

            for (size_t p = 0; p < num_pixels; p++) {
                double val1 = raquet::get_pixel_value(raw1, p, band1_dtype);
                double val2 = raquet::get_pixel_value(raw2, p, band2_dtype);

                double res;
                switch (operation) {
                    case Op::ADD:
                        res = val1 + val2;
                        break;
                    case Op::SUBTRACT:
                        res = val1 - val2;
                        break;
                    case Op::MULTIPLY:
                        res = val1 * val2;
                        break;
                    case Op::DIVIDE:
                        res = (val2 != 0.0) ? val1 / val2 : std::numeric_limits<double>::quiet_NaN();
                        break;
                    case Op::NDIFF:
                        res = (val1 + val2 != 0.0) ? (val1 - val2) / (val1 + val2) : 0.0;
                        break;
                }

                child_data[total_list_size + p] = res;
            }

            list_data[i].offset = total_list_size;
            list_data[i].length = num_pixels;
            total_list_size += num_pixels;

        } catch (const std::exception &e) {
            throw InvalidInputException("ST_BandMath error: %s", e.what());
        }
    }

    ListVector::SetListSize(result, total_list_size);
}

// ============================================================================
// ST_NormalizedDifferenceStats - Get statistics of normalized difference directly
// Returns STRUCT(count, sum, mean, min, max, stddev) without materializing the array
// ============================================================================

static void STNormalizedDifferenceStatsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    args.data[0].Flatten(args.size());
    args.data[1].Flatten(args.size());
    args.data[2].Flatten(args.size());

    auto band1_data = FlatVector::GetData<string_t>(args.data[0]);
    auto band2_data = FlatVector::GetData<string_t>(args.data[1]);
    auto metadata_data = FlatVector::GetData<string_t>(args.data[2]);

    auto &band1_validity = FlatVector::Validity(args.data[0]);
    auto &band2_validity = FlatVector::Validity(args.data[1]);

    auto &struct_entries = StructVector::GetEntries(result);
    auto count_data = FlatVector::GetData<int64_t>(*struct_entries[0]);
    auto sum_data = FlatVector::GetData<double>(*struct_entries[1]);
    auto mean_data = FlatVector::GetData<double>(*struct_entries[2]);
    auto min_data = FlatVector::GetData<double>(*struct_entries[3]);
    auto max_data = FlatVector::GetData<double>(*struct_entries[4]);
    auto stddev_data = FlatVector::GetData<double>(*struct_entries[5]);

    auto &result_validity = FlatVector::Validity(result);

    for (idx_t i = 0; i < args.size(); i++) {
        if (!band1_validity.RowIsValid(i) || !band2_validity.RowIsValid(i)) {
            result_validity.SetInvalid(i);
            continue;
        }

        auto band1 = band1_data[i];
        auto band2 = band2_data[i];
        auto metadata_str = metadata_data[i].GetString();

        if (band1.GetSize() == 0 || band2.GetSize() == 0) {
            result_validity.SetInvalid(i);
            continue;
        }

        try {
            auto meta = raquet::parse_metadata(metadata_str);
            bool compressed = (meta.compression == "gzip");
            int width = meta.block_width;
            int height = meta.block_height;
            size_t num_pixels = static_cast<size_t>(width) * height;

            std::string dtype1 = meta.bands.size() > 0 ? meta.bands[0].second : "float32";
            std::string dtype2 = meta.bands.size() > 1 ? meta.bands[1].second : dtype1;

            auto band1_dtype = raquet::parse_dtype(dtype1);
            auto band2_dtype = raquet::parse_dtype(dtype2);

            std::vector<uint8_t> decompressed1, decompressed2;
            const uint8_t *raw1 = DecodeBandData(band1, compressed, decompressed1);
            const uint8_t *raw2 = DecodeBandData(band2, compressed, decompressed2);

            // Streaming statistics using Welford's algorithm
            int64_t count = 0;
            double sum = 0.0;
            double mean = 0.0;
            double m2 = 0.0;
            double min_val = std::numeric_limits<double>::max();
            double max_val = std::numeric_limits<double>::lowest();

            for (size_t p = 0; p < num_pixels; p++) {
                double val1 = raquet::get_pixel_value(raw1, p, band1_dtype);
                double val2 = raquet::get_pixel_value(raw2, p, band2_dtype);

                double s = val1 + val2;
                double nd = (s != 0.0) ? (val1 - val2) / s : 0.0;

                count++;
                sum += nd;

                if (nd < min_val) min_val = nd;
                if (nd > max_val) max_val = nd;

                // Welford's update
                double delta = nd - mean;
                mean += delta / count;
                double delta2 = nd - mean;
                m2 += delta * delta2;
            }

            count_data[i] = count;
            sum_data[i] = sum;
            mean_data[i] = (count > 0) ? sum / count : 0.0;
            min_data[i] = min_val;
            max_data[i] = max_val;
            stddev_data[i] = (count > 1) ? std::sqrt(m2 / (count - 1)) : 0.0;

        } catch (const std::exception &e) {
            result_validity.SetInvalid(i);
        }
    }
}

// ============================================================================
// Function Registration
// ============================================================================

void RegisterBandMathFunctions(ExtensionLoader &loader) {
    // Define the stats struct type
    child_list_t<LogicalType> stats_struct;
    stats_struct.push_back(make_pair("count", LogicalType::BIGINT));
    stats_struct.push_back(make_pair("sum", LogicalType::DOUBLE));
    stats_struct.push_back(make_pair("mean", LogicalType::DOUBLE));
    stats_struct.push_back(make_pair("min", LogicalType::DOUBLE));
    stats_struct.push_back(make_pair("max", LogicalType::DOUBLE));
    stats_struct.push_back(make_pair("stddev", LogicalType::DOUBLE));
    auto stats_type = LogicalType::STRUCT(stats_struct);

    // ST_NormalizedDifference(band1 BLOB, band2 BLOB, metadata VARCHAR) -> DOUBLE[]
    ScalarFunction nd_fn("ST_NormalizedDifference",
        {LogicalType::BLOB, LogicalType::BLOB, LogicalType::VARCHAR},
        LogicalType::LIST(LogicalType::DOUBLE),
        STNormalizedDifferenceFunction);
    loader.RegisterFunction(nd_fn);

    // ST_NormalizedDifference(band1, band2, metadata, nodata) -> DOUBLE[]
    ScalarFunction nd_nodata_fn("ST_NormalizedDifference",
        {LogicalType::BLOB, LogicalType::BLOB, LogicalType::VARCHAR, LogicalType::DOUBLE},
        LogicalType::LIST(LogicalType::DOUBLE),
        STNormalizedDifferenceNodataFunction);
    loader.RegisterFunction(nd_nodata_fn);

    // ST_BandMath(band1, band2, operation, metadata) -> DOUBLE[]
    ScalarFunction bandmath_fn("ST_BandMath",
        {LogicalType::BLOB, LogicalType::BLOB, LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::LIST(LogicalType::DOUBLE),
        STBandMathFunction);
    loader.RegisterFunction(bandmath_fn);

    // ST_NormalizedDifferenceStats(band1, band2, metadata) -> STRUCT(...)
    ScalarFunction nd_stats_fn("ST_NormalizedDifferenceStats",
        {LogicalType::BLOB, LogicalType::BLOB, LogicalType::VARCHAR},
        stats_type,
        STNormalizedDifferenceStatsFunction);
    loader.RegisterFunction(nd_stats_fn);
}

} // namespace duckdb
