#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "raquet_metadata.hpp"
#include "band_decoder.hpp"
#include "quadbin.hpp"

namespace duckdb {

// ─────────────────────────────────────────────
// raquet_validate_metadata(json) → STRUCT(is_valid, errors, warnings, num_blocks, num_bands, zoom_range)
//
// Validates a raquet metadata JSON string
// ─────────────────────────────────────────────
static void RaquetValidateMetadataFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    auto count = args.size();

    auto &entries = StructVector::GetEntries(result);
    auto &is_valid_vec = *entries[0];
    auto &errors_vec = *entries[1];
    auto &warnings_vec = *entries[2];
    auto &num_blocks_vec = *entries[3];
    auto &num_bands_vec = *entries[4];
    auto &zoom_range_vec = *entries[5];

    for (idx_t i = 0; i < count; i++) {
        auto json_val = input.GetValue(i);
        if (json_val.IsNull()) {
            result.SetValue(i, Value());
            continue;
        }

        std::string json = json_val.GetValue<string>();
        std::vector<std::string> errors;
        std::vector<std::string> warnings;
        bool valid = true;

        // Parse metadata
        raquet::RaquetMetadata meta;
        try {
            meta = raquet::parse_metadata(json);
        } catch (...) {
            errors.push_back("Failed to parse metadata JSON");
            valid = false;
        }

        if (valid) {
            // Validate file_format
            if (meta.file_format != "raquet") {
                errors.push_back("file_format is '" + meta.file_format + "', expected 'raquet'");
                valid = false;
            }

            // Validate compression
            if (meta.compression != "gzip" && meta.compression != "jpeg" &&
                meta.compression != "webp" && meta.compression != "none" &&
                !meta.compression.empty()) {
                warnings.push_back("Unknown compression: " + meta.compression);
            }

            // Validate tiling
            if (meta.scheme != "quadbin" && !meta.scheme.empty()) {
                errors.push_back("Unsupported tiling scheme: " + meta.scheme);
                valid = false;
            }

            if (meta.max_zoom < meta.min_zoom) {
                errors.push_back("max_zoom (" + std::to_string(meta.max_zoom) +
                                 ") < min_zoom (" + std::to_string(meta.min_zoom) + ")");
                valid = false;
            }

            if (meta.max_zoom < 0 || meta.max_zoom > 26) {
                errors.push_back("max_zoom out of range [0, 26]: " + std::to_string(meta.max_zoom));
                valid = false;
            }

            if (meta.block_width != 256 && meta.block_width != 512 && meta.block_width != 1024) {
                warnings.push_back("Non-standard block_width: " + std::to_string(meta.block_width));
            }

            // Validate bands
            if (meta.bands.empty()) {
                errors.push_back("No bands defined in metadata");
                valid = false;
            }

            for (size_t b = 0; b < meta.bands.size(); b++) {
                auto &band = meta.bands[b];
                if (band.first.empty()) {
                    errors.push_back("Band " + std::to_string(b) + " has no name");
                    valid = false;
                }
                try {
                    raquet::parse_dtype(band.second);
                } catch (...) {
                    errors.push_back("Band '" + band.first + "' has unknown type: " + band.second);
                    valid = false;
                }
            }

            // Validate band_layout
            if (meta.band_layout != "sequential" && meta.band_layout != "interleaved") {
                warnings.push_back("Unknown band_layout: " + meta.band_layout);
            }

            // Validate CRS
            if (meta.crs != "EPSG:3857" && !meta.crs.empty()) {
                warnings.push_back("Non-standard CRS: " + meta.crs + " (expected EPSG:3857)");
            }
        }

        FlatVector::GetData<bool>(is_valid_vec)[i] = valid;

        // Build error/warning lists
        vector<Value> err_vals, warn_vals;
        for (auto &e : errors) err_vals.push_back(Value(e));
        for (auto &w : warnings) warn_vals.push_back(Value(w));

        errors_vec.SetValue(i, err_vals.empty()
            ? Value::LIST(LogicalType::VARCHAR, vector<Value>())
            : Value::LIST(LogicalType::VARCHAR, err_vals));
        warnings_vec.SetValue(i, warn_vals.empty()
            ? Value::LIST(LogicalType::VARCHAR, vector<Value>())
            : Value::LIST(LogicalType::VARCHAR, warn_vals));

        FlatVector::GetData<int32_t>(num_blocks_vec)[i] = meta.num_blocks;
        FlatVector::GetData<int32_t>(num_bands_vec)[i] = static_cast<int>(meta.bands.size());
        zoom_range_vec.SetValue(i, Value(std::to_string(meta.min_zoom) + "-" + std::to_string(meta.max_zoom)));
    }
}

void RegisterMetadataFunctions(ExtensionLoader &loader) {
    // Return type for validate functions
    child_list_t<LogicalType> validate_fields;
    validate_fields.push_back({"is_valid", LogicalType::BOOLEAN});
    validate_fields.push_back({"errors", LogicalType::LIST(LogicalType::VARCHAR)});
    validate_fields.push_back({"warnings", LogicalType::LIST(LogicalType::VARCHAR)});
    validate_fields.push_back({"num_blocks", LogicalType::INTEGER});
    validate_fields.push_back({"num_bands", LogicalType::INTEGER});
    validate_fields.push_back({"zoom_range", LogicalType::VARCHAR});
    auto validate_type = LogicalType::STRUCT(validate_fields);

    // raquet_validate_metadata(json VARCHAR) → STRUCT
    ScalarFunction validate_meta_fn("raquet_validate_metadata", {LogicalType::VARCHAR},
                                     validate_type, RaquetValidateMetadataFunction);
    loader.RegisterFunction(validate_meta_fn);
}

} // namespace duckdb
