#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/client_context.hpp"
#include "raquet_metadata.hpp"

namespace duckdb {

// ============================================================================
// NOTE: read_raquet() is implemented as a table macro in raquet_extension.cpp
// that wraps read_parquet(). Example usage:
//
//   -- Read a raquet file
//   SELECT * FROM read_raquet('file.raquet');
//
//   -- Filter by resolution level
//   SELECT * FROM read_raquet('file.raquet')
//   WHERE quadbin_resolution(block) = 10;
//
//   -- Get metadata
//   SELECT raquet_parse_metadata(value)
//   FROM parquet_kv_metadata('file.raquet')
//   WHERE key = 'raquet';
// ============================================================================

// ============================================================================
// raquet_parse_metadata - parse metadata JSON string into a struct
// ============================================================================

// raquet_parse_metadata(metadata_json VARCHAR) -> STRUCT
static void RaquetParseMetadataFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    args.data[0].Flatten(args.size());

    auto metadata_data = FlatVector::GetData<string_t>(args.data[0]);
    auto &result_mask = FlatVector::Validity(result);

    auto &struct_entries = StructVector::GetEntries(result);
    auto &compression_vec = *struct_entries[0];
    auto &compression_quality_vec = *struct_entries[1];  // v0.4.0
    auto &band_layout_vec = *struct_entries[2];          // v0.4.0
    auto &block_width_vec = *struct_entries[3];
    auto &block_height_vec = *struct_entries[4];
    auto &min_zoom_vec = *struct_entries[5];
    auto &max_zoom_vec = *struct_entries[6];
    auto &num_bands_vec = *struct_entries[7];

    for (idx_t i = 0; i < args.size(); i++) {
        auto metadata_str = metadata_data[i].GetString();

        if (metadata_str.empty()) {
            result_mask.SetInvalid(i);
            continue;
        }

        try {
            auto meta = raquet::parse_metadata(metadata_str);

            FlatVector::GetData<string_t>(compression_vec)[i] = StringVector::AddString(compression_vec, meta.compression);
            FlatVector::GetData<int32_t>(compression_quality_vec)[i] = meta.compression_quality;
            FlatVector::GetData<string_t>(band_layout_vec)[i] = StringVector::AddString(band_layout_vec, meta.band_layout);
            FlatVector::GetData<int32_t>(block_width_vec)[i] = meta.block_width;
            FlatVector::GetData<int32_t>(block_height_vec)[i] = meta.block_height;
            FlatVector::GetData<int32_t>(min_zoom_vec)[i] = meta.min_zoom;
            FlatVector::GetData<int32_t>(max_zoom_vec)[i] = meta.max_zoom;
            FlatVector::GetData<int32_t>(num_bands_vec)[i] = static_cast<int32_t>(meta.bands.size());
        } catch (...) {
            result_mask.SetInvalid(i);
        }
    }

    result.SetVectorType(VectorType::FLAT_VECTOR);
}

void RegisterRaquetTableFunctions(ExtensionLoader &loader) {
    // Note: read_raquet() table macro is registered in raquet_extension.cpp

    // =========================================================================
    // raquet_metadata() - get metadata from a raquet file
    // =========================================================================
    // This is a simpler function that just returns the metadata

    // =========================================================================
    // Scalar helper functions
    // =========================================================================

    // raquet_parse_metadata(metadata) -> STRUCT (v0.4.0 format)
    child_list_t<LogicalType> meta_struct;
    meta_struct.push_back(make_pair("compression", LogicalType::VARCHAR));
    meta_struct.push_back(make_pair("compression_quality", LogicalType::INTEGER));  // v0.4.0
    meta_struct.push_back(make_pair("band_layout", LogicalType::VARCHAR));          // v0.4.0
    meta_struct.push_back(make_pair("block_width", LogicalType::INTEGER));
    meta_struct.push_back(make_pair("block_height", LogicalType::INTEGER));
    meta_struct.push_back(make_pair("min_zoom", LogicalType::INTEGER));
    meta_struct.push_back(make_pair("max_zoom", LogicalType::INTEGER));
    meta_struct.push_back(make_pair("num_bands", LogicalType::INTEGER));

    ScalarFunction parse_metadata_fn("raquet_parse_metadata",
        {LogicalType::VARCHAR},
        LogicalType::STRUCT(meta_struct),
        RaquetParseMetadataFunction);
    loader.RegisterFunction(parse_metadata_fn);

}

} // namespace duckdb
