#include "merge_bands.hpp"
#include "raquet_metadata.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/materialized_query_result.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {

// SQL single-quote escape — apostrophes inside the path are doubled.
static std::string SqlSingleQuote(const std::string &s) {
    std::string out;
    out.reserve(s.size() + 2);
    out += '\'';
    for (char c : s) {
        if (c == '\'') out += "''";
        else out += c;
    }
    out += '\'';
    return out;
}

// ─────────────────────────────────────────────
// Read each input's metadata row via an internal Connection.
// Returns the raw JSON strings; the caller parses them via parse_metadata.
// ─────────────────────────────────────────────
static std::vector<std::string> ReadMetadataJsons(ClientContext &context,
                                                   const std::vector<std::string> &paths) {
    std::vector<std::string> jsons;
    jsons.reserve(paths.size());

    Connection con(*context.db);
    for (const auto &p : paths) {
        std::string q = "SELECT metadata FROM read_parquet(" + SqlSingleQuote(p) +
                        ") WHERE block = 0";
        auto result = con.Query(q);
        if (result->HasError()) {
            throw InvalidInputException(
                "raquet_merge_bands: failed to read metadata from '%s': %s",
                p, result->GetError());
        }
        auto chunk = result->Fetch();
        if (!chunk || chunk->size() == 0) {
            throw InvalidInputException(
                "raquet_merge_bands: '%s' has no metadata row (block=0)", p);
        }
        Value v = chunk->GetValue(0, 0);
        if (v.IsNull()) {
            throw InvalidInputException(
                "raquet_merge_bands: '%s' has NULL metadata at block=0", p);
        }
        jsons.push_back(v.GetValue<std::string>());
    }
    return jsons;
}

// Detect output format from the input's raw JSON: v0.5.0+ outputs nest tile
// geometry under "tiling": { ... }; v0.1.0 outputs it flat at top level.
// Preserving the input format keeps the merged file legible by the same
// readers that handled the inputs.
static bool IsV05Format(const std::string &json) {
    return json.find("\"tiling\"") != std::string::npos;
}

// ─────────────────────────────────────────────
// Validate input compatibility and build the merged RaquetMetadata.
// Each input must be a single-band raquet (the only shape `bands='N'` in
// read_raster produces). All inputs must share spatial frame and band[0]
// type. Spatial-frame fields are compared as parsed struct values, not as
// raw JSON strings — so two inputs whose JSON differs only in formatting
// (whitespace, key order) still compare equal.
//
// Returns the merged struct with num_blocks left as input 0's value;
// caller must overwrite via CountMergedBlocks before serialising.
// ─────────────────────────────────────────────
static raquet::RaquetMetadata BuildMergedMetadata(
        const std::vector<std::string> &paths,
        const std::vector<raquet::RaquetMetadata> &metas) {

    for (size_t i = 0; i < metas.size(); i++) {
        if (metas[i].band_info.size() != 1) {
            throw InvalidInputException(
                "raquet_merge_bands: input '%s' has %d bands; only single-band "
                "inputs are supported (use read_raster(..., bands='N') to produce them)",
                paths[i], static_cast<int>(metas[i].band_info.size()));
        }
    }

    auto fail_str = [&](const std::string &name, const std::string &a,
                         const std::string &b, size_t i) {
        throw InvalidInputException(
            "raquet_merge_bands: '%s' differs between '%s' ('%s') and '%s' ('%s') — "
            "all inputs must share the same spatial frame",
            name, paths[0], a, paths[i], b);
    };
    auto fail_int = [&](const std::string &name, int a, int b, size_t i) {
        throw InvalidInputException(
            "raquet_merge_bands: '%s' differs between '%s' (%d) and '%s' (%d) — "
            "all inputs must share the same spatial frame",
            name, paths[0], a, paths[i], b);
    };
    auto fail_dbl = [&](const std::string &name, double a, double b, size_t i) {
        throw InvalidInputException(
            "raquet_merge_bands: '%s' differs between '%s' (%g) and '%s' (%g) — "
            "all inputs must share the same spatial frame",
            name, paths[0], a, paths[i], b);
    };

    const auto &m0 = metas[0];
    for (size_t i = 1; i < metas.size(); i++) {
        const auto &mi = metas[i];
        if (m0.compression  != mi.compression)  fail_str("compression",  m0.compression,  mi.compression,  i);
        if (m0.crs          != mi.crs)          fail_str("crs",          m0.crs,          mi.crs,          i);
        if (m0.tile_matrix_set != mi.tile_matrix_set)
            fail_str("tile_matrix_set", m0.tile_matrix_set, mi.tile_matrix_set, i);
        if (m0.block_width  != mi.block_width)  fail_int("block_width",  m0.block_width,  mi.block_width,  i);
        if (m0.block_height != mi.block_height) fail_int("block_height", m0.block_height, mi.block_height, i);
        if (m0.min_zoom     != mi.min_zoom)     fail_int("min_zoom",     m0.min_zoom,     mi.min_zoom,     i);
        if (m0.max_zoom     != mi.max_zoom)     fail_int("max_zoom",     m0.max_zoom,     mi.max_zoom,     i);
        if (m0.width        != mi.width)        fail_int("width",        m0.width,        mi.width,        i);
        if (m0.height       != mi.height)       fail_int("height",       m0.height,       mi.height,       i);
        if (m0.bounds_minlon != mi.bounds_minlon) fail_dbl("bounds[0]", m0.bounds_minlon, mi.bounds_minlon, i);
        if (m0.bounds_minlat != mi.bounds_minlat) fail_dbl("bounds[1]", m0.bounds_minlat, mi.bounds_minlat, i);
        if (m0.bounds_maxlon != mi.bounds_maxlon) fail_dbl("bounds[2]", m0.bounds_maxlon, mi.bounds_maxlon, i);
        if (m0.bounds_maxlat != mi.bounds_maxlat) fail_dbl("bounds[3]", m0.bounds_maxlat, mi.bounds_maxlat, i);
        if (m0.band_info[0].type != mi.band_info[0].type) {
            fail_str("bands[0].type", m0.band_info[0].type, mi.band_info[0].type, i);
        }
    }

    // Compose: start from input 0 (carries all the validated common fields),
    // replace the bands list with one entry per input. Each band entry is
    // renumbered to its output position (name = "band_{i+1}", source_band =
    // i+1) so downstream readers can map output columns back to source files.
    raquet::RaquetMetadata merged = m0;
    merged.band_info.clear();
    merged.bands.clear();
    for (size_t i = 0; i < metas.size(); i++) {
        raquet::BandInfo bi = metas[i].band_info[0];
        bi.name = "band_" + std::to_string(i + 1);
        bi.source_band = static_cast<int>(i + 1);
        merged.band_info.push_back(bi);
        merged.bands.push_back({bi.name, bi.type});
    }
    return merged;
}

// Count distinct block IDs in the union of all inputs' data rows.
// `num_blocks` in the merged metadata can't be inferred from any single
// input — must be computed via a SQL union-dedup over all inputs.
static int64_t CountMergedBlocks(ClientContext &context,
                                  const std::vector<std::string> &paths) {
    if (paths.empty()) return 0;

    std::stringstream sql;
    sql << "SELECT COUNT(*) FROM (";
    for (size_t i = 0; i < paths.size(); i++) {
        if (i > 0) sql << " UNION ";
        sql << "SELECT block FROM read_parquet(" << SqlSingleQuote(paths[i])
            << ") WHERE block != 0";
    }
    sql << ")";

    Connection con(*context.db);
    auto result = con.Query(sql.str());
    if (result->HasError()) {
        throw InvalidInputException(
            "raquet_merge_bands: failed to count merged blocks: %s",
            result->GetError());
    }
    auto chunk = result->Fetch();
    if (!chunk || chunk->size() == 0) return 0;
    return chunk->GetValue(0, 0).GetValue<int64_t>();
}

// Build the FULL OUTER JOIN of N input parquets on `block`. Each input
// contributes its `band_1` column under output name `band_{i+1}`.
static std::string BuildJoinSQL(const std::vector<std::string> &paths) {
    std::stringstream sql;
    sql << "SELECT ";
    sql << "COALESCE(";
    for (size_t i = 0; i < paths.size(); i++) {
        if (i > 0) sql << ", ";
        sql << "b" << (i + 1) << ".block";
    }
    sql << ") AS block, NULL::VARCHAR AS metadata";
    for (size_t i = 0; i < paths.size(); i++) {
        sql << ", b" << (i + 1) << ".band_1 AS band_" << (i + 1);
    }
    sql << " FROM read_parquet(" << SqlSingleQuote(paths[0]) << ") b1";
    for (size_t i = 1; i < paths.size(); i++) {
        sql << " FULL OUTER JOIN read_parquet(" << SqlSingleQuote(paths[i])
            << ") b" << (i + 1) << " USING (block)";
    }
    sql << " WHERE COALESCE(";
    for (size_t i = 0; i < paths.size(); i++) {
        if (i > 0) sql << ", ";
        sql << "b" << (i + 1) << ".block";
    }
    sql << ") != 0";
    sql << " ORDER BY block";
    return sql.str();
}

// ─────────────────────────────────────────────
// Bind state
// ─────────────────────────────────────────────
struct RaquetMergeBandsBindData : public TableFunctionData {
    std::vector<std::string> input_paths;
    std::string merged_metadata;
    std::string join_sql;
};

struct RaquetMergeBandsGlobalState : public GlobalTableFunctionState {
    // Connection must outlive `result`; `result` must outlive
    // `current_chunk`. Field-declaration order encodes this.
    std::unique_ptr<Connection> connection;
    duckdb::unique_ptr<QueryResult> result;
    duckdb::unique_ptr<DataChunk> current_chunk;
    idx_t current_row = 0;
    bool metadata_emitted = false;
    bool finished = false;

    idx_t MaxThreads() const override { return 1; }
};

// ─────────────────────────────────────────────
// Bind: read each input's metadata row, validate compatibility, compose
// the merged metadata, recompute num_blocks, build the data-join SQL,
// publish the output schema.
// ─────────────────────────────────────────────
static unique_ptr<FunctionData> RaquetMergeBandsBind(
        ClientContext &context, TableFunctionBindInput &input,
        vector<LogicalType> &return_types, vector<string> &names) {

    if (input.inputs.empty()) {
        throw InvalidInputException("raquet_merge_bands: missing input list");
    }
    auto &list_value = input.inputs[0];
    if (list_value.IsNull()) {
        throw InvalidInputException("raquet_merge_bands: input list cannot be NULL");
    }

    auto &paths_values = ListValue::GetChildren(list_value);
    if (paths_values.empty()) {
        throw InvalidInputException("raquet_merge_bands: input list is empty");
    }

    auto bind_data = make_uniq<RaquetMergeBandsBindData>();
    bind_data->input_paths.reserve(paths_values.size());
    for (const auto &v : paths_values) {
        if (v.IsNull()) {
            throw InvalidInputException("raquet_merge_bands: input list contains NULL paths");
        }
        bind_data->input_paths.push_back(v.GetValue<std::string>());
    }

    auto jsons = ReadMetadataJsons(context, bind_data->input_paths);

    std::vector<raquet::RaquetMetadata> metas;
    metas.reserve(jsons.size());
    for (const auto &j : jsons) {
        metas.push_back(raquet::parse_metadata(j));
    }

    auto merged = BuildMergedMetadata(bind_data->input_paths, metas);
    merged.num_blocks = static_cast<int>(
        CountMergedBlocks(context, bind_data->input_paths));

    // Preserve the input format. Mixed-format inputs would have failed
    // validation already (different shapes parse to different structs);
    // we sniff input 0's raw JSON here only to decide which serializer
    // to call.
    bind_data->merged_metadata = IsV05Format(jsons[0])
        ? merged.to_json() : merged.to_json_v0();

    bind_data->join_sql = BuildJoinSQL(bind_data->input_paths);

    names.push_back("block");      return_types.push_back(LogicalType::UBIGINT);
    names.push_back("metadata");   return_types.push_back(LogicalType::VARCHAR);
    for (size_t i = 0; i < bind_data->input_paths.size(); i++) {
        names.push_back("band_" + std::to_string(i + 1));
        return_types.push_back(LogicalType::BLOB);
    }
    return std::move(bind_data);
}

// ─────────────────────────────────────────────
// InitGlobal: open an internal Connection and submit the join SQL.
// `Connection::Query` returns a MaterializedQueryResult — the entire
// joined result-set is buffered in memory before Execute pulls chunks.
// This is acceptable for the documented workflow (the per-band → merge
// pattern exists to dodge the parquet writer's row-group buffering, not
// the join's working set).
// ─────────────────────────────────────────────
static unique_ptr<GlobalTableFunctionState> RaquetMergeBandsInitGlobal(
        ClientContext &context, TableFunctionInitInput &input) {
    auto &bind = input.bind_data->Cast<RaquetMergeBandsBindData>();
    auto state = make_uniq<RaquetMergeBandsGlobalState>();

    state->connection = std::unique_ptr<Connection>(new Connection(*context.db));
    state->result = state->connection->Query(bind.join_sql);
    if (state->result->HasError()) {
        throw InvalidInputException(
            "raquet_merge_bands: data-join query failed: %s",
            state->result->GetError());
    }
    return std::move(state);
}

// ─────────────────────────────────────────────
// Execute: emit the metadata row first (block=0, metadata=merged JSON,
// band_* = NULL), then drain the join result chunk by chunk.
// ─────────────────────────────────────────────
static void RaquetMergeBandsExecute(ClientContext &context, TableFunctionInput &input,
                                     DataChunk &output) {
    auto &bind = input.bind_data->Cast<RaquetMergeBandsBindData>();
    auto &state = input.global_state->Cast<RaquetMergeBandsGlobalState>();

    if (state.finished) {
        output.SetCardinality(0);
        return;
    }

    idx_t out_row = 0;
    idx_t max_rows = STANDARD_VECTOR_SIZE;
    idx_t band_count = bind.input_paths.size();

    if (!state.metadata_emitted) {
        FlatVector::GetData<uint64_t>(output.data[0])[out_row] = 0;
        FlatVector::GetData<string_t>(output.data[1])[out_row] =
            StringVector::AddString(output.data[1], bind.merged_metadata);
        for (idx_t i = 0; i < band_count; i++) {
            FlatVector::SetNull(output.data[2 + i], out_row, true);
        }
        out_row++;
        state.metadata_emitted = true;
    }

    while (out_row < max_rows) {
        if (!state.current_chunk || state.current_row >= state.current_chunk->size()) {
            state.current_chunk = state.result->Fetch();
            state.current_row = 0;
            if (!state.current_chunk || state.current_chunk->size() == 0) {
                state.finished = true;
                break;
            }
        }
        for (idx_t col = 0; col < band_count + 2; col++) {
            Value v = state.current_chunk->GetValue(col, state.current_row);
            output.SetValue(col, out_row, v);
        }
        out_row++;
        state.current_row++;
    }

    output.SetCardinality(out_row);
}

// Rough cardinality estimate (planner hint only). Uses input 0's row
// count; the FULL OUTER JOIN can return up to sum(rows[i]) when inputs
// share few blocks, but most merges are produced from rasters with the
// same spatial frame so input 0 is a reasonable lower bound.
static unique_ptr<NodeStatistics> RaquetMergeBandsCardinality(
        ClientContext &context, const FunctionData *bind_data_p) {
    auto &bind = bind_data_p->Cast<RaquetMergeBandsBindData>();
    if (bind.input_paths.empty()) {
        return make_uniq<NodeStatistics>();
    }
    Connection con(*context.db);
    auto q = "SELECT COUNT(*) FROM read_parquet(" +
             SqlSingleQuote(bind.input_paths[0]) + ") WHERE block != 0";
    auto result = con.Query(q);
    if (!result || result->HasError()) {
        return make_uniq<NodeStatistics>();
    }
    auto chunk = result->Fetch();
    if (!chunk || chunk->size() == 0) {
        return make_uniq<NodeStatistics>();
    }
    int64_t n = chunk->GetValue(0, 0).GetValue<int64_t>();
    return make_uniq<NodeStatistics>(static_cast<idx_t>(n + 1));
}

void RegisterMergeBandsFunction(ExtensionLoader &loader) {
    TableFunction merge_fn(
        "raquet_merge_bands",
        {LogicalType::LIST(LogicalType::VARCHAR)},
        RaquetMergeBandsExecute,
        RaquetMergeBandsBind,
        RaquetMergeBandsInitGlobal);
    merge_fn.cardinality = RaquetMergeBandsCardinality;
    loader.RegisterFunction(merge_fn);
}

}  // namespace duckdb
