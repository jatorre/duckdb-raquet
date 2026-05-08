#include "merge_bands.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/materialized_query_result.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {

// ─────────────────────────────────────────────
// Minimal JSON helpers — extract scalar fields and substrings from
// well-formed raquet metadata JSON. The metadata is produced by our own
// serializer (RaquetMetadata::to_json / to_json_v0) so we don't need a
// full RFC-8259 parser; just enough to find specific keys at known
// positions in the structure.
//
// All helpers assume the JSON is syntactically valid; they throw
// InvalidInputException on missing/malformed fields rather than
// silently returning defaults.
// ─────────────────────────────────────────────

// Skip whitespace starting at pos.
static size_t SkipWS(const std::string &s, size_t pos) {
    while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t' || s[pos] == '\n' || s[pos] == '\r')) {
        pos++;
    }
    return pos;
}

// Find the position of a top-level key inside a JSON object substring.
// Returns the position of the colon following "key", or std::string::npos
// if not found. Top level here means depth == 1 — the keys directly
// inside the outermost `{ ... }`. Handles nested objects/arrays
// correctly via brace counting (keys at deeper levels are skipped).
static size_t FindTopLevelKey(const std::string &json, size_t start, size_t end, const std::string &key) {
    int depth = 0;
    bool in_string = false;
    bool escape = false;
    std::string needle = "\"" + key + "\"";

    for (size_t i = start; i < end; i++) {
        char c = json[i];
        if (escape) { escape = false; continue; }
        if (in_string) {
            if (c == '\\') escape = true;
            else if (c == '"') in_string = false;
            continue;
        }
        if (c == '"') {
            // Check if this opens our needle at top level (one inside
            // the outermost {})
            if (depth == 1 && i + needle.size() <= end &&
                json.compare(i, needle.size(), needle) == 0) {
                size_t after = SkipWS(json, i + needle.size());
                if (after < end && json[after] == ':') {
                    return after;
                }
            }
            in_string = true;
            continue;
        }
        if (c == '{' || c == '[') depth++;
        else if (c == '}' || c == ']') depth--;
    }
    return std::string::npos;
}

// Read a JSON value (string, number, array, object, true/false/null)
// starting at pos. Returns the (start, end) range of the value (end is
// one past the last char). Throws on malformed input.
static std::pair<size_t, size_t> ReadJSONValue(const std::string &json, size_t pos, size_t end) {
    pos = SkipWS(json, pos);
    if (pos >= end) {
        throw InvalidInputException("raquet_merge_bands: unexpected end of metadata JSON");
    }
    size_t value_start = pos;
    char c = json[pos];

    if (c == '"') {
        // string
        pos++;
        bool escape = false;
        while (pos < end) {
            if (escape) { escape = false; pos++; continue; }
            if (json[pos] == '\\') { escape = true; pos++; continue; }
            if (json[pos] == '"') { pos++; return {value_start, pos}; }
            pos++;
        }
        throw InvalidInputException("raquet_merge_bands: unterminated string in metadata JSON");
    }
    if (c == '{' || c == '[') {
        char open = c;
        char close = (c == '{') ? '}' : ']';
        int depth = 1;
        pos++;
        bool in_string = false;
        bool escape = false;
        while (pos < end && depth > 0) {
            if (escape) { escape = false; pos++; continue; }
            if (in_string) {
                if (json[pos] == '\\') escape = true;
                else if (json[pos] == '"') in_string = false;
            } else {
                if (json[pos] == '"') in_string = true;
                else if (json[pos] == open) depth++;
                else if (json[pos] == close) depth--;
            }
            pos++;
        }
        if (depth != 0) {
            throw InvalidInputException("raquet_merge_bands: unbalanced brackets in metadata JSON");
        }
        return {value_start, pos};
    }
    // number / true / false / null — read until comma, closing bracket, or whitespace
    while (pos < end) {
        char ch = json[pos];
        if (ch == ',' || ch == '}' || ch == ']' || ch == ' ' ||
            ch == '\t' || ch == '\n' || ch == '\r') {
            return {value_start, pos};
        }
        pos++;
    }
    return {value_start, pos};
}

// Extract the raw JSON substring of a top-level field.
// Returns the value as it appears in the source (string with quotes,
// number as digits, object/array with braces/brackets, etc.).
// Throws on missing field.
static std::string ExtractRawValue(const std::string &json, const std::string &key) {
    size_t colon = FindTopLevelKey(json, 0, json.size(), key);
    if (colon == std::string::npos) {
        throw InvalidInputException("raquet_merge_bands: required field '%s' missing from metadata", key);
    }
    auto [start, end] = ReadJSONValue(json, colon + 1, json.size());
    return json.substr(start, end - start);
}

// Try to extract a top-level field; returns empty string if absent.
static std::string TryExtractRawValue(const std::string &json, const std::string &key) {
    size_t colon = FindTopLevelKey(json, 0, json.size(), key);
    if (colon == std::string::npos) {
        return "";
    }
    auto [start, end] = ReadJSONValue(json, colon + 1, json.size());
    return json.substr(start, end - start);
}

// Unquote a JSON string literal (assumes the value was extracted via
// ReadJSONValue and includes the surrounding quotes).
static std::string Unquote(const std::string &raw) {
    if (raw.size() < 2 || raw[0] != '"' || raw[raw.size() - 1] != '"') {
        return raw;  // not a string; return as-is
    }
    std::string out;
    out.reserve(raw.size() - 2);
    for (size_t i = 1; i < raw.size() - 1; i++) {
        if (raw[i] == '\\' && i + 1 < raw.size() - 1) {
            char nxt = raw[i + 1];
            if (nxt == '"') out += '"';
            else if (nxt == '\\') out += '\\';
            else if (nxt == '/') out += '/';
            else if (nxt == 'n') out += '\n';
            else if (nxt == 't') out += '\t';
            else if (nxt == 'r') out += '\r';
            else { out += '\\'; out += nxt; }
            i++;
        } else {
            out += raw[i];
        }
    }
    return out;
}

// Replace the value of a top-level JSON object field with `new_value`
// (which must be a complete, well-formed JSON value: a quoted string,
// number, array, or object). Returns the original JSON unchanged if the
// field is absent.
static std::string ReplaceTopLevelField(std::string json, const std::string &key,
                                          const std::string &new_value) {
    size_t colon = FindTopLevelKey(json, 0, json.size(), key);
    if (colon == std::string::npos) {
        return json;
    }
    auto [vs, ve] = ReadJSONValue(json, colon + 1, json.size());
    return json.substr(0, vs) + new_value + json.substr(ve);
}

// ─────────────────────────────────────────────
// Bind state
// ─────────────────────────────────────────────
struct RaquetMergeBandsBindData : public TableFunctionData {
    std::vector<std::string> input_paths;
    std::string merged_metadata;          // composed at bind time
    std::string join_sql;                 // SQL string executed in InitGlobal
    std::vector<std::string> column_names;
};

struct RaquetMergeBandsGlobalState : public GlobalTableFunctionState {
    std::unique_ptr<Connection> connection;
    duckdb::unique_ptr<QueryResult> result;
    duckdb::unique_ptr<DataChunk> current_chunk;
    idx_t current_row = 0;
    bool metadata_emitted = false;
    bool finished = false;

    idx_t MaxThreads() const override { return 1; }
};

// ─────────────────────────────────────────────
// Validation
// ─────────────────────────────────────────────
static void EnsureFieldEqual(const std::string &field,
                              const std::string &v0, const std::string &p0,
                              const std::string &v1, const std::string &p1) {
    if (v0 != v1) {
        throw InvalidInputException(
            "raquet_merge_bands: '%s' differs between '%s' (%s) and '%s' (%s) — "
            "all inputs must share the same spatial frame",
            field, p0, v0, p1, v1);
    }
}

// ─────────────────────────────────────────────
// SQL helpers
// ─────────────────────────────────────────────
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
// Read each input's metadata row via Connection
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

// ─────────────────────────────────────────────
// Compose merged metadata JSON from N inputs.
// Validates compatibility, then splices the bands arrays.
//
// Strategy: keep the first input's full metadata as a base, then:
//   1. Replace the top-level "bands" with a new array containing each
//      input's bands[0] (with name renumbered to band_{i+1}).
//   2. If "nodata" is present at top level as a scalar, replace with
//      array [nd1..ndN].
//
// All other fields (version, compression, dimensions, bounds, etc.) are
// kept from input 0 — they're validated to match across all inputs.
// ─────────────────────────────────────────────
static std::string MergeMetadata(const std::vector<std::string> &paths,
                                  const std::vector<std::string> &jsons) {
    if (jsons.empty()) {
        throw InvalidInputException("raquet_merge_bands: at least one input required");
    }

    // Validate: extract spatial-frame fields from each, compare to input 0.
    // Fields that must match exactly (as raw JSON values).
    static const std::vector<std::string> required_match = {
        "version", "compression", "block_width", "block_height",
        "block_resolution", "bounds", "width", "height"
    };
    // min_zoom/minresolution and max_zoom/maxresolution have version-dependent
    // names; check whichever is present.
    static const std::vector<std::pair<std::string, std::string>> alt_keys = {
        {"min_zoom", "minresolution"},
        {"max_zoom", "maxresolution"},
    };

    auto first = jsons[0];
    auto first_path = paths[0];

    for (size_t i = 1; i < jsons.size(); i++) {
        for (const auto &k : required_match) {
            std::string a = TryExtractRawValue(first, k);
            std::string b = TryExtractRawValue(jsons[i], k);
            if (a.empty() && b.empty()) continue;  // neither has it — fine
            EnsureFieldEqual(k, a, first_path, b, paths[i]);
        }
        for (const auto &kp : alt_keys) {
            std::string a = TryExtractRawValue(first, kp.first);
            if (a.empty()) a = TryExtractRawValue(first, kp.second);
            std::string b = TryExtractRawValue(jsons[i], kp.first);
            if (b.empty()) b = TryExtractRawValue(jsons[i], kp.second);
            if (a.empty() && b.empty()) continue;
            EnsureFieldEqual(kp.first, a, first_path, b, paths[i]);
        }
        // Compare band[0].type via a structural extract: the bands array's
        // first element's "type" field. Approximate via substring search:
        // both inputs are single-band so bands[0] is the only band.
        std::string bands_a = TryExtractRawValue(first, "bands");
        std::string bands_b = TryExtractRawValue(jsons[i], "bands");
        if (!bands_a.empty() && !bands_b.empty()) {
            // bands_X looks like [{...}] — look for "type":"..."
            auto find_type = [](const std::string &arr) -> std::string {
                size_t p = arr.find("\"type\"");
                if (p == std::string::npos) return "";
                p = arr.find(':', p);
                if (p == std::string::npos) return "";
                auto [vs, ve] = ReadJSONValue(arr, p + 1, arr.size());
                return arr.substr(vs, ve - vs);
            };
            std::string ta = find_type(bands_a);
            std::string tb = find_type(bands_b);
            if (!ta.empty() && !tb.empty()) {
                EnsureFieldEqual("bands[0].type", ta, first_path, tb, paths[i]);
            }
        }
    }

    // Compose merged JSON.
    // Step 1: extract each input's bands[0] (the single-band entry).
    std::vector<std::string> band_entries;
    for (const auto &j : jsons) {
        std::string bands = TryExtractRawValue(j, "bands");
        if (bands.empty() || bands.front() != '[') {
            throw InvalidInputException(
                "raquet_merge_bands: input metadata missing or malformed 'bands' array");
        }
        // Extract first array element.
        size_t pos = SkipWS(bands, 1);  // skip '['
        if (pos >= bands.size() - 1 || bands[pos] == ']') {
            throw InvalidInputException(
                "raquet_merge_bands: input metadata has empty 'bands' array");
        }
        auto [vs, ve] = ReadJSONValue(bands, pos, bands.size() - 1);
        band_entries.push_back(bands.substr(vs, ve - vs));
    }

    // Step 2: renumber each band's "name" AND "source_band" to its
    // position in the merged raster (1..N). The output is a new
    // self-contained multi-band raquet — the band at output position
    // i+1 IS source band i+1 of THIS raster (regardless of which
    // input file it came from). Leaving source_band as the original
    // source-band index from the input parquet would confuse readers
    // that use source_band to determine the default band to render
    // (CARTO Builder defaults to source_band=1, which won't match the
    // intended output column 1 if we don't renumber).
    for (size_t i = 0; i < band_entries.size(); i++) {
        std::string &b = band_entries[i];
        size_t name_colon = FindTopLevelKey(b, 0, b.size(), "name");
        if (name_colon == std::string::npos) {
            throw InvalidInputException(
                "raquet_merge_bands: band entry missing 'name' field");
        }
        auto [vs, ve] = ReadJSONValue(b, name_colon + 1, b.size());
        std::string new_name = "\"band_" + std::to_string(i + 1) + "\"";
        b = b.substr(0, vs) + new_name + b.substr(ve);

        // Also replace source_band if present (the per-band source-band
        // tracking from the bands= filter feature in PR #7).
        size_t src_colon = FindTopLevelKey(b, 0, b.size(), "source_band");
        if (src_colon != std::string::npos) {
            auto [svs, sve] = ReadJSONValue(b, src_colon + 1, b.size());
            b = b.substr(0, svs) + std::to_string(i + 1) + b.substr(sve);
        }
    }

    // Step 3: build the top-level nodata array.
    //
    // Use each input's TOP-LEVEL `nodata` field (scalar number in
    // single-band raquet output — produced by `nodata_to_json`). Don't
    // use bands[i].nodata, which raquet's v0 format quotes as a string
    // ("255") for raster_loader compatibility — that's the per-band
    // shape, not the top-level shape, and putting quoted strings in
    // the top-level array confuses readers that expect numbers.
    std::vector<std::string> per_band_nodatas;
    for (const auto &j : jsons) {
        std::string nd = TryExtractRawValue(j, "nodata");
        if (nd.empty()) {
            per_band_nodatas.push_back("null");
        } else if (!nd.empty() && nd.front() == '[') {
            // Input was already multi-band (unusual for the merge use
            // case but supported): extract its first element.
            size_t pos = SkipWS(nd, 1);
            if (pos < nd.size() - 1 && nd[pos] != ']') {
                auto [vs, ve] = ReadJSONValue(nd, pos, nd.size() - 1);
                per_band_nodatas.push_back(nd.substr(vs, ve - vs));
            } else {
                per_band_nodatas.push_back("null");
            }
        } else {
            // Scalar number form — use directly.
            per_band_nodatas.push_back(nd);
        }
    }

    // Step 4: rebuild the merged metadata. Take input 0's JSON, then:
    //   - replace top-level "bands" with the renumbered array
    //   - replace top-level "nodata" (if scalar) with the array form
    //
    // We do this with substring replacement: find each field's
    // (key+value) span and splice in the new value.
    auto replace_field = [](std::string json, const std::string &key,
                             const std::string &new_value) -> std::string {
        size_t colon = FindTopLevelKey(json, 0, json.size(), key);
        if (colon == std::string::npos) {
            return json;  // field absent; caller should handle
        }
        auto [vs, ve] = ReadJSONValue(json, colon + 1, json.size());
        return json.substr(0, vs) + new_value + json.substr(ve);
    };

    std::string new_bands = "[";
    for (size_t i = 0; i < band_entries.size(); i++) {
        if (i > 0) new_bands += ",";
        new_bands += band_entries[i];
    }
    new_bands += "]";

    std::string new_nodata = "[";
    for (size_t i = 0; i < per_band_nodatas.size(); i++) {
        if (i > 0) new_nodata += ",";
        new_nodata += per_band_nodatas[i];
    }
    new_nodata += "]";

    std::string merged = first;
    merged = replace_field(merged, "bands", new_bands);
    merged = replace_field(merged, "nodata", new_nodata);
    return merged;
}

// ─────────────────────────────────────────────
// Count the number of distinct block IDs in the union of all inputs'
// data rows (block != 0). This is what `num_blocks` should be in the
// merged metadata — it can't be inferred from any single input.
//
// Uses UNION (not UNION ALL) so DuckDB dedups across inputs naturally.
// ─────────────────────────────────────────────
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

// ─────────────────────────────────────────────
// Build the data-join SQL: FULL OUTER JOIN of N parquet inputs on `block`.
// Each input contributes its band_1 column under output name band_(i+1).
// ─────────────────────────────────────────────
static std::string BuildJoinSQL(const std::vector<std::string> &paths) {
    std::stringstream sql;
    sql << "SELECT ";
    // COALESCE all inputs' block columns
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
    // Filter out the metadata rows (block=0) from each input
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
// Bind: parse list, read metadata from each input, validate, compose,
// build SQL, set output schema.
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

    // Read metadata rows from each input via Connection.
    auto jsons = ReadMetadataJsons(context, bind_data->input_paths);

    // Validate + compose merged metadata.
    bind_data->merged_metadata = MergeMetadata(bind_data->input_paths, jsons);

    // Recompute num_blocks: distinct block IDs in the union of all inputs'
    // data rows. The composed metadata starts with input 0's value, which
    // is wrong for any merge of >1 inputs (it's just one band's count).
    int64_t merged_blocks = CountMergedBlocks(context, bind_data->input_paths);
    bind_data->merged_metadata = ReplaceTopLevelField(
        bind_data->merged_metadata, "num_blocks", std::to_string(merged_blocks));

    // Build the data-join SQL string for InitGlobal to execute.
    bind_data->join_sql = BuildJoinSQL(bind_data->input_paths);

    // Output schema: block, metadata, band_1..band_N.
    names.push_back("block");      return_types.push_back(LogicalType::UBIGINT);
    names.push_back("metadata");   return_types.push_back(LogicalType::VARCHAR);
    for (size_t i = 0; i < bind_data->input_paths.size(); i++) {
        names.push_back("band_" + std::to_string(i + 1));
        return_types.push_back(LogicalType::BLOB);
    }
    bind_data->column_names = names;
    return std::move(bind_data);
}

// ─────────────────────────────────────────────
// InitGlobal: open a Connection, run the join SQL, store streaming result.
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
// Execute: emit metadata row first, then stream chunks from the join
// QueryResult into the output DataChunk.
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

    // Phase 1: emit metadata row exactly once
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

    // Phase 2: stream from the join QueryResult
    while (out_row < max_rows) {
        if (!state.current_chunk || state.current_row >= state.current_chunk->size()) {
            state.current_chunk = state.result->Fetch();
            state.current_row = 0;
            if (!state.current_chunk || state.current_chunk->size() == 0) {
                state.finished = true;
                break;
            }
        }
        // Copy one row from current_chunk into output
        for (idx_t col = 0; col < band_count + 2; col++) {
            // Use Vector::SetValue/GetValue for safety; performance is
            // acceptable since each row is small (one UBIGINT + one
            // VARCHAR + N BLOBs, all already in memory).
            Value v = state.current_chunk->GetValue(col, state.current_row);
            output.SetValue(col, out_row, v);
        }
        out_row++;
        state.current_row++;
    }

    output.SetCardinality(out_row);
}

// ─────────────────────────────────────────────
// Cardinality estimate
// ─────────────────────────────────────────────
static unique_ptr<NodeStatistics> RaquetMergeBandsCardinality(
        ClientContext &context, const FunctionData *bind_data_p) {
    // We don't know the joined row count without scanning. Provide a
    // rough estimate based on the first input's row count.
    auto &bind = bind_data_p->Cast<RaquetMergeBandsBindData>();
    if (bind.input_paths.empty()) {
        return make_uniq<NodeStatistics>();
    }
    Connection con(*context.db);
    auto q = "SELECT COUNT(*) FROM read_parquet(" +
             SqlSingleQuote(bind.input_paths[0]) + ") WHERE block != 0";
    auto result = con.Query(q);
    if (result->HasError() || !result) {
        return make_uniq<NodeStatistics>();
    }
    auto chunk = result->Fetch();
    if (!chunk || chunk->size() == 0) {
        return make_uniq<NodeStatistics>();
    }
    int64_t n = chunk->GetValue(0, 0).GetValue<int64_t>();
    return make_uniq<NodeStatistics>(static_cast<idx_t>(n + 1));
}

// ─────────────────────────────────────────────
// Registration
// ─────────────────────────────────────────────
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
