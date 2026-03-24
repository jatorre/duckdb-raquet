#pragma once

#include <string>
#include <vector>
#include <stdexcept>
#include <cmath>
#include <limits>

namespace duckdb {
namespace raquet {

// Band info structure (v0.3.0+ format)
struct BandInfo {
    std::string name;
    std::string type;
    double nodata;
    bool has_nodata;

    // Extended band metadata (v0.3.0+)
    std::string description;
    std::string unit;
    std::string colorinterp;
    double scale;
    double offset;
    bool has_scale;
    bool has_offset;

    BandInfo() : nodata(0), has_nodata(false), scale(1.0), offset(0.0),
                 has_scale(false), has_offset(false) {}
    BandInfo(const std::string &n, const std::string &t)
        : name(n), type(t), nodata(0), has_nodata(false), scale(1.0), offset(0.0),
          has_scale(false), has_offset(false) {}
    BandInfo(const std::string &n, const std::string &t, double nd)
        : name(n), type(t), nodata(nd), has_nodata(true), scale(1.0), offset(0.0),
          has_scale(false), has_offset(false) {}
};

// Parsed raquet metadata (v0.4.0 format)
struct RaquetMetadata {
    std::string file_format;  // new in v0.3.0: should be "raquet"
    std::string compression;
    int compression_quality;  // new in v0.4.0: JPEG/WebP quality (1-100), 0 if not specified
    std::string band_layout;  // new in v0.4.0: "sequential" (default) or "interleaved"
    int block_width;
    int block_height;
    int min_zoom;       // was minresolution
    int max_zoom;       // was maxresolution
    int pixel_zoom;     // new in v0.3.0
    int num_blocks;
    std::string scheme; // "quadbin"
    std::string crs;    // "EPSG:3857"
    bool tile_statistics;              // v0.5.0: pre-computed per-tile stats
    std::vector<std::string> tile_statistics_columns; // v0.5.0: which stats are available
    std::vector<BandInfo> band_info;  // Full band info including nodata
    std::vector<std::pair<std::string, std::string>> bands;  // name -> type (for backward compat)

    // Get band type by index (0-based) or name
    std::string get_band_type(int band_index) const {
        if (band_index < 0 || band_index >= static_cast<int>(bands.size())) {
            throw std::invalid_argument("Band index out of range");
        }
        return bands[band_index].second;
    }

    // Get band index by name (0-based), returns -1 if not found
    int get_band_index(const std::string &band_name) const {
        for (size_t i = 0; i < bands.size(); i++) {
            if (bands[i].first == band_name) {
                return static_cast<int>(i);
            }
        }
        return -1;
    }

    std::string get_band_type(const std::string &band_name) const {
        for (const auto &band : bands) {
            if (band.first == band_name) {
                return band.second;
            }
        }
        throw std::invalid_argument("Band not found: " + band_name);
    }

    // Get band info by index
    const BandInfo& get_band_info(int band_index) const {
        if (band_index < 0 || band_index >= static_cast<int>(band_info.size())) {
            throw std::invalid_argument("Band index out of range");
        }
        return band_info[band_index];
    }

    // Check if a value is nodata for a band
    bool is_nodata(int band_index, double value) const {
        if (band_index < 0 || band_index >= static_cast<int>(band_info.size())) {
            return false;
        }
        const auto &info = band_info[band_index];
        if (!info.has_nodata) return false;
        // Use exact comparison for integer types, approximate for float
        return value == info.nodata ||
               (std::isnan(value) && std::isnan(info.nodata));
    }

    // v0.4.0: Check if band layout is interleaved
    bool is_interleaved() const {
        return band_layout == "interleaved";
    }

    // v0.5.0: Check if tile statistics are available
    bool has_tile_statistics() const {
        return tile_statistics;
    }

    // v0.4.0: Check if compression is lossy (JPEG/WebP)
    bool is_lossy_compression() const {
        return compression == "jpeg" || compression == "webp";
    }

    // v0.4.0: Get the number of bands
    int num_bands() const {
        return static_cast<int>(bands.size());
    }

    // Bounds in WGS84 (EPSG:4326) — used for metadata generation
    double bounds_minlon = 0, bounds_minlat = 0, bounds_maxlon = 0, bounds_maxlat = 0;

    // Serialize to raquet format v0.5.0 JSON metadata
    std::string to_json() const {
        std::string json = "{";
        json += "\"file_format\":\"raquet\"";
        json += ",\"version\":\"0.5.0\"";
        json += ",\"crs\":\"" + crs + "\"";

        // Bounds
        json += ",\"bounds\":[" +
            std::to_string(bounds_minlon) + "," +
            std::to_string(bounds_minlat) + "," +
            std::to_string(bounds_maxlon) + "," +
            std::to_string(bounds_maxlat) + "]";
        json += ",\"bounds_crs\":\"EPSG:4326\"";

        // Compression
        if (compression == "none" || compression.empty()) {
            json += ",\"compression\":null";
        } else {
            json += ",\"compression\":\"" + compression + "\"";
        }
        if (compression_quality > 0) {
            json += ",\"compression_quality\":" + std::to_string(compression_quality);
        }

        // Band layout
        if (band_layout == "interleaved") {
            json += ",\"band_layout\":\"interleaved\"";
        }

        // Tiling
        json += ",\"tiling\":{";
        json += "\"scheme\":\"" + scheme + "\"";
        json += ",\"block_width\":" + std::to_string(block_width);
        json += ",\"block_height\":" + std::to_string(block_height);
        json += ",\"min_zoom\":" + std::to_string(min_zoom);
        json += ",\"max_zoom\":" + std::to_string(max_zoom);
        json += ",\"pixel_zoom\":" + std::to_string(pixel_zoom);
        json += ",\"num_blocks\":" + std::to_string(num_blocks);
        json += "}";

        // Bands
        json += ",\"bands\":[";
        for (size_t i = 0; i < band_info.size(); i++) {
            if (i > 0) json += ",";
            const auto &bi = band_info[i];
            json += "{\"name\":\"" + bi.name + "\"";
            json += ",\"type\":\"" + bi.type + "\"";
            if (bi.has_nodata) {
                if (std::isnan(bi.nodata)) {
                    json += ",\"nodata\":\"NaN\"";
                } else if (std::isinf(bi.nodata) && bi.nodata > 0) {
                    json += ",\"nodata\":\"Infinity\"";
                } else if (std::isinf(bi.nodata) && bi.nodata < 0) {
                    json += ",\"nodata\":\"-Infinity\"";
                } else {
                    json += ",\"nodata\":" + std::to_string(bi.nodata);
                }
            } else {
                json += ",\"nodata\":null";
            }
            if (!bi.description.empty()) {
                json += ",\"description\":\"" + bi.description + "\"";
            }
            if (!bi.unit.empty()) {
                json += ",\"unit\":\"" + bi.unit + "\"";
            }
            if (!bi.colorinterp.empty()) {
                json += ",\"colorinterp\":\"" + bi.colorinterp + "\"";
            }
            if (bi.has_scale) {
                json += ",\"scale\":" + std::to_string(bi.scale);
            }
            if (bi.has_offset) {
                json += ",\"offset\":" + std::to_string(bi.offset);
            }
            json += "}";
        }
        json += "]";

        // Tile statistics
        if (tile_statistics) {
            json += ",\"tile_statistics\":true";
            json += ",\"tile_statistics_columns\":[";
            for (size_t i = 0; i < tile_statistics_columns.size(); i++) {
                if (i > 0) json += ",";
                json += "\"" + tile_statistics_columns[i] + "\"";
            }
            json += "]";
        }

        json += "}";
        return json;
    }
};

// Simple JSON value extraction (finds "key": value or "key": "value")
inline std::string extract_json_string(const std::string &json, const std::string &key) {
    std::string search = "\"" + key + "\":";
    size_t pos = json.find(search);
    if (pos == std::string::npos) {
        return "";
    }
    pos += search.length();

    // Skip whitespace
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) {
        pos++;
    }

    if (pos >= json.size()) return "";

    if (json[pos] == '"') {
        // String value
        pos++;
        size_t end = json.find('"', pos);
        if (end == std::string::npos) return "";
        return json.substr(pos, end - pos);
    } else {
        // Numeric or other value
        size_t end = pos;
        while (end < json.size() && json[end] != ',' && json[end] != '}' && json[end] != ']') {
            end++;
        }
        std::string val = json.substr(pos, end - pos);
        // Trim whitespace
        while (!val.empty() && (val.back() == ' ' || val.back() == '\t')) {
            val.pop_back();
        }
        return val;
    }
}

inline int extract_json_int(const std::string &json, const std::string &key, int default_val = 0) {
    std::string val = extract_json_string(json, key);
    if (val.empty()) return default_val;
    try {
        return std::stoi(val);
    } catch (...) {
        return default_val;
    }
}

inline double extract_json_double(const std::string &json, const std::string &key, double default_val = 0.0) {
    std::string val = extract_json_string(json, key);
    if (val.empty() || val == "null") return default_val;
    try {
        return std::stod(val);
    } catch (...) {
        return default_val;
    }
}

inline bool extract_json_has_value(const std::string &json, const std::string &key) {
    std::string val = extract_json_string(json, key);
    return !val.empty() && val != "null";
}

// Parse nodata value handling Zarr v3 string conventions:
// - "NaN" -> NaN
// - "Infinity" -> +Infinity
// - "-Infinity" -> -Infinity
// - numeric values as-is
inline double parse_nodata_value(const std::string &val) {
    if (val.empty() || val == "null") {
        return 0.0;
    }
    // Handle Zarr v3 string conventions (case-sensitive per spec)
    if (val == "NaN") {
        return std::nan("");
    }
    if (val == "Infinity") {
        return std::numeric_limits<double>::infinity();
    }
    if (val == "-Infinity") {
        return -std::numeric_limits<double>::infinity();
    }
    // Try to parse as numeric
    try {
        return std::stod(val);
    } catch (...) {
        return 0.0;
    }
}

// Extract a JSON array of strings: "key": ["a", "b", "c"]
inline std::vector<std::string> extract_json_string_array(const std::string &json, const std::string &key) {
    std::vector<std::string> result;
    std::string search = "\"" + key + "\":";
    size_t pos = json.find(search);
    if (pos == std::string::npos) return result;
    pos += search.length();

    // Skip whitespace
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t' || json[pos] == '\n')) {
        pos++;
    }
    if (pos >= json.size() || json[pos] != '[') return result;

    size_t arr_end = json.find(']', pos);
    if (arr_end == std::string::npos) return result;

    std::string arr = json.substr(pos + 1, arr_end - pos - 1);
    size_t p = 0;
    while (p < arr.size()) {
        size_t q_start = arr.find('"', p);
        if (q_start == std::string::npos) break;
        size_t q_end = arr.find('"', q_start + 1);
        if (q_end == std::string::npos) break;
        result.push_back(arr.substr(q_start + 1, q_end - q_start - 1));
        p = q_end + 1;
    }
    return result;
}

// Extract a nested JSON object as a string
inline std::string extract_json_object(const std::string &json, const std::string &key) {
    std::string search = "\"" + key + "\":";
    size_t pos = json.find(search);
    if (pos == std::string::npos) return "";
    pos += search.length();

    // Skip whitespace
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t' || json[pos] == '\n')) {
        pos++;
    }

    if (pos >= json.size() || json[pos] != '{') return "";

    // Find matching closing brace
    int depth = 1;
    size_t start = pos;
    pos++;
    while (pos < json.size() && depth > 0) {
        if (json[pos] == '{') depth++;
        else if (json[pos] == '}') depth--;
        pos++;
    }

    return json.substr(start, pos - start);
}

// Parse bands array from metadata JSON (returns both legacy format and full BandInfo)
inline void parse_bands_full(const std::string &json,
                             std::vector<std::pair<std::string, std::string>> &bands,
                             std::vector<BandInfo> &band_info) {
    bands.clear();
    band_info.clear();

    // Find "bands": [...]
    size_t bands_pos = json.find("\"bands\":");
    if (bands_pos == std::string::npos) return;

    size_t arr_start = json.find('[', bands_pos);
    if (arr_start == std::string::npos) return;

    size_t arr_end = json.find(']', arr_start);
    if (arr_end == std::string::npos) return;

    std::string bands_str = json.substr(arr_start + 1, arr_end - arr_start - 1);

    // Parse each band object
    size_t pos = 0;
    while (pos < bands_str.size()) {
        size_t obj_start = bands_str.find('{', pos);
        if (obj_start == std::string::npos) break;

        size_t obj_end = bands_str.find('}', obj_start);
        if (obj_end == std::string::npos) break;

        std::string band_obj = bands_str.substr(obj_start, obj_end - obj_start + 1);

        std::string name = extract_json_string(band_obj, "name");
        std::string type = extract_json_string(band_obj, "type");

        if (!name.empty() && !type.empty()) {
            bands.push_back({name, type});

            BandInfo info(name, type);
            if (extract_json_has_value(band_obj, "nodata")) {
                // Use parse_nodata_value to handle Zarr v3 string conventions
                std::string nodata_str = extract_json_string(band_obj, "nodata");
                info.nodata = parse_nodata_value(nodata_str);
                info.has_nodata = true;
            }
            band_info.push_back(info);
        }

        pos = obj_end + 1;
    }
}

// Legacy function for backward compatibility
inline std::vector<std::pair<std::string, std::string>> parse_bands(const std::string &json) {
    std::vector<std::pair<std::string, std::string>> bands;
    std::vector<BandInfo> band_info;
    parse_bands_full(json, bands, band_info);
    return bands;
}

// Parse metadata JSON string (v0.4.0 format)
inline RaquetMetadata parse_metadata(const std::string &json) {
    RaquetMetadata meta;

    // New in v0.3.0: file_format identifier
    meta.file_format = extract_json_string(json, "file_format");

    meta.compression = extract_json_string(json, "compression");
    if (meta.compression.empty()) meta.compression = "none";

    // New in v0.4.0: compression quality for JPEG/WebP
    meta.compression_quality = extract_json_int(json, "compression_quality", 0);

    // New in v0.4.0: band layout (sequential or interleaved)
    meta.band_layout = extract_json_string(json, "band_layout");
    if (meta.band_layout.empty()) meta.band_layout = "sequential";

    meta.crs = extract_json_string(json, "crs");

    // Parse tiling object (v0.3.0)
    std::string tiling = extract_json_object(json, "tiling");
    if (!tiling.empty()) {
        meta.min_zoom = extract_json_int(tiling, "min_zoom", 0);
        meta.max_zoom = extract_json_int(tiling, "max_zoom", 26);
        meta.pixel_zoom = extract_json_int(tiling, "pixel_zoom", 0);
        meta.num_blocks = extract_json_int(tiling, "num_blocks", 0);
        meta.block_width = extract_json_int(tiling, "block_width", 256);
        meta.block_height = extract_json_int(tiling, "block_height", 256);
        meta.scheme = extract_json_string(tiling, "scheme");
    } else {
        // Defaults if no tiling object
        meta.min_zoom = 0;
        meta.max_zoom = 26;
        meta.pixel_zoom = 0;
        meta.num_blocks = 0;
        meta.block_width = 256;
        meta.block_height = 256;
        meta.scheme = "quadbin";
    }

    parse_bands_full(json, meta.bands, meta.band_info);

    // v0.5.0: tile statistics
    std::string tile_stats_val = extract_json_string(json, "tile_statistics");
    meta.tile_statistics = (tile_stats_val == "true" || tile_stats_val == "1");
    meta.tile_statistics_columns = extract_json_string_array(json, "tile_statistics_columns");

    return meta;
}

} // namespace raquet
} // namespace duckdb
