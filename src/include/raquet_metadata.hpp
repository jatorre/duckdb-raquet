#pragma once

#include <string>
#include <vector>
#include <stdexcept>

namespace duckdb {
namespace raquet {

// Parsed raquet metadata (v0.3.0 format)
struct RaquetMetadata {
    std::string compression;
    int block_width;
    int block_height;
    int min_zoom;       // was minresolution
    int max_zoom;       // was maxresolution
    int pixel_zoom;     // new in v0.3.0
    int num_blocks;
    std::string scheme; // "quadbin"
    std::string crs;    // "EPSG:3857"
    std::vector<std::pair<std::string, std::string>> bands;  // name -> type

    // Get band type by index (0-based) or name
    std::string get_band_type(int band_index) const {
        if (band_index < 0 || band_index >= static_cast<int>(bands.size())) {
            throw std::invalid_argument("Band index out of range");
        }
        return bands[band_index].second;
    }

    std::string get_band_type(const std::string &band_name) const {
        for (const auto &band : bands) {
            if (band.first == band_name) {
                return band.second;
            }
        }
        throw std::invalid_argument("Band not found: " + band_name);
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

// Parse bands array from metadata JSON
inline std::vector<std::pair<std::string, std::string>> parse_bands(const std::string &json) {
    std::vector<std::pair<std::string, std::string>> bands;

    // Find "bands": [...]
    size_t bands_pos = json.find("\"bands\":");
    if (bands_pos == std::string::npos) return bands;

    size_t arr_start = json.find('[', bands_pos);
    if (arr_start == std::string::npos) return bands;

    size_t arr_end = json.find(']', arr_start);
    if (arr_end == std::string::npos) return bands;

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
        }

        pos = obj_end + 1;
    }

    return bands;
}

// Parse metadata JSON string (v0.3.0 format)
inline RaquetMetadata parse_metadata(const std::string &json) {
    RaquetMetadata meta;

    meta.compression = extract_json_string(json, "compression");
    if (meta.compression.empty()) meta.compression = "none";

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

    meta.bands = parse_bands(json);

    return meta;
}

} // namespace raquet
} // namespace duckdb
