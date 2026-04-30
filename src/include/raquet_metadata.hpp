#pragma once

#include <array>
#include <cstdint>
#include <map>
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

    // Palette color table (only populated when colorinterp == "palette").
    // Each entry is RGBA in [0, 255].
    std::vector<std::array<int, 4>> colortable;
    bool has_colortable = false;

    // Per-band statistics. v0.5.0 path populates the GDAL-approxOK fields
    // (count derived from STATISTICS_VALID_PERCENT × pixels). v0.1.0 path
    // populates the same accumulators from a 1000-pixel sample, plus the
    // raster-loader-shape extension fields (quantiles, top_values, version).
    struct Stats {
        int64_t count = 0;
        double  min = 0.0;
        double  max = 0.0;
        double  mean = 0.0;
        double  stddev = 0.0;
        double  sum = 0.0;
        double  sum_squares = 0.0;
        double  valid_percent = 0.0;  // STATISTICS_VALID_PERCENT (0–100)
        bool    approximated = true;
        bool    has_stats = false;
        // v0.1.0-only extension fields. Empty in v0.5.0 output (and the
        // serializer skips empty ones).
        std::string version;                              // producer tag
        std::map<int, std::vector<double>> quantiles;     // keys 3..19
        std::map<double, int64_t> top_values;             // up to 10 entries
    } stats;

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

    // Source raster dimensions in source pixel space (GDAL XSize/YSize).
    // Emitted as top-level "width"/"height" in both v0.1.0 and v0.5.0 metadata.
    int width = 0;
    int height = 0;

    // CF time dimension metadata (NetCDF)
    bool has_time = false;
    std::string time_cf_units;   // e.g., "minutes since 1980-01-01 00:00:00"
    std::string time_calendar;   // e.g., "standard"

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

    // Serialize a single nodata value (or null), reused by v0.1.0 and v0.5.0.
    static std::string nodata_to_json(const BandInfo &bi) {
        if (!bi.has_nodata) return "null";
        if (std::isnan(bi.nodata)) return "\"NaN\"";
        if (std::isinf(bi.nodata)) {
            return bi.nodata > 0 ? "\"Infinity\"" : "\"-Infinity\"";
        }
        return std::to_string(bi.nodata);
    }

    // Per-band nodata for v0.1.0 bands[i].nodata. raster-loader emits this
    // as a string ("255" rather than 255) so consumers do unconditional
    // string-typed reads. We mirror that shape: always-quoted, integer
    // formatting when the value is exactly integer.
    static std::string nodata_to_json_v0_band(const BandInfo &bi) {
        if (!bi.has_nodata) return "null";
        std::string body;
        if (std::isnan(bi.nodata)) {
            body = "NaN";
        } else if (std::isinf(bi.nodata)) {
            body = bi.nodata > 0 ? "Infinity" : "-Infinity";
        } else if (std::floor(bi.nodata) == bi.nodata) {
            body = std::to_string(static_cast<int64_t>(bi.nodata));
        } else {
            body = std::to_string(bi.nodata);
        }
        return "\"" + body + "\"";
    }

    // Render the quantiles map as {"3":[...],"4":[...],...,"19":[...]}.
    // Empty map → "{}" (caller can choose to skip emitting the key).
    static std::string quantiles_to_json(const std::map<int, std::vector<double>> &q) {
        std::string out = "{";
        bool first = true;
        for (auto &kv : q) {
            if (!first) out += ",";
            first = false;
            out += "\"" + std::to_string(kv.first) + "\":[";
            for (size_t i = 0; i < kv.second.size(); i++) {
                if (i > 0) out += ",";
                // Quantile values come from sample pixels; format as integer
                // when exact (matches raster-loader for integer-typed bands).
                double v = kv.second[i];
                if (std::floor(v) == v && !std::isinf(v) && !std::isnan(v)) {
                    out += std::to_string(static_cast<int64_t>(v));
                } else {
                    out += std::to_string(v);
                }
            }
            out += "]";
        }
        out += "}";
        return out;
    }

    // Render top_values as {"<value>": <count>, ...}. raster-loader uses
    // string-cast keys (numpy int -> Python str via JSON encoding).
    static std::string top_values_to_json(const std::map<double, int64_t> &tv) {
        std::string out = "{";
        bool first = true;
        for (auto &kv : tv) {
            if (!first) out += ",";
            first = false;
            std::string key_str;
            const double k = kv.first;
            if (std::floor(k) == k && !std::isinf(k) && !std::isnan(k)) {
                key_str = std::to_string(static_cast<int64_t>(k));
            } else {
                key_str = std::to_string(k);
            }
            out += "\"" + key_str + "\":" + std::to_string(kv.second);
        }
        out += "}";
        return out;
    }

    // Render a band's color table as a JSON object keyed by stringified index:
    //   {"0":[r,g,b,a], "1":[r,g,b,a], ...}
    // Returns "null" when the band has no palette.
    static std::string colortable_to_json(const BandInfo &bi) {
        if (!bi.has_colortable || bi.colortable.empty()) return "null";
        std::string out = "{";
        for (size_t i = 0; i < bi.colortable.size(); i++) {
            if (i > 0) out += ",";
            const auto &e = bi.colortable[i];
            out += "\"" + std::to_string(i) + "\":["
                + std::to_string(e[0]) + ","
                + std::to_string(e[1]) + ","
                + std::to_string(e[2]) + ","
                + std::to_string(e[3]) + "]";
        }
        out += "}";
        return out;
    }

    // Render the v0.1.0 stats object for a single band. When stats are missing
    // the band still gets a stats key but valued null, so consumers can rely on
    // the field being present. quantiles, top_values, and version are emitted
    // when populated (raster-loader-shape extensions, not in spec but used by
    // CARTO Builder colormap UIs).
    static std::string stats_to_json_v0(const BandInfo &bi) {
        if (!bi.stats.has_stats) return "null";
        const auto &s = bi.stats;
        std::string out = "{";
        out += "\"count\":" + std::to_string(s.count);
        out += ",\"min\":" + std::to_string(s.min);
        out += ",\"max\":" + std::to_string(s.max);
        out += ",\"mean\":" + std::to_string(s.mean);
        out += ",\"stddev\":" + std::to_string(s.stddev);
        out += ",\"sum\":" + std::to_string(s.sum);
        out += ",\"sum_squares\":" + std::to_string(s.sum_squares);
        if (!s.quantiles.empty()) {
            out += ",\"quantiles\":" + quantiles_to_json(s.quantiles);
        }
        if (!s.top_values.empty()) {
            out += ",\"top_values\":" + top_values_to_json(s.top_values);
        }
        if (!s.version.empty()) {
            out += ",\"version\":\"" + s.version + "\"";
        }
        out += ",\"approximated_stats\":";
        out += s.approximated ? "true" : "false";
        out += "}";
        return out;
    }

    // Serialize to v0.1.0 JSON metadata format (raquet 0.2.5 parity).
    // Top-level: version, compression, flat tiling fields, bounds, center,
    // width, height, num_pixels, block_resolution, nodata (scalar or array),
    // and bands as objects with {name, type, colorinterp, colortable, stats}.
    std::string to_json_v0() const {
        std::string json = "{";
        json += "\"version\":\"0.1.0\"";

        // Compression
        if (compression == "none" || compression.empty()) {
            json += ",\"compression\":null";
        } else {
            json += ",\"compression\":\"" + compression + "\"";
        }

        // Flat tiling fields (v0 names)
        json += ",\"block_width\":" + std::to_string(block_width);
        json += ",\"block_height\":" + std::to_string(block_height);
        json += ",\"minresolution\":" + std::to_string(min_zoom);
        json += ",\"maxresolution\":" + std::to_string(max_zoom);
        json += ",\"pixel_resolution\":" + std::to_string(pixel_zoom);
        json += ",\"num_blocks\":" + std::to_string(num_blocks);

        // Bounds in WGS84 [W, S, E, N]
        json += ",\"bounds\":[" +
            std::to_string(bounds_minlon) + "," +
            std::to_string(bounds_minlat) + "," +
            std::to_string(bounds_maxlon) + "," +
            std::to_string(bounds_maxlat) + "]";

        // Center [lon, lat, min_zoom]
        double center_lon = (bounds_minlon + bounds_maxlon) / 2.0;
        double center_lat = (bounds_minlat + bounds_maxlat) / 2.0;
        json += ",\"center\":[" +
            std::to_string(center_lon) + "," +
            std::to_string(center_lat) + "," +
            std::to_string(min_zoom) + "]";

        // Source raster dimensions
        json += ",\"width\":" + std::to_string(width);
        json += ",\"height\":" + std::to_string(height);
        json += ",\"num_pixels\":" +
            std::to_string(static_cast<int64_t>(width) * height);
        json += ",\"block_resolution\":" + std::to_string(max_zoom);

        // Nodata: scalar for single-band rasters, array otherwise (raquet 0.2.5 shape)
        if (band_info.size() == 1) {
            json += ",\"nodata\":" + nodata_to_json(band_info[0]);
        } else {
            json += ",\"nodata\":[";
            for (size_t i = 0; i < band_info.size(); i++) {
                if (i > 0) json += ",";
                json += nodata_to_json(band_info[i]);
            }
            json += "]";
        }

        // Bands as objects: [{name, type, nodata, colorinterp, colortable, stats}, ...]
        // raster-loader emits per-band nodata as a string ("255") even though
        // the v0.1.0 spec doesn't define this field; downstream Builder UIs
        // expect it, so we mirror it here.
        json += ",\"bands\":[";
        for (size_t i = 0; i < band_info.size(); i++) {
            if (i > 0) json += ",";
            const auto &bi = band_info[i];
            json += "{\"name\":\"" + bi.name + "\"";
            json += ",\"type\":\"" + bi.type + "\"";
            json += ",\"nodata\":" + nodata_to_json_v0_band(bi);
            json += ",\"colorinterp\":";
            if (bi.colorinterp.empty()) {
                json += "null";
            } else {
                json += "\"" + bi.colorinterp + "\"";
            }
            json += ",\"colortable\":" + colortable_to_json(bi);
            json += ",\"stats\":" + stats_to_json_v0(bi);
            json += "}";
        }
        json += "]";

        json += "}";
        return json;
    }

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

        // Source raster dimensions (v0.5.0 spec top-level fields)
        json += ",\"width\":" + std::to_string(width);
        json += ",\"height\":" + std::to_string(height);

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
            if (bi.has_colortable && !bi.colortable.empty()) {
                json += ",\"colortable\":" + colortable_to_json(bi);
            }
            if (bi.stats.has_stats) {
                const auto &s = bi.stats;
                json += ",\"STATISTICS_MINIMUM\":" + std::to_string(s.min);
                json += ",\"STATISTICS_MAXIMUM\":" + std::to_string(s.max);
                json += ",\"STATISTICS_MEAN\":" + std::to_string(s.mean);
                json += ",\"STATISTICS_STDDEV\":" + std::to_string(s.stddev);
                json += ",\"STATISTICS_VALID_PERCENT\":" + std::to_string(s.valid_percent);
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

        // CF time dimension
        if (has_time) {
            json += ",\"time\":{";
            json += "\"cf:units\":\"" + time_cf_units + "\"";
            json += ",\"cf:calendar\":\"" + time_calendar + "\"";
            json += "}";
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

// Extract a JSON array of numbers: "key": [1.0, null, 0]
// Returns values with a parallel has_value vector for null detection
inline void extract_json_number_array(const std::string &json, const std::string &key,
                                       std::vector<double> &values, std::vector<bool> &has_value) {
    values.clear();
    has_value.clear();
    std::string search = "\"" + key + "\":";
    size_t pos = json.find(search);
    if (pos == std::string::npos) return;
    pos += search.length();

    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t' || json[pos] == '\n')) pos++;
    if (pos >= json.size() || json[pos] != '[') return;

    size_t arr_end = json.find(']', pos);
    if (arr_end == std::string::npos) return;

    std::string arr = json.substr(pos + 1, arr_end - pos - 1);
    size_t p = 0;
    while (p < arr.size()) {
        while (p < arr.size() && (arr[p] == ' ' || arr[p] == ',')) p++;
        if (p >= arr.size()) break;

        if (arr.compare(p, 4, "null") == 0) {
            values.push_back(0);
            has_value.push_back(false);
            p += 4;
        } else {
            size_t end = p;
            while (end < arr.size() && arr[end] != ',' && arr[end] != ']') end++;
            std::string val_str = arr.substr(p, end - p);
            // Trim whitespace
            while (!val_str.empty() && (val_str.back() == ' ' || val_str.back() == '\t')) val_str.pop_back();
            try {
                values.push_back(parse_nodata_value(val_str));
                has_value.push_back(true);
            } catch (...) {
                values.push_back(0);
                has_value.push_back(false);
            }
            p = end;
        }
    }
}

// Parse a colortable object: {"0":[r,g,b,a], "1":[r,g,b,a], ...}.
// The serializer emits entries in order, so we just walk RGBA arrays sequentially.
// Returns an empty vector when the key is missing or the value is null.
inline std::vector<std::array<int, 4>> parse_colortable(const std::string &band_json) {
    std::vector<std::array<int, 4>> out;
    std::string obj = extract_json_object(band_json, "colortable");
    if (obj.empty()) return out;

    size_t pos = 0;
    while (pos < obj.size()) {
        size_t lb = obj.find('[', pos);
        if (lb == std::string::npos) break;
        size_t rb = obj.find(']', lb);
        if (rb == std::string::npos) break;
        std::string inner = obj.substr(lb + 1, rb - lb - 1);
        std::array<int, 4> rgba{0, 0, 0, 255};
        size_t p = 0;
        int i = 0;
        while (p < inner.size() && i < 4) {
            while (p < inner.size() && (inner[p] == ' ' || inner[p] == ',')) p++;
            size_t end = p;
            while (end < inner.size() && inner[end] != ',' && inner[end] != ' ') end++;
            if (end > p) {
                try { rgba[i] = std::stoi(inner.substr(p, end - p)); } catch (...) {}
            }
            p = end;
            i++;
        }
        out.push_back(rgba);
        pos = rb + 1;
    }
    return out;
}

// Parse a v0.1.0 stats object: {"count":...,"min":...,"max":...,...}.
inline BandInfo::Stats parse_stats_v0(const std::string &band_json) {
    BandInfo::Stats s;
    std::string obj = extract_json_object(band_json, "stats");
    if (obj.empty()) return s;

    s.count       = static_cast<int64_t>(extract_json_double(obj, "count", 0));
    s.min         = extract_json_double(obj, "min", 0);
    s.max         = extract_json_double(obj, "max", 0);
    s.mean        = extract_json_double(obj, "mean", 0);
    s.stddev      = extract_json_double(obj, "stddev", 0);
    s.sum         = extract_json_double(obj, "sum", 0);
    s.sum_squares = extract_json_double(obj, "sum_squares", 0);
    std::string approx = extract_json_string(obj, "approximated_stats");
    s.approximated = (approx == "true" || approx == "1");
    s.has_stats = true;
    return s;
}

// Parse v0.5.0 STATISTICS_* keys from a band object (flat top-level fields).
inline BandInfo::Stats parse_stats_v05(const std::string &band_json) {
    BandInfo::Stats s;
    if (!extract_json_has_value(band_json, "STATISTICS_MINIMUM")) return s;
    s.min          = extract_json_double(band_json, "STATISTICS_MINIMUM", 0);
    s.max          = extract_json_double(band_json, "STATISTICS_MAXIMUM", 0);
    s.mean         = extract_json_double(band_json, "STATISTICS_MEAN", 0);
    s.stddev       = extract_json_double(band_json, "STATISTICS_STDDEV", 0);
    s.valid_percent = extract_json_double(band_json, "STATISTICS_VALID_PERCENT", 0);
    s.has_stats    = true;
    return s;
}

// Parse bands array from metadata JSON (handles legacy v0 arrays, new v0.1.0
// objects, and v0.5.0 objects — they share the object branch).
// Legacy v0: bands is array of arrays [["name", "type"], ...] with separate "nodata" array
// v0.1.0+ / v0.5.0: bands is array of objects [{"name": ..., "type": ..., ...}, ...]
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

    // Detect format: v0 has nested arrays [[...], ...], v0.5.0 has objects [{...}, ...]
    // Skip whitespace after '['
    size_t first_elem = arr_start + 1;
    while (first_elem < json.size() && (json[first_elem] == ' ' || json[first_elem] == '\t' ||
           json[first_elem] == '\n' || json[first_elem] == '\r')) first_elem++;

    if (first_elem < json.size() && json[first_elem] == '[') {
        // v0 format: bands is array of arrays [["name", "type"], ...]
        // Find the end of the outer array (need to handle nested brackets)
        int depth = 1;
        size_t pos = arr_start + 1;
        while (pos < json.size() && depth > 0) {
            if (json[pos] == '[') depth++;
            else if (json[pos] == ']') depth--;
            pos++;
        }
        size_t arr_end = pos - 1;
        std::string bands_str = json.substr(arr_start + 1, arr_end - arr_start - 1);

        // Parse each inner array ["name", "type"]
        size_t p = 0;
        while (p < bands_str.size()) {
            size_t inner_start = bands_str.find('[', p);
            if (inner_start == std::string::npos) break;
            size_t inner_end = bands_str.find(']', inner_start);
            if (inner_end == std::string::npos) break;

            std::string inner = bands_str.substr(inner_start + 1, inner_end - inner_start - 1);

            // Extract two quoted strings from the inner array
            std::vector<std::string> parts;
            size_t q = 0;
            while (q < inner.size() && parts.size() < 2) {
                size_t q_start = inner.find('"', q);
                if (q_start == std::string::npos) break;
                size_t q_end = inner.find('"', q_start + 1);
                if (q_end == std::string::npos) break;
                parts.push_back(inner.substr(q_start + 1, q_end - q_start - 1));
                q = q_end + 1;
            }

            if (parts.size() == 2) {
                bands.push_back({parts[0], parts[1]});
                band_info.push_back(BandInfo(parts[0], parts[1]));
            }

            p = inner_end + 1;
        }

        // v0: nodata is a separate top-level array
        std::vector<double> nodata_values;
        std::vector<bool> nodata_has;
        extract_json_number_array(json, "nodata", nodata_values, nodata_has);
        for (size_t i = 0; i < band_info.size() && i < nodata_values.size(); i++) {
            if (nodata_has[i]) {
                band_info[i].nodata = nodata_values[i];
                band_info[i].has_nodata = true;
            }
        }
    } else if (first_elem < json.size() && json[first_elem] == '{') {
        // v0.5.0 format: bands is array of objects [{"name": ..., "type": ...}, ...]
        size_t arr_end = arr_start + 1;
        int depth = 1;
        while (arr_end < json.size() && depth > 0) {
            if (json[arr_end] == '[') depth++;
            else if (json[arr_end] == ']') depth--;
            arr_end++;
        }
        arr_end--;
        std::string bands_str = json.substr(arr_start + 1, arr_end - arr_start - 1);

        size_t pos = 0;
        while (pos < bands_str.size()) {
            size_t obj_start = bands_str.find('{', pos);
            if (obj_start == std::string::npos) break;

            // Find matching closing brace (handle nested objects)
            int obj_depth = 1;
            size_t obj_end = obj_start + 1;
            while (obj_end < bands_str.size() && obj_depth > 0) {
                if (bands_str[obj_end] == '{') obj_depth++;
                else if (bands_str[obj_end] == '}') obj_depth--;
                obj_end++;
            }
            obj_end--;

            std::string band_obj = bands_str.substr(obj_start, obj_end - obj_start + 1);

            std::string name = extract_json_string(band_obj, "name");
            std::string type = extract_json_string(band_obj, "type");

            if (!name.empty() && !type.empty()) {
                bands.push_back({name, type});

                BandInfo info(name, type);
                if (extract_json_has_value(band_obj, "nodata")) {
                    std::string nodata_str = extract_json_string(band_obj, "nodata");
                    info.nodata = parse_nodata_value(nodata_str);
                    info.has_nodata = true;
                }
                info.colorinterp = extract_json_string(band_obj, "colorinterp");
                info.description = extract_json_string(band_obj, "description");
                info.unit = extract_json_string(band_obj, "unit");

                // Color table — present in both v0.1.0 (raquet 0.2.5) and v0.5.0 outputs.
                info.colortable = parse_colortable(band_obj);
                info.has_colortable = !info.colortable.empty();

                // Stats — try v0.1.0 nested object first, fall back to v0.5.0 flat keys.
                BandInfo::Stats s = parse_stats_v0(band_obj);
                if (!s.has_stats) {
                    s = parse_stats_v05(band_obj);
                }
                info.stats = s;

                band_info.push_back(info);
            }

            pos = obj_end + 1;
        }
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

    // Detect version: v0 has no "version" field, v0.5.0+ has it
    std::string version = extract_json_string(json, "version");

    // Parse tiling info — differs between v0 (flat) and v0.5.0+ (nested tiling object)
    std::string tiling = extract_json_object(json, "tiling");
    if (!tiling.empty()) {
        // v0.3.0+ format: nested tiling object
        meta.min_zoom = extract_json_int(tiling, "min_zoom", 0);
        meta.max_zoom = extract_json_int(tiling, "max_zoom", 26);
        meta.pixel_zoom = extract_json_int(tiling, "pixel_zoom", 0);
        meta.num_blocks = extract_json_int(tiling, "num_blocks", 0);
        meta.block_width = extract_json_int(tiling, "block_width", 256);
        meta.block_height = extract_json_int(tiling, "block_height", 256);
        meta.scheme = extract_json_string(tiling, "scheme");
    } else {
        // v0 format: flat fields with old names (minresolution/maxresolution)
        meta.block_width = extract_json_int(json, "block_width", 256);
        meta.block_height = extract_json_int(json, "block_height", 256);
        meta.min_zoom = extract_json_int(json, "minresolution", 0);
        meta.max_zoom = extract_json_int(json, "maxresolution", 26);
        meta.pixel_zoom = extract_json_int(json, "pixel_resolution", 0);
        meta.num_blocks = extract_json_int(json, "num_blocks", 0);
        meta.scheme = "quadbin";
    }

    // Top-level source raster dimensions (present in v0.1.0 and v0.5.0+ outputs).
    meta.width = extract_json_int(json, "width", 0);
    meta.height = extract_json_int(json, "height", 0);

    parse_bands_full(json, meta.bands, meta.band_info);

    // v0.5.0: tile statistics
    std::string tile_stats_val = extract_json_string(json, "tile_statistics");
    meta.tile_statistics = (tile_stats_val == "true" || tile_stats_val == "1");
    meta.tile_statistics_columns = extract_json_string_array(json, "tile_statistics_columns");

    return meta;
}

} // namespace raquet
} // namespace duckdb
