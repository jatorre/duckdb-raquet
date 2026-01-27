#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "band_decoder.hpp"
#include "quadbin.hpp"
#include <cmath>
#include <limits>
#include <cstring>
#include <string>
#include "yyjson.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

// ============================================================================
// Geometry helper functions (for point-in-polygon tests)
// ============================================================================

// Check if a point is inside a polygon ring using ray casting
static bool PointInRing(double px, double py, const double* ring_coords, uint32_t num_points) {
    bool inside = false;
    double x1, y1, x2, y2;

    for (uint32_t i = 0, j = num_points - 1; i < num_points; j = i++) {
        x1 = ring_coords[i * 2];
        y1 = ring_coords[i * 2 + 1];
        x2 = ring_coords[j * 2];
        y2 = ring_coords[j * 2 + 1];

        if (((y1 > py) != (y2 > py)) &&
            (px < (x2 - x1) * (py - y1) / (y2 - y1) + x1)) {
            inside = !inside;
        }
    }
    return inside;
}

// Check if a point is inside a polygon (handles holes)
static bool PointInPolygonImpl(double px, double py, const uint8_t* data, idx_t& offset, idx_t size) {
    if (size < offset + 4) return false;
    uint32_t num_rings;
    memcpy(&num_rings, data + offset, 4);
    offset += 4;

    if (num_rings == 0) return false;

    // First ring is outer boundary
    if (size < offset + 4) return false;
    uint32_t outer_points;
    memcpy(&outer_points, data + offset, 4);
    offset += 4;

    if (size < offset + outer_points * 16) return false;

    std::vector<double> outer_coords(outer_points * 2);
    for (uint32_t i = 0; i < outer_points; i++) {
        memcpy(&outer_coords[i * 2], data + offset + i * 16, 8);
        memcpy(&outer_coords[i * 2 + 1], data + offset + i * 16 + 8, 8);
    }
    offset += outer_points * 16;

    // Check if inside outer boundary
    if (!PointInRing(px, py, outer_coords.data(), outer_points)) {
        // Skip remaining rings
        for (uint32_t r = 1; r < num_rings; r++) {
            if (size < offset + 4) return false;
            uint32_t ring_points;
            memcpy(&ring_points, data + offset, 4);
            offset += 4 + ring_points * 16;
        }
        return false;
    }

    // Check holes (if inside a hole, point is outside polygon)
    for (uint32_t r = 1; r < num_rings; r++) {
        if (size < offset + 4) return false;
        uint32_t hole_points;
        memcpy(&hole_points, data + offset, 4);
        offset += 4;

        if (size < offset + hole_points * 16) return false;

        std::vector<double> hole_coords(hole_points * 2);
        for (uint32_t i = 0; i < hole_points; i++) {
            memcpy(&hole_coords[i * 2], data + offset + i * 16, 8);
            memcpy(&hole_coords[i * 2 + 1], data + offset + i * 16 + 8, 8);
        }
        offset += hole_points * 16;

        if (PointInRing(px, py, hole_coords.data(), hole_points)) {
            return false;  // Inside a hole
        }
    }

    return true;
}

// Check if a point is inside a geometry
static bool PointInGeometry(double px, double py, const string_t &geom) {
    const uint8_t *data = reinterpret_cast<const uint8_t*>(geom.GetData());
    idx_t size = geom.GetSize();

    if (size < 5) return false;

    uint32_t geom_type;
    memcpy(&geom_type, data + 1, 4);

    uint32_t base_type = geom_type & 0xFF;
    idx_t offset = 5;

    // Handle SRID prefix
    if (geom_type & 0x20000000) {
        offset += 4;
    }

    if (base_type == 3) {  // POLYGON
        return PointInPolygonImpl(px, py, data, offset, size);
    } else if (base_type == 6) {  // MULTIPOLYGON
        if (size < offset + 4) return false;
        uint32_t num_polygons;
        memcpy(&num_polygons, data + offset, 4);
        offset += 4;

        for (uint32_t poly = 0; poly < num_polygons; poly++) {
            // Skip WKB header for nested polygon
            if (size < offset + 5) return false;
            offset += 5;

            // Handle SRID in nested polygon if present
            uint32_t poly_type;
            memcpy(&poly_type, data + offset - 4, 4);
            if (poly_type & 0x20000000) {
                offset += 4;
            }

            idx_t poly_offset = offset;
            if (PointInPolygonImpl(px, py, data, poly_offset, size)) {
                return true;
            }
            offset = poly_offset;
        }
        return false;
    }

    return false;
}

// Extract bounding box from geometry
static bool ExtractGeometryBBox(const string_t &geom, double &min_x, double &min_y, double &max_x, double &max_y) {
    const uint8_t *data = reinterpret_cast<const uint8_t*>(geom.GetData());
    idx_t size = geom.GetSize();

    if (size < 5) return false;

    uint32_t geom_type;
    memcpy(&geom_type, data + 1, 4);

    uint32_t base_type = geom_type & 0xFF;
    idx_t offset = 5;

    // Handle SRID prefix
    if (geom_type & 0x20000000) {
        offset += 4;
    }

    min_x = std::numeric_limits<double>::max();
    min_y = std::numeric_limits<double>::max();
    max_x = std::numeric_limits<double>::lowest();
    max_y = std::numeric_limits<double>::lowest();

    auto update_bounds = [&](double x, double y) {
        if (x < min_x) min_x = x;
        if (x > max_x) max_x = x;
        if (y < min_y) min_y = y;
        if (y > max_y) max_y = y;
    };

    if (base_type == 3) {  // POLYGON
        if (size < offset + 4) return false;
        uint32_t num_rings;
        memcpy(&num_rings, data + offset, 4);
        offset += 4;

        for (uint32_t r = 0; r < num_rings; r++) {
            if (size < offset + 4) return false;
            uint32_t num_points;
            memcpy(&num_points, data + offset, 4);
            offset += 4;

            if (size < offset + num_points * 16) return false;
            for (uint32_t p = 0; p < num_points; p++) {
                double x, y;
                memcpy(&x, data + offset + p * 16, 8);
                memcpy(&y, data + offset + p * 16 + 8, 8);
                update_bounds(x, y);
            }
            offset += num_points * 16;
        }
        return true;
    } else if (base_type == 6) {  // MULTIPOLYGON
        if (size < offset + 4) return false;
        uint32_t num_polygons;
        memcpy(&num_polygons, data + offset, 4);
        offset += 4;

        for (uint32_t poly = 0; poly < num_polygons; poly++) {
            if (size < offset + 5) return false;
            offset += 5;  // Skip WKB header

            uint32_t poly_type;
            memcpy(&poly_type, data + offset - 4, 4);
            if (poly_type & 0x20000000) {
                offset += 4;
            }

            if (size < offset + 4) return false;
            uint32_t num_rings;
            memcpy(&num_rings, data + offset, 4);
            offset += 4;

            for (uint32_t r = 0; r < num_rings; r++) {
                if (size < offset + 4) return false;
                uint32_t num_points;
                memcpy(&num_points, data + offset, 4);
                offset += 4;

                if (size < offset + num_points * 16) return false;
                for (uint32_t p = 0; p < num_points; p++) {
                    double x, y;
                    memcpy(&x, data + offset + p * 16, 8);
                    memcpy(&y, data + offset + p * 16 + 8, 8);
                    update_bounds(x, y);
                }
                offset += num_points * 16;
            }
        }
        return true;
    }

    return false;
}

// Check if region fully contains tile (optimization to skip per-pixel tests)
static bool RegionContainsTile(const string_t &region, double tile_min_lon, double tile_min_lat,
                               double tile_max_lon, double tile_max_lat) {
    return PointInGeometry(tile_min_lon, tile_min_lat, region) &&
           PointInGeometry(tile_max_lon, tile_min_lat, region) &&
           PointInGeometry(tile_min_lon, tile_max_lat, region) &&
           PointInGeometry(tile_max_lon, tile_max_lat, region);
}

// ============================================================================
// Resolution Selection
// ============================================================================

enum class ResolutionMode { EXPLICIT, AUTO, MAX };

struct ResolutionConfig {
    ResolutionMode mode;
    int explicit_level;
    int auto_max_tiles;  // Target max tiles for AUTO mode

    ResolutionConfig() : mode(ResolutionMode::MAX), explicit_level(-1), auto_max_tiles(50) {}
};

// Parse resolution parameter: integer string, 'auto', or 'max'
static ResolutionConfig ParseResolution(const std::string &resolution_str, int max_resolution) {
    ResolutionConfig config;

    if (resolution_str == "auto") {
        config.mode = ResolutionMode::AUTO;
    } else if (resolution_str == "max" || resolution_str.empty()) {
        config.mode = ResolutionMode::MAX;
        config.explicit_level = max_resolution;
    } else {
        // Try to parse as integer
        try {
            config.mode = ResolutionMode::EXPLICIT;
            config.explicit_level = std::stoi(resolution_str);
            if (config.explicit_level < 0 || config.explicit_level > max_resolution) {
                throw InvalidInputException("ST_RegionStats: Resolution must be between 0 and %d, got %d",
                                            max_resolution, config.explicit_level);
            }
        } catch (const std::invalid_argument &) {
            throw InvalidInputException("ST_RegionStats: Invalid resolution '%s'. Use integer (0-%d), 'auto', or 'max'",
                                        resolution_str.c_str(), max_resolution);
        }
    }
    return config;
}

// Estimate optimal resolution based on tile count
// At zoom Z, each tile covers: width = 360/2^Z degrees, height = 180/2^Z degrees
// Pick highest Z where estimated_tiles <= max_tiles
static int EstimateAutoResolution(double min_lon, double min_lat, double max_lon, double max_lat,
                                   int min_resolution, int max_resolution, int max_tiles = 50) {
    double bbox_width = max_lon - min_lon;
    double bbox_height = max_lat - min_lat;

    // Search from max resolution down to min
    for (int z = max_resolution; z >= min_resolution; z--) {
        double tile_width = 360.0 / (1 << z);   // 360 / 2^z
        double tile_height = 180.0 / (1 << z);  // 180 / 2^z

        int tiles_x = static_cast<int>(std::ceil(bbox_width / tile_width));
        int tiles_y = static_cast<int>(std::ceil(bbox_height / tile_height));
        int estimated_tiles = tiles_x * tiles_y;

        if (estimated_tiles <= max_tiles) {
            return z;
        }
    }

    return min_resolution;  // Fallback to coarsest
}

// ============================================================================
// ST_RegionStats Aggregate State
// ============================================================================

struct RegionStatsState {
    int64_t count;
    double sum;
    double mean;
    double m2;        // Welford's variance accumulator
    double min_val;
    double max_val;
};

// ============================================================================
// Metadata parsing
// ============================================================================

struct RaquetMetadata {
    std::string compression;
    int block_width;
    int block_height;
    int min_zoom;       // v0.3.0: was min_resolution
    int max_zoom;       // v0.3.0: was max_resolution
    std::vector<std::pair<std::string, std::string>> bands;

    static RaquetMetadata Parse(const std::string &json_str) {
        RaquetMetadata meta;
        meta.compression = "none";
        meta.block_width = 256;
        meta.block_height = 256;
        meta.min_zoom = 0;
        meta.max_zoom = 26;

        yyjson_doc *doc = yyjson_read(json_str.c_str(), json_str.length(), 0);
        if (!doc) {
            throw InvalidInputException("ST_RegionStats: Invalid metadata JSON");
        }

        yyjson_val *root = yyjson_doc_get_root(doc);
        if (!yyjson_is_obj(root)) {
            yyjson_doc_free(doc);
            throw InvalidInputException("ST_RegionStats: Metadata must be a JSON object");
        }

        yyjson_val *compression_val = yyjson_obj_get(root, "compression");
        if (compression_val && yyjson_is_str(compression_val)) {
            meta.compression = yyjson_get_str(compression_val);
        }

        // v0.3.0: Parse tiling object
        yyjson_val *tiling_val = yyjson_obj_get(root, "tiling");
        if (tiling_val && yyjson_is_obj(tiling_val)) {
            yyjson_val *width_val = yyjson_obj_get(tiling_val, "block_width");
            if (width_val && yyjson_is_int(width_val)) {
                meta.block_width = yyjson_get_int(width_val);
            }

            yyjson_val *height_val = yyjson_obj_get(tiling_val, "block_height");
            if (height_val && yyjson_is_int(height_val)) {
                meta.block_height = yyjson_get_int(height_val);
            }

            yyjson_val *minzoom_val = yyjson_obj_get(tiling_val, "min_zoom");
            if (minzoom_val && yyjson_is_int(minzoom_val)) {
                meta.min_zoom = yyjson_get_int(minzoom_val);
            }

            yyjson_val *maxzoom_val = yyjson_obj_get(tiling_val, "max_zoom");
            if (maxzoom_val && yyjson_is_int(maxzoom_val)) {
                meta.max_zoom = yyjson_get_int(maxzoom_val);
            }
        }

        // v0.3.0: bands are objects with name and type fields
        yyjson_val *bands_val = yyjson_obj_get(root, "bands");
        if (bands_val && yyjson_is_arr(bands_val)) {
            size_t idx, max;
            yyjson_val *band;
            yyjson_arr_foreach(bands_val, idx, max, band) {
                if (yyjson_is_obj(band)) {
                    yyjson_val *name_val = yyjson_obj_get(band, "name");
                    yyjson_val *dtype_val = yyjson_obj_get(band, "type");
                    if (name_val && dtype_val && yyjson_is_str(name_val) && yyjson_is_str(dtype_val)) {
                        meta.bands.emplace_back(yyjson_get_str(name_val), yyjson_get_str(dtype_val));
                    }
                }
            }
        }

        yyjson_doc_free(doc);
        return meta;
    }
};

// ============================================================================
// ST_RegionStats Aggregate Function Implementation
// ============================================================================

struct RegionStatsBindData : public FunctionData {
    bool has_nodata;
    double nodata;
    bool has_resolution;
    std::string resolution_str;

    RegionStatsBindData()
        : has_nodata(false), nodata(0.0), has_resolution(false), resolution_str("max") {}

    RegionStatsBindData(bool has_nodata_p, double nodata_p, bool has_resolution_p, const std::string &resolution_p)
        : has_nodata(has_nodata_p), nodata(nodata_p), has_resolution(has_resolution_p), resolution_str(resolution_p) {}

    unique_ptr<FunctionData> Copy() const override {
        return make_uniq<RegionStatsBindData>(has_nodata, nodata, has_resolution, resolution_str);
    }

    bool Equals(const FunctionData &other_p) const override {
        auto &other = other_p.Cast<RegionStatsBindData>();
        return has_nodata == other.has_nodata && nodata == other.nodata &&
               has_resolution == other.has_resolution && resolution_str == other.resolution_str;
    }
};

static idx_t RegionStatsStateSize(const AggregateFunction &) {
    return sizeof(RegionStatsState);
}

static void RegionStatsStateInitialize(const AggregateFunction &, data_ptr_t state) {
    auto &s = *reinterpret_cast<RegionStatsState *>(state);
    s.count = 0;
    s.sum = 0.0;
    s.mean = 0.0;
    s.m2 = 0.0;
    s.min_val = std::numeric_limits<double>::max();
    s.max_val = std::numeric_limits<double>::lowest();
}

static void RegionStatsCombine(Vector &source, Vector &target, AggregateInputData &aggr_input_data, idx_t count) {
    auto source_data = FlatVector::GetData<RegionStatsState *>(source);
    auto target_data = FlatVector::GetData<RegionStatsState *>(target);

    for (idx_t i = 0; i < count; i++) {
        auto &src = *source_data[i];
        auto &tgt = *target_data[i];

        if (src.count == 0) continue;

        if (tgt.count == 0) {
            tgt = src;
            continue;
        }

        // Parallel Welford's merge
        int64_t combined_count = tgt.count + src.count;
        double delta = src.mean - tgt.mean;
        double combined_mean = tgt.mean + delta * src.count / combined_count;
        double combined_m2 = tgt.m2 + src.m2 + delta * delta * tgt.count * src.count / combined_count;

        tgt.count = combined_count;
        tgt.sum += src.sum;
        tgt.mean = combined_mean;
        tgt.m2 = combined_m2;

        if (src.min_val < tgt.min_val) tgt.min_val = src.min_val;
        if (src.max_val > tgt.max_val) tgt.max_val = src.max_val;
    }
}

static void RegionStatsFinalize(Vector &states, AggregateInputData &aggr_input_data, Vector &result,
                                 idx_t count, idx_t offset) {
    auto state_data = FlatVector::GetData<RegionStatsState *>(states);
    auto &struct_entries = StructVector::GetEntries(result);

    auto count_data = FlatVector::GetData<int64_t>(*struct_entries[0]);
    auto sum_data = FlatVector::GetData<double>(*struct_entries[1]);
    auto mean_data = FlatVector::GetData<double>(*struct_entries[2]);
    auto min_data = FlatVector::GetData<double>(*struct_entries[3]);
    auto max_data = FlatVector::GetData<double>(*struct_entries[4]);
    auto stddev_data = FlatVector::GetData<double>(*struct_entries[5]);

    auto &result_validity = FlatVector::Validity(result);

    for (idx_t i = 0; i < count; i++) {
        auto &state = *state_data[i];
        idx_t result_idx = offset + i;

        if (state.count == 0) {
            result_validity.SetInvalid(result_idx);
            continue;
        }

        count_data[result_idx] = state.count;
        sum_data[result_idx] = state.sum;
        mean_data[result_idx] = state.sum / state.count;
        min_data[result_idx] = state.min_val;
        max_data[result_idx] = state.max_val;

        if (state.count > 1) {
            stddev_data[result_idx] = std::sqrt(state.m2 / (state.count - 1));
        } else {
            stddev_data[result_idx] = 0.0;
        }
    }
}

// Helper function to process a tile for region stats
static void ProcessTileForRegionStats(RegionStatsState &state, const string_t &band, uint64_t block,
                                       const string_t &region, const RaquetMetadata &meta,
                                       bool has_nodata, double nodata, int target_resolution) {
    if (band.GetSize() == 0) return;
    if (meta.bands.empty()) return;

    // Get tile resolution and filter
    int tile_x, tile_y, tile_z;
    quadbin::cell_to_tile(block, tile_x, tile_y, tile_z);

    // Skip tiles not at target resolution (if specified, -1 means no filter)
    if (target_resolution >= 0 && tile_z != target_resolution) {
        return;
    }

    std::string dtype = meta.bands[0].second;
    bool compressed = (meta.compression == "gzip");
    int width = meta.block_width;
    int height = meta.block_height;

    double tile_min_lon, tile_min_lat, tile_max_lon, tile_max_lat;
    quadbin::tile_to_bbox_wgs84(tile_x, tile_y, tile_z, tile_min_lon, tile_min_lat, tile_max_lon, tile_max_lat);

    // Get region bbox for quick filtering
    double region_min_lon, region_min_lat, region_max_lon, region_max_lat;
    if (!ExtractGeometryBBox(region, region_min_lon, region_min_lat, region_max_lon, region_max_lat)) {
        return;
    }

    // Fast reject: if tile bbox doesn't intersect region bbox, skip
    if (tile_max_lon < region_min_lon || tile_min_lon > region_max_lon ||
        tile_max_lat < region_min_lat || tile_min_lat > region_max_lat) {
        return;
    }

    // Decompress band data if needed
    const uint8_t *raw_data;
    std::vector<uint8_t> decompressed;

    if (compressed) {
        decompressed = raquet::decompress_gzip(
            reinterpret_cast<const uint8_t*>(band.GetData()),
            band.GetSize()
        );
        raw_data = decompressed.data();
    } else {
        raw_data = reinterpret_cast<const uint8_t*>(band.GetData());
    }

    auto band_dtype = raquet::parse_dtype(dtype);

    // Optimization: Check if region fully contains tile
    bool full_tile = RegionContainsTile(region, tile_min_lon, tile_min_lat, tile_max_lon, tile_max_lat);

    // Calculate pixel dimensions
    double pixel_width = (tile_max_lon - tile_min_lon) / width;
    double pixel_height = (tile_max_lat - tile_min_lat) / height;

    // Process pixels
    for (int py = 0; py < height; py++) {
        for (int px = 0; px < width; px++) {
            // Calculate pixel center coordinates
            double pixel_lon = tile_min_lon + (px + 0.5) * pixel_width;
            double pixel_lat = tile_max_lat - (py + 0.5) * pixel_height;

            // Check if pixel is in region
            bool in_region = full_tile || PointInGeometry(pixel_lon, pixel_lat, region);

            if (in_region) {
                size_t offset = static_cast<size_t>(py) * width + px;
                double value = raquet::get_pixel_value(raw_data, offset, band_dtype);

                // Skip nodata values
                if (has_nodata && value == nodata) {
                    continue;
                }

                // Welford's algorithm update
                state.count++;
                state.sum += value;

                if (value < state.min_val) state.min_val = value;
                if (value > state.max_val) state.max_val = value;

                double delta = value - state.mean;
                state.mean += delta / state.count;
                double delta2 = value - state.mean;
                state.m2 += delta * delta2;
            }
        }
    }
}

// Update function for 4-argument version
static void RegionStatsUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                               Vector &state_vector, idx_t count) {
    inputs[0].Flatten(count);
    inputs[1].Flatten(count);
    inputs[2].Flatten(count);
    inputs[3].Flatten(count);

    auto band_data = FlatVector::GetData<string_t>(inputs[0]);
    auto block_data = FlatVector::GetData<uint64_t>(inputs[1]);
    auto region_data = FlatVector::GetData<string_t>(inputs[2]);
    auto metadata_data = FlatVector::GetData<string_t>(inputs[3]);

    auto &band_validity = FlatVector::Validity(inputs[0]);
    auto &region_validity = FlatVector::Validity(inputs[2]);

    auto states = FlatVector::GetData<RegionStatsState *>(state_vector);

    for (idx_t i = 0; i < count; i++) {
        if (!band_validity.RowIsValid(i) || !region_validity.RowIsValid(i)) {
            continue;
        }

        auto &state = *states[i];

        try {
            auto meta = RaquetMetadata::Parse(metadata_data[i].GetString());
            // Default: use max resolution (no filtering, -1 = all resolutions pass through,
            // but we actually want only max resolution for backward compatibility)
            int target_res = meta.max_zoom;
            ProcessTileForRegionStats(state, band_data[i], block_data[i], region_data[i], meta, false, 0.0, target_res);
        } catch (...) {
            // Skip tiles with errors
            continue;
        }
    }
}

// Update function for 5-argument version (with nodata)
static void RegionStatsUpdateNodata(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                                     Vector &state_vector, idx_t count) {
    inputs[0].Flatten(count);
    inputs[1].Flatten(count);
    inputs[2].Flatten(count);
    inputs[3].Flatten(count);
    inputs[4].Flatten(count);

    auto band_data = FlatVector::GetData<string_t>(inputs[0]);
    auto block_data = FlatVector::GetData<uint64_t>(inputs[1]);
    auto region_data = FlatVector::GetData<string_t>(inputs[2]);
    auto metadata_data = FlatVector::GetData<string_t>(inputs[3]);
    auto nodata_data = FlatVector::GetData<double>(inputs[4]);

    auto &band_validity = FlatVector::Validity(inputs[0]);
    auto &region_validity = FlatVector::Validity(inputs[2]);

    auto states = FlatVector::GetData<RegionStatsState *>(state_vector);

    for (idx_t i = 0; i < count; i++) {
        if (!band_validity.RowIsValid(i) || !region_validity.RowIsValid(i)) {
            continue;
        }

        auto &state = *states[i];
        double nodata = nodata_data[i];
        bool has_nodata = !std::isnan(nodata);

        try {
            auto meta = RaquetMetadata::Parse(metadata_data[i].GetString());
            int target_res = meta.max_zoom;
            ProcessTileForRegionStats(state, band_data[i], block_data[i], region_data[i], meta, has_nodata, nodata, target_res);
        } catch (...) {
            continue;
        }
    }
}

// Helper to compute target resolution from string and geometry
static int ComputeTargetResolution(const std::string &resolution_str, const string_t &region,
                                    const RaquetMetadata &meta) {
    auto config = ParseResolution(resolution_str, meta.max_zoom);

    if (config.mode == ResolutionMode::MAX) {
        return meta.max_zoom;
    } else if (config.mode == ResolutionMode::EXPLICIT) {
        return config.explicit_level;
    } else {  // AUTO
        double min_lon, min_lat, max_lon, max_lat;
        if (!ExtractGeometryBBox(region, min_lon, min_lat, max_lon, max_lat)) {
            return meta.max_zoom;  // Fallback
        }
        return EstimateAutoResolution(min_lon, min_lat, max_lon, max_lat,
                                       meta.min_zoom, meta.max_zoom);
    }
}

// Update function for 5-argument version (with resolution)
static void RegionStatsUpdateResolution(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                                         Vector &state_vector, idx_t count) {
    inputs[0].Flatten(count);
    inputs[1].Flatten(count);
    inputs[2].Flatten(count);
    inputs[3].Flatten(count);
    inputs[4].Flatten(count);

    auto band_data = FlatVector::GetData<string_t>(inputs[0]);
    auto block_data = FlatVector::GetData<uint64_t>(inputs[1]);
    auto region_data = FlatVector::GetData<string_t>(inputs[2]);
    auto metadata_data = FlatVector::GetData<string_t>(inputs[3]);
    auto resolution_data = FlatVector::GetData<string_t>(inputs[4]);

    auto &band_validity = FlatVector::Validity(inputs[0]);
    auto &region_validity = FlatVector::Validity(inputs[2]);

    auto states = FlatVector::GetData<RegionStatsState *>(state_vector);

    for (idx_t i = 0; i < count; i++) {
        if (!band_validity.RowIsValid(i) || !region_validity.RowIsValid(i)) {
            continue;
        }

        auto &state = *states[i];

        try {
            auto meta = RaquetMetadata::Parse(metadata_data[i].GetString());
            std::string res_str = resolution_data[i].GetString();
            int target_res = ComputeTargetResolution(res_str, region_data[i], meta);
            ProcessTileForRegionStats(state, band_data[i], block_data[i], region_data[i], meta, false, 0.0, target_res);
        } catch (...) {
            continue;
        }
    }
}

// Update function for 6-argument version (with nodata + resolution)
static void RegionStatsUpdateNodataResolution(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                                               Vector &state_vector, idx_t count) {
    inputs[0].Flatten(count);
    inputs[1].Flatten(count);
    inputs[2].Flatten(count);
    inputs[3].Flatten(count);
    inputs[4].Flatten(count);
    inputs[5].Flatten(count);

    auto band_data = FlatVector::GetData<string_t>(inputs[0]);
    auto block_data = FlatVector::GetData<uint64_t>(inputs[1]);
    auto region_data = FlatVector::GetData<string_t>(inputs[2]);
    auto metadata_data = FlatVector::GetData<string_t>(inputs[3]);
    auto nodata_data = FlatVector::GetData<double>(inputs[4]);
    auto resolution_data = FlatVector::GetData<string_t>(inputs[5]);

    auto &band_validity = FlatVector::Validity(inputs[0]);
    auto &region_validity = FlatVector::Validity(inputs[2]);

    auto states = FlatVector::GetData<RegionStatsState *>(state_vector);

    for (idx_t i = 0; i < count; i++) {
        if (!band_validity.RowIsValid(i) || !region_validity.RowIsValid(i)) {
            continue;
        }

        auto &state = *states[i];
        double nodata = nodata_data[i];
        bool has_nodata = !std::isnan(nodata);

        try {
            auto meta = RaquetMetadata::Parse(metadata_data[i].GetString());
            std::string res_str = resolution_data[i].GetString();
            int target_res = ComputeTargetResolution(res_str, region_data[i], meta);
            ProcessTileForRegionStats(state, band_data[i], block_data[i], region_data[i], meta, has_nodata, nodata, target_res);
        } catch (...) {
            continue;
        }
    }
}

static unique_ptr<FunctionData> RegionStatsBind(ClientContext &context, AggregateFunction &function,
                                                 vector<unique_ptr<Expression>> &arguments) {
    return make_uniq<RegionStatsBindData>();
}

void RegisterRegionStatsFunctions(ExtensionLoader &loader) {
    // Define the stats struct type
    child_list_t<LogicalType> stats_struct;
    stats_struct.push_back(make_pair("count", LogicalType::BIGINT));
    stats_struct.push_back(make_pair("sum", LogicalType::DOUBLE));
    stats_struct.push_back(make_pair("mean", LogicalType::DOUBLE));
    stats_struct.push_back(make_pair("min", LogicalType::DOUBLE));
    stats_struct.push_back(make_pair("max", LogicalType::DOUBLE));
    stats_struct.push_back(make_pair("stddev", LogicalType::DOUBLE));
    auto stats_type = LogicalType::STRUCT(stats_struct);

    AggregateFunctionSet region_stats_set("ST_RegionStats");

    // ST_RegionStats(band BLOB, block UBIGINT, region GEOMETRY, metadata VARCHAR)
    AggregateFunction region_stats(
        {LogicalType::BLOB, LogicalType::UBIGINT, LogicalType::GEOMETRY(), LogicalType::VARCHAR},
        stats_type,
        RegionStatsStateSize,
        RegionStatsStateInitialize,
        RegionStatsUpdate,
        RegionStatsCombine,
        RegionStatsFinalize,
        FunctionNullHandling::DEFAULT_NULL_HANDLING,
        nullptr,  // simple_update
        RegionStatsBind
    );
    region_stats_set.AddFunction(region_stats);

    // ST_RegionStats(band BLOB, block UBIGINT, region GEOMETRY, metadata VARCHAR, nodata DOUBLE)
    AggregateFunction region_stats_nodata(
        {LogicalType::BLOB, LogicalType::UBIGINT, LogicalType::GEOMETRY(), LogicalType::VARCHAR, LogicalType::DOUBLE},
        stats_type,
        RegionStatsStateSize,
        RegionStatsStateInitialize,
        RegionStatsUpdateNodata,
        RegionStatsCombine,
        RegionStatsFinalize,
        FunctionNullHandling::DEFAULT_NULL_HANDLING,
        nullptr,  // simple_update
        RegionStatsBind
    );
    region_stats_set.AddFunction(region_stats_nodata);

    // ST_RegionStats(band BLOB, block UBIGINT, region GEOMETRY, metadata VARCHAR, resolution VARCHAR)
    // resolution can be: integer (0-26), 'auto', or 'max'
    AggregateFunction region_stats_resolution(
        {LogicalType::BLOB, LogicalType::UBIGINT, LogicalType::GEOMETRY(), LogicalType::VARCHAR, LogicalType::VARCHAR},
        stats_type,
        RegionStatsStateSize,
        RegionStatsStateInitialize,
        RegionStatsUpdateResolution,
        RegionStatsCombine,
        RegionStatsFinalize,
        FunctionNullHandling::DEFAULT_NULL_HANDLING,
        nullptr,
        RegionStatsBind
    );
    region_stats_set.AddFunction(region_stats_resolution);

    // ST_RegionStats(band BLOB, block UBIGINT, region GEOMETRY, metadata VARCHAR, nodata DOUBLE, resolution VARCHAR)
    AggregateFunction region_stats_nodata_resolution(
        {LogicalType::BLOB, LogicalType::UBIGINT, LogicalType::GEOMETRY(), LogicalType::VARCHAR, LogicalType::DOUBLE, LogicalType::VARCHAR},
        stats_type,
        RegionStatsStateSize,
        RegionStatsStateInitialize,
        RegionStatsUpdateNodataResolution,
        RegionStatsCombine,
        RegionStatsFinalize,
        FunctionNullHandling::DEFAULT_NULL_HANDLING,
        nullptr,
        RegionStatsBind
    );
    region_stats_set.AddFunction(region_stats_nodata_resolution);

    loader.RegisterFunction(region_stats_set);
}

} // namespace duckdb
