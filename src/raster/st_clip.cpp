#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
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
static bool ClipPointInRing(double px, double py, const double* ring_coords, uint32_t num_points) {
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
static bool ClipPointInPolygonImpl(double px, double py, const uint8_t* data, idx_t& offset, idx_t size) {
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
    if (!ClipPointInRing(px, py, outer_coords.data(), outer_points)) {
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

        if (ClipPointInRing(px, py, hole_coords.data(), hole_points)) {
            return false;  // Inside a hole
        }
    }

    return true;
}

// Check if a point is inside a geometry
static bool ClipPointInGeometry(double px, double py, const string_t &geom) {
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
        return ClipPointInPolygonImpl(px, py, data, offset, size);
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
            if (ClipPointInPolygonImpl(px, py, data, poly_offset, size)) {
                return true;
            }
            offset = poly_offset;
        }
        return false;
    }

    return false;
}

// Extract bounding box from geometry
static bool ClipExtractGeometryBBox(const string_t &geom, double &min_x, double &min_y, double &max_x, double &max_y) {
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

// ============================================================================
// Metadata parsing
// ============================================================================

struct ClipMetadata {
    std::string compression;
    int block_width;
    int block_height;
    std::vector<std::pair<std::string, std::string>> bands;

    static ClipMetadata Parse(const std::string &json_str) {
        ClipMetadata meta;
        meta.compression = "none";
        meta.block_width = 256;
        meta.block_height = 256;

        yyjson_doc *doc = yyjson_read(json_str.c_str(), json_str.length(), 0);
        if (!doc) {
            throw InvalidInputException("ST_Clip: Invalid metadata JSON");
        }

        yyjson_val *root = yyjson_doc_get_root(doc);
        if (!yyjson_is_obj(root)) {
            yyjson_doc_free(doc);
            throw InvalidInputException("ST_Clip: Metadata must be a JSON object");
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
// ST_Clip(band, block, clip_geometry, metadata) -> DOUBLE[]
// Returns pixel values within the clipping geometry
// Pixels outside the geometry are excluded from the result
// ============================================================================

static void STClipFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    args.data[0].Flatten(args.size());
    args.data[1].Flatten(args.size());
    args.data[2].Flatten(args.size());
    args.data[3].Flatten(args.size());

    auto band_data = FlatVector::GetData<string_t>(args.data[0]);
    auto block_data = FlatVector::GetData<uint64_t>(args.data[1]);
    auto clip_geom_data = FlatVector::GetData<string_t>(args.data[2]);
    auto metadata_data = FlatVector::GetData<string_t>(args.data[3]);

    auto &band_validity = FlatVector::Validity(args.data[0]);
    auto &clip_validity = FlatVector::Validity(args.data[2]);

    auto list_data = ListVector::GetData(result);
    auto &list_child = ListVector::GetEntry(result);

    idx_t total_list_size = 0;

    for (idx_t i = 0; i < args.size(); i++) {
        if (!band_validity.RowIsValid(i) || !clip_validity.RowIsValid(i)) {
            list_data[i].offset = total_list_size;
            list_data[i].length = 0;
            FlatVector::Validity(result).SetInvalid(i);
            continue;
        }

        auto band = band_data[i];
        auto block = block_data[i];
        auto clip_geom = clip_geom_data[i];
        auto metadata_str = metadata_data[i].GetString();

        if (band.GetSize() == 0) {
            list_data[i].offset = total_list_size;
            list_data[i].length = 0;
            FlatVector::Validity(result).SetInvalid(i);
            continue;
        }

        try {
            auto meta = ClipMetadata::Parse(metadata_str);
            std::string dtype = meta.bands.empty() ? "float32" : meta.bands[0].second;
            bool compressed = (meta.compression == "gzip");
            int width = meta.block_width;
            int height = meta.block_height;

            // Get tile bounds from block
            int tile_x, tile_y, tile_z;
            quadbin::cell_to_tile(block, tile_x, tile_y, tile_z);

            double tile_min_lon, tile_min_lat, tile_max_lon, tile_max_lat;
            quadbin::tile_to_bbox_wgs84(tile_x, tile_y, tile_z, tile_min_lon, tile_min_lat, tile_max_lon, tile_max_lat);

            // Get clip geometry bbox for quick filtering
            double clip_min_lon, clip_min_lat, clip_max_lon, clip_max_lat;
            if (!ClipExtractGeometryBBox(clip_geom, clip_min_lon, clip_min_lat, clip_max_lon, clip_max_lat)) {
                list_data[i].offset = total_list_size;
                list_data[i].length = 0;
                continue;
            }

            // Fast reject: if tile bbox doesn't intersect clip bbox, return empty
            if (tile_max_lon < clip_min_lon || tile_min_lon > clip_max_lon ||
                tile_max_lat < clip_min_lat || tile_min_lat > clip_max_lat) {
                list_data[i].offset = total_list_size;
                list_data[i].length = 0;
                continue;
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

            // Calculate pixel dimensions
            double pixel_width = (tile_max_lon - tile_min_lon) / width;
            double pixel_height = (tile_max_lat - tile_min_lat) / height;

            // Collect pixels inside the clip geometry
            std::vector<double> clipped_values;
            clipped_values.reserve(static_cast<size_t>(width) * height / 4);  // Rough estimate

            for (int py = 0; py < height; py++) {
                for (int px = 0; px < width; px++) {
                    // Calculate pixel center coordinates
                    double pixel_lon = tile_min_lon + (px + 0.5) * pixel_width;
                    double pixel_lat = tile_max_lat - (py + 0.5) * pixel_height;

                    // Quick bbox check for pixel
                    if (pixel_lon < clip_min_lon || pixel_lon > clip_max_lon ||
                        pixel_lat < clip_min_lat || pixel_lat > clip_max_lat) {
                        continue;
                    }

                    // Check if pixel center is inside clip geometry
                    if (ClipPointInGeometry(pixel_lon, pixel_lat, clip_geom)) {
                        size_t offset = static_cast<size_t>(py) * width + px;
                        double value = raquet::get_pixel_value(raw_data, offset, band_dtype);
                        clipped_values.push_back(value);
                    }
                }
            }

            // Write to result list
            ListVector::Reserve(result, total_list_size + clipped_values.size());
            auto child_data = FlatVector::GetData<double>(ListVector::GetEntry(result));

            for (size_t j = 0; j < clipped_values.size(); j++) {
                child_data[total_list_size + j] = clipped_values[j];
            }

            list_data[i].offset = total_list_size;
            list_data[i].length = clipped_values.size();
            total_list_size += clipped_values.size();

        } catch (const std::exception &e) {
            list_data[i].offset = total_list_size;
            list_data[i].length = 0;
            FlatVector::Validity(result).SetInvalid(i);
        }
    }

    ListVector::SetListSize(result, total_list_size);
}

// ============================================================================
// ST_Clip with nodata handling
// ST_Clip(band, block, clip_geometry, metadata, nodata) -> DOUBLE[]
// ============================================================================

static void STClipNodataFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    args.data[0].Flatten(args.size());
    args.data[1].Flatten(args.size());
    args.data[2].Flatten(args.size());
    args.data[3].Flatten(args.size());
    args.data[4].Flatten(args.size());

    auto band_data = FlatVector::GetData<string_t>(args.data[0]);
    auto block_data = FlatVector::GetData<uint64_t>(args.data[1]);
    auto clip_geom_data = FlatVector::GetData<string_t>(args.data[2]);
    auto metadata_data = FlatVector::GetData<string_t>(args.data[3]);
    auto nodata_data = FlatVector::GetData<double>(args.data[4]);

    auto &band_validity = FlatVector::Validity(args.data[0]);
    auto &clip_validity = FlatVector::Validity(args.data[2]);

    auto list_data = ListVector::GetData(result);

    idx_t total_list_size = 0;

    for (idx_t i = 0; i < args.size(); i++) {
        if (!band_validity.RowIsValid(i) || !clip_validity.RowIsValid(i)) {
            list_data[i].offset = total_list_size;
            list_data[i].length = 0;
            FlatVector::Validity(result).SetInvalid(i);
            continue;
        }

        auto band = band_data[i];
        auto block = block_data[i];
        auto clip_geom = clip_geom_data[i];
        auto metadata_str = metadata_data[i].GetString();
        double nodata = nodata_data[i];

        if (band.GetSize() == 0) {
            list_data[i].offset = total_list_size;
            list_data[i].length = 0;
            FlatVector::Validity(result).SetInvalid(i);
            continue;
        }

        try {
            auto meta = ClipMetadata::Parse(metadata_str);
            std::string dtype = meta.bands.empty() ? "float32" : meta.bands[0].second;
            bool compressed = (meta.compression == "gzip");
            int width = meta.block_width;
            int height = meta.block_height;

            int tile_x, tile_y, tile_z;
            quadbin::cell_to_tile(block, tile_x, tile_y, tile_z);

            double tile_min_lon, tile_min_lat, tile_max_lon, tile_max_lat;
            quadbin::tile_to_bbox_wgs84(tile_x, tile_y, tile_z, tile_min_lon, tile_min_lat, tile_max_lon, tile_max_lat);

            double clip_min_lon, clip_min_lat, clip_max_lon, clip_max_lat;
            if (!ClipExtractGeometryBBox(clip_geom, clip_min_lon, clip_min_lat, clip_max_lon, clip_max_lat)) {
                list_data[i].offset = total_list_size;
                list_data[i].length = 0;
                continue;
            }

            if (tile_max_lon < clip_min_lon || tile_min_lon > clip_max_lon ||
                tile_max_lat < clip_min_lat || tile_min_lat > clip_max_lat) {
                list_data[i].offset = total_list_size;
                list_data[i].length = 0;
                continue;
            }

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

            double pixel_width = (tile_max_lon - tile_min_lon) / width;
            double pixel_height = (tile_max_lat - tile_min_lat) / height;

            std::vector<double> clipped_values;
            clipped_values.reserve(static_cast<size_t>(width) * height / 4);

            for (int py = 0; py < height; py++) {
                for (int px = 0; px < width; px++) {
                    double pixel_lon = tile_min_lon + (px + 0.5) * pixel_width;
                    double pixel_lat = tile_max_lat - (py + 0.5) * pixel_height;

                    if (pixel_lon < clip_min_lon || pixel_lon > clip_max_lon ||
                        pixel_lat < clip_min_lat || pixel_lat > clip_max_lat) {
                        continue;
                    }

                    if (ClipPointInGeometry(pixel_lon, pixel_lat, clip_geom)) {
                        size_t offset = static_cast<size_t>(py) * width + px;
                        double value = raquet::get_pixel_value(raw_data, offset, band_dtype);

                        // Skip nodata values
                        if (value != nodata) {
                            clipped_values.push_back(value);
                        }
                    }
                }
            }

            ListVector::Reserve(result, total_list_size + clipped_values.size());
            auto child_data = FlatVector::GetData<double>(ListVector::GetEntry(result));

            for (size_t j = 0; j < clipped_values.size(); j++) {
                child_data[total_list_size + j] = clipped_values[j];
            }

            list_data[i].offset = total_list_size;
            list_data[i].length = clipped_values.size();
            total_list_size += clipped_values.size();

        } catch (const std::exception &e) {
            list_data[i].offset = total_list_size;
            list_data[i].length = 0;
            FlatVector::Validity(result).SetInvalid(i);
        }
    }

    ListVector::SetListSize(result, total_list_size);
}

// ============================================================================
// ST_ClipMask - Returns a full tile with pixels outside geometry set to nodata
// ST_ClipMask(band, block, clip_geometry, metadata, nodata_value) -> DOUBLE[]
// ============================================================================

static void STClipMaskFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    args.data[0].Flatten(args.size());
    args.data[1].Flatten(args.size());
    args.data[2].Flatten(args.size());
    args.data[3].Flatten(args.size());
    args.data[4].Flatten(args.size());

    auto band_data = FlatVector::GetData<string_t>(args.data[0]);
    auto block_data = FlatVector::GetData<uint64_t>(args.data[1]);
    auto clip_geom_data = FlatVector::GetData<string_t>(args.data[2]);
    auto metadata_data = FlatVector::GetData<string_t>(args.data[3]);
    auto nodata_data = FlatVector::GetData<double>(args.data[4]);

    auto &band_validity = FlatVector::Validity(args.data[0]);
    auto &clip_validity = FlatVector::Validity(args.data[2]);

    auto list_data = ListVector::GetData(result);

    idx_t total_list_size = 0;

    for (idx_t i = 0; i < args.size(); i++) {
        if (!band_validity.RowIsValid(i) || !clip_validity.RowIsValid(i)) {
            list_data[i].offset = total_list_size;
            list_data[i].length = 0;
            FlatVector::Validity(result).SetInvalid(i);
            continue;
        }

        auto band = band_data[i];
        auto block = block_data[i];
        auto clip_geom = clip_geom_data[i];
        auto metadata_str = metadata_data[i].GetString();
        double nodata_value = nodata_data[i];

        if (band.GetSize() == 0) {
            list_data[i].offset = total_list_size;
            list_data[i].length = 0;
            FlatVector::Validity(result).SetInvalid(i);
            continue;
        }

        try {
            auto meta = ClipMetadata::Parse(metadata_str);
            std::string dtype = meta.bands.empty() ? "float32" : meta.bands[0].second;
            bool compressed = (meta.compression == "gzip");
            int width = meta.block_width;
            int height = meta.block_height;
            size_t num_pixels = static_cast<size_t>(width) * height;

            int tile_x, tile_y, tile_z;
            quadbin::cell_to_tile(block, tile_x, tile_y, tile_z);

            double tile_min_lon, tile_min_lat, tile_max_lon, tile_max_lat;
            quadbin::tile_to_bbox_wgs84(tile_x, tile_y, tile_z, tile_min_lon, tile_min_lat, tile_max_lon, tile_max_lat);

            double clip_min_lon, clip_min_lat, clip_max_lon, clip_max_lat;
            bool has_clip_bbox = ClipExtractGeometryBBox(clip_geom, clip_min_lon, clip_min_lat, clip_max_lon, clip_max_lat);

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

            double pixel_width = (tile_max_lon - tile_min_lon) / width;
            double pixel_height = (tile_max_lat - tile_min_lat) / height;

            // Reserve space for full tile
            ListVector::Reserve(result, total_list_size + num_pixels);
            auto child_data = FlatVector::GetData<double>(ListVector::GetEntry(result));

            // Check if tile is completely outside clip geometry
            bool tile_outside = has_clip_bbox && (
                tile_max_lon < clip_min_lon || tile_min_lon > clip_max_lon ||
                tile_max_lat < clip_min_lat || tile_min_lat > clip_max_lat
            );

            for (int py = 0; py < height; py++) {
                for (int px = 0; px < width; px++) {
                    size_t pixel_idx = static_cast<size_t>(py) * width + px;
                    double value = raquet::get_pixel_value(raw_data, pixel_idx, band_dtype);

                    if (tile_outside) {
                        // Entire tile outside - all nodata
                        child_data[total_list_size + pixel_idx] = nodata_value;
                    } else {
                        double pixel_lon = tile_min_lon + (px + 0.5) * pixel_width;
                        double pixel_lat = tile_max_lat - (py + 0.5) * pixel_height;

                        // Quick bbox check
                        bool in_bbox = !has_clip_bbox || (
                            pixel_lon >= clip_min_lon && pixel_lon <= clip_max_lon &&
                            pixel_lat >= clip_min_lat && pixel_lat <= clip_max_lat
                        );

                        if (in_bbox && ClipPointInGeometry(pixel_lon, pixel_lat, clip_geom)) {
                            child_data[total_list_size + pixel_idx] = value;
                        } else {
                            child_data[total_list_size + pixel_idx] = nodata_value;
                        }
                    }
                }
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
// Function Registration
// ============================================================================

void RegisterClipFunctions(ExtensionLoader &loader) {
    // ST_Clip(band BLOB, block UBIGINT, clip_geometry GEOMETRY, metadata VARCHAR) -> DOUBLE[]
    // Returns only pixel values inside the clip geometry
    ScalarFunction clip_fn("ST_Clip",
        {LogicalType::BLOB, LogicalType::UBIGINT, LogicalType::GEOMETRY(), LogicalType::VARCHAR},
        LogicalType::LIST(LogicalType::DOUBLE),
        STClipFunction);
    loader.RegisterFunction(clip_fn);

    // ST_Clip(band, block, clip_geometry, metadata, nodata) -> DOUBLE[]
    // Returns pixel values inside geometry, excluding nodata values
    ScalarFunction clip_nodata_fn("ST_Clip",
        {LogicalType::BLOB, LogicalType::UBIGINT, LogicalType::GEOMETRY(), LogicalType::VARCHAR, LogicalType::DOUBLE},
        LogicalType::LIST(LogicalType::DOUBLE),
        STClipNodataFunction);
    loader.RegisterFunction(clip_nodata_fn);

    // ST_ClipMask(band, block, clip_geometry, metadata, nodata_value) -> DOUBLE[]
    // Returns full tile with pixels outside geometry set to nodata_value
    ScalarFunction clip_mask_fn("ST_ClipMask",
        {LogicalType::BLOB, LogicalType::UBIGINT, LogicalType::GEOMETRY(), LogicalType::VARCHAR, LogicalType::DOUBLE},
        LogicalType::LIST(LogicalType::DOUBLE),
        STClipMaskFunction);
    loader.RegisterFunction(clip_mask_fn);
}

} // namespace duckdb
