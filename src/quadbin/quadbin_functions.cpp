#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "quadbin.hpp"
#include <sstream>
#include <iomanip>
#include <vector>
#include <cstring>

namespace duckdb {

// ============================================================================
// Geometry helpers - extract coordinates from GEOMETRY (WKB) type
// ============================================================================

// Check if a point is inside a polygon ring using ray casting algorithm
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
static bool PointInPolygon(double px, double py, const uint8_t* data, idx_t& offset, idx_t size) {
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

// Check if a point is inside a geometry (works for POLYGON and MULTIPOLYGON)
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
        return PointInPolygon(px, py, data, offset, size);
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
            if (PointInPolygon(px, py, data, poly_offset, size)) {
                return true;
            }
            offset = poly_offset;
        }
        return false;
    }

    return false;
}

// Extract X and Y from a GEOMETRY POINT value
// Returns true on success, false if not a valid point
static bool ExtractPointCoordinates(const string_t &geom, double &x, double &y) {
    const uint8_t *data = reinterpret_cast<const uint8_t*>(geom.GetData());
    idx_t size = geom.GetSize();

    if (size < 21) return false;

    uint8_t byte_order = data[0];
    bool little_endian = (byte_order == 1);

    uint32_t geom_type;
    if (little_endian) {
        memcpy(&geom_type, data + 1, 4);
    } else {
        geom_type = (static_cast<uint32_t>(data[1]) << 24) |
                    (static_cast<uint32_t>(data[2]) << 16) |
                    (static_cast<uint32_t>(data[3]) << 8) |
                    static_cast<uint32_t>(data[4]);
    }

    uint32_t base_type = geom_type & 0xFF;
    if (base_type != 1) return false;

    idx_t coord_offset = 5;
    if (geom_type & 0x20000000) {
        coord_offset += 4;
        if (size < coord_offset + 16) return false;
    }

    if (little_endian) {
        memcpy(&x, data + coord_offset, 8);
        memcpy(&y, data + coord_offset + 8, 8);
    } else {
        uint64_t x_bits, y_bits;
        for (int i = 0; i < 8; i++) {
            reinterpret_cast<uint8_t*>(&x_bits)[7-i] = data[coord_offset + i];
            reinterpret_cast<uint8_t*>(&y_bits)[7-i] = data[coord_offset + 8 + i];
        }
        memcpy(&x, &x_bits, 8);
        memcpy(&y, &y_bits, 8);
    }

    return true;
}

// Extract bounding box from a GEOMETRY value (works for POINT, POLYGON, etc.)
// For POINT, min=max
static bool ExtractBoundingBox(const string_t &geom, double &min_x, double &min_y, double &max_x, double &max_y) {
    const uint8_t *data = reinterpret_cast<const uint8_t*>(geom.GetData());
    idx_t size = geom.GetSize();

    if (size < 21) return false;

    uint8_t byte_order = data[0];
    bool little_endian = (byte_order == 1);

    uint32_t geom_type;
    if (little_endian) {
        memcpy(&geom_type, data + 1, 4);
    } else {
        geom_type = (static_cast<uint32_t>(data[1]) << 24) |
                    (static_cast<uint32_t>(data[2]) << 16) |
                    (static_cast<uint32_t>(data[3]) << 8) |
                    static_cast<uint32_t>(data[4]);
    }

    uint32_t base_type = geom_type & 0xFF;
    idx_t offset = 5;

    // Handle SRID prefix
    if (geom_type & 0x20000000) {
        offset += 4;
    }

    if (base_type == 1) {  // POINT
        double x, y;
        if (!ExtractPointCoordinates(geom, x, y)) return false;
        min_x = max_x = x;
        min_y = max_y = y;
        return true;
    } else if (base_type == 3) {  // POLYGON
        // Read number of rings
        if (size < offset + 4) return false;
        uint32_t num_rings;
        if (little_endian) {
            memcpy(&num_rings, data + offset, 4);
        } else {
            num_rings = (static_cast<uint32_t>(data[offset]) << 24) |
                       (static_cast<uint32_t>(data[offset+1]) << 16) |
                       (static_cast<uint32_t>(data[offset+2]) << 8) |
                       static_cast<uint32_t>(data[offset+3]);
        }
        offset += 4;

        if (num_rings == 0) return false;

        // Read number of points in first ring
        if (size < offset + 4) return false;
        uint32_t num_points;
        if (little_endian) {
            memcpy(&num_points, data + offset, 4);
        } else {
            num_points = (static_cast<uint32_t>(data[offset]) << 24) |
                        (static_cast<uint32_t>(data[offset+1]) << 16) |
                        (static_cast<uint32_t>(data[offset+2]) << 8) |
                        static_cast<uint32_t>(data[offset+3]);
        }
        offset += 4;

        if (num_points == 0 || size < offset + num_points * 16) return false;

        // Initialize with first point
        if (little_endian) {
            memcpy(&min_x, data + offset, 8);
            memcpy(&min_y, data + offset + 8, 8);
        } else {
            return false; // Simplified: skip big endian for polygon
        }
        max_x = min_x;
        max_y = min_y;

        // Find bbox of all points
        for (uint32_t i = 1; i < num_points; i++) {
            double x, y;
            memcpy(&x, data + offset + i * 16, 8);
            memcpy(&y, data + offset + i * 16 + 8, 8);
            if (x < min_x) min_x = x;
            if (x > max_x) max_x = x;
            if (y < min_y) min_y = y;
            if (y > max_y) max_y = y;
        }
        return true;
    }

    return false;
}

// quadbin_from_tile(x, y, z) -> UBIGINT
static void QuadbinFromTileFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &x_vec = args.data[0];
    auto &y_vec = args.data[1];
    auto &z_vec = args.data[2];

    TernaryExecutor::Execute<int32_t, int32_t, int32_t, uint64_t>(
        x_vec, y_vec, z_vec, result, args.size(),
        [&](int32_t x, int32_t y, int32_t z) {
            return quadbin::tile_to_cell(x, y, z);
        });
}

// quadbin_to_tile(cell) -> STRUCT(x INT, y INT, z INT)
static void QuadbinToTileFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &cell_vec = args.data[0];

    // Result is a struct with x, y, z fields
    auto &struct_entries = StructVector::GetEntries(result);
    auto &x_result = *struct_entries[0];
    auto &y_result = *struct_entries[1];
    auto &z_result = *struct_entries[2];

    UnaryExecutor::Execute<uint64_t, int32_t>(cell_vec, x_result, args.size(),
        [](uint64_t cell) {
            int x, y, z;
            quadbin::cell_to_tile(cell, x, y, z);
            return x;
        });

    UnaryExecutor::Execute<uint64_t, int32_t>(cell_vec, y_result, args.size(),
        [](uint64_t cell) {
            int x, y, z;
            quadbin::cell_to_tile(cell, x, y, z);
            return y;
        });

    UnaryExecutor::Execute<uint64_t, int32_t>(cell_vec, z_result, args.size(),
        [](uint64_t cell) {
            int x, y, z;
            quadbin::cell_to_tile(cell, x, y, z);
            return z;
        });

    result.SetVectorType(VectorType::FLAT_VECTOR);
}

// quadbin_from_lonlat(lon, lat, resolution) -> UBIGINT
static void QuadbinFromLonLatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &lon_vec = args.data[0];
    auto &lat_vec = args.data[1];
    auto &res_vec = args.data[2];

    TernaryExecutor::Execute<double, double, int32_t, uint64_t>(
        lon_vec, lat_vec, res_vec, result, args.size(),
        [](double lon, double lat, int32_t resolution) {
            return quadbin::lonlat_to_cell(lon, lat, resolution);
        });
}

// quadbin_to_lonlat(cell) -> STRUCT(lon DOUBLE, lat DOUBLE)
static void QuadbinToLonLatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &cell_vec = args.data[0];

    auto &struct_entries = StructVector::GetEntries(result);
    auto &lon_result = *struct_entries[0];
    auto &lat_result = *struct_entries[1];

    UnaryExecutor::Execute<uint64_t, double>(cell_vec, lon_result, args.size(),
        [](uint64_t cell) {
            double lon, lat;
            quadbin::cell_to_lonlat(cell, lon, lat);
            return lon;
        });

    UnaryExecutor::Execute<uint64_t, double>(cell_vec, lat_result, args.size(),
        [](uint64_t cell) {
            double lon, lat;
            quadbin::cell_to_lonlat(cell, lon, lat);
            return lat;
        });

    result.SetVectorType(VectorType::FLAT_VECTOR);
}

// quadbin_resolution(cell) -> INT
static void QuadbinResolutionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &cell_vec = args.data[0];

    UnaryExecutor::Execute<uint64_t, int32_t>(cell_vec, result, args.size(),
        [](uint64_t cell) {
            return quadbin::cell_to_resolution(cell);
        });
}

// quadbin_to_bbox(cell) -> STRUCT(min_lon DOUBLE, min_lat DOUBLE, max_lon DOUBLE, max_lat DOUBLE)
static void QuadbinToBboxFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &cell_vec = args.data[0];

    auto &struct_entries = StructVector::GetEntries(result);
    auto &min_lon_result = *struct_entries[0];
    auto &min_lat_result = *struct_entries[1];
    auto &max_lon_result = *struct_entries[2];
    auto &max_lat_result = *struct_entries[3];

    for (idx_t i = 0; i < args.size(); i++) {
        auto cell = FlatVector::GetData<uint64_t>(cell_vec)[i];

        int x, y, z;
        quadbin::cell_to_tile(cell, x, y, z);

        double min_lon, min_lat, max_lon, max_lat;
        quadbin::tile_to_bbox_wgs84(x, y, z, min_lon, min_lat, max_lon, max_lat);

        FlatVector::GetData<double>(min_lon_result)[i] = min_lon;
        FlatVector::GetData<double>(min_lat_result)[i] = min_lat;
        FlatVector::GetData<double>(max_lon_result)[i] = max_lon;
        FlatVector::GetData<double>(max_lat_result)[i] = max_lat;
    }

    result.SetVectorType(VectorType::FLAT_VECTOR);
}

// quadbin_pixel_xy(lon, lat, resolution, tile_size) -> STRUCT(pixel_x INT, pixel_y INT)
static void QuadbinPixelXYFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &lon_vec = args.data[0];
    auto &lat_vec = args.data[1];
    auto &res_vec = args.data[2];
    auto &size_vec = args.data[3];

    auto &struct_entries = StructVector::GetEntries(result);
    auto &px_result = *struct_entries[0];
    auto &py_result = *struct_entries[1];

    for (idx_t i = 0; i < args.size(); i++) {
        auto lon = FlatVector::GetData<double>(lon_vec)[i];
        auto lat = FlatVector::GetData<double>(lat_vec)[i];
        auto resolution = FlatVector::GetData<int32_t>(res_vec)[i];
        auto tile_size = FlatVector::GetData<int32_t>(size_vec)[i];

        int pixel_x, pixel_y, tile_x, tile_y;
        quadbin::lonlat_to_pixel(lon, lat, resolution, tile_size, pixel_x, pixel_y, tile_x, tile_y);

        FlatVector::GetData<int32_t>(px_result)[i] = pixel_x;
        FlatVector::GetData<int32_t>(py_result)[i] = pixel_y;
    }

    result.SetVectorType(VectorType::FLAT_VECTOR);
}

// ============================================================================
// Spatial Filtering Functions
// ============================================================================

// quadbin_contains(cell, point_geometry) -> BOOLEAN
// Check if a point geometry falls within the tile represented by cell
// Requires DuckDB Spatial extension for GEOMETRY type
static void QuadbinContainsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    args.data[0].Flatten(args.size());
    args.data[1].Flatten(args.size());

    auto cell_data = FlatVector::GetData<uint64_t>(args.data[0]);
    auto geom_data = FlatVector::GetData<string_t>(args.data[1]);
    auto result_data = FlatVector::GetData<bool>(result);
    auto &result_mask = FlatVector::Validity(result);

    for (idx_t i = 0; i < args.size(); i++) {
        auto cell = cell_data[i];
        auto geom = geom_data[i];

        double lon, lat;
        if (!ExtractPointCoordinates(geom, lon, lat)) {
            throw InvalidInputException("quadbin_contains: geometry must be a POINT");
        }

        // Get the tile coordinates from the cell
        int tile_x, tile_y, z;
        quadbin::cell_to_tile(cell, tile_x, tile_y, z);

        // Get the tile coordinates for the point at the same resolution
        int point_tile_x, point_tile_y;
        quadbin::lonlat_to_tile(lon, lat, z, point_tile_x, point_tile_y);

        // Point is in tile if tile coordinates match
        result_data[i] = (tile_x == point_tile_x && tile_y == point_tile_y);
    }
}

// quadbin_intersects(cell, geometry) -> BOOLEAN
// Check if a tile intersects a geometry (uses bounding box of geometry)
// Requires DuckDB Spatial extension for GEOMETRY type
static void QuadbinIntersectsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    args.data[0].Flatten(args.size());
    args.data[1].Flatten(args.size());

    auto cell_data = FlatVector::GetData<uint64_t>(args.data[0]);
    auto geom_data = FlatVector::GetData<string_t>(args.data[1]);
    auto result_data = FlatVector::GetData<bool>(result);
    auto &result_mask = FlatVector::Validity(result);

    for (idx_t i = 0; i < args.size(); i++) {
        auto cell = cell_data[i];
        auto geom = geom_data[i];

        double query_min_lon, query_min_lat, query_max_lon, query_max_lat;
        if (!ExtractBoundingBox(geom, query_min_lon, query_min_lat, query_max_lon, query_max_lat)) {
            throw InvalidInputException("quadbin_intersects: could not extract bounding box from geometry");
        }

        // Get tile bbox
        int tile_x, tile_y, z;
        quadbin::cell_to_tile(cell, tile_x, tile_y, z);

        double tile_min_lon, tile_min_lat, tile_max_lon, tile_max_lat;
        quadbin::tile_to_bbox_wgs84(tile_x, tile_y, z, tile_min_lon, tile_min_lat, tile_max_lon, tile_max_lat);

        // Check for intersection (two boxes intersect if they overlap in both dimensions)
        bool intersects = !(tile_max_lon < query_min_lon ||  // tile is left of query
                           tile_min_lon > query_max_lon ||  // tile is right of query
                           tile_max_lat < query_min_lat ||  // tile is below query
                           tile_min_lat > query_max_lat);   // tile is above query

        result_data[i] = intersects;
    }
}

// ============================================================================
// PostGIS-like Spatial Predicate Functions
// ============================================================================

// ST_Intersects(block, geometry) -> BOOLEAN
// PostGIS-like API: Returns TRUE if the tile's bounding box intersects the geometry
// Users don't need to understand quadbins - they just think of 'block' as tile location
static void STIntersectsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    args.data[0].Flatten(args.size());
    args.data[1].Flatten(args.size());

    auto cell_data = FlatVector::GetData<uint64_t>(args.data[0]);
    auto geom_data = FlatVector::GetData<string_t>(args.data[1]);
    auto result_data = FlatVector::GetData<bool>(result);

    for (idx_t i = 0; i < args.size(); i++) {
        auto cell = cell_data[i];
        auto geom = geom_data[i];

        double query_min_lon, query_min_lat, query_max_lon, query_max_lat;
        if (!ExtractBoundingBox(geom, query_min_lon, query_min_lat, query_max_lon, query_max_lat)) {
            throw InvalidInputException("ST_Intersects: could not extract bounding box from geometry");
        }

        // Get tile bbox
        int tile_x, tile_y, z;
        quadbin::cell_to_tile(cell, tile_x, tile_y, z);

        double tile_min_lon, tile_min_lat, tile_max_lon, tile_max_lat;
        quadbin::tile_to_bbox_wgs84(tile_x, tile_y, z, tile_min_lon, tile_min_lat, tile_max_lon, tile_max_lat);

        // Check for bbox intersection (fast O(1) comparison)
        bool intersects = !(tile_max_lon < query_min_lon ||  // tile is left of query
                           tile_min_lon > query_max_lon ||  // tile is right of query
                           tile_max_lat < query_min_lat ||  // tile is below query
                           tile_min_lat > query_max_lat);   // tile is above query

        result_data[i] = intersects;
    }
}

// ST_Contains(geometry, block) -> BOOLEAN
// PostGIS-like API: Returns TRUE if the geometry fully contains the tile
// Note: argument order matches PostGIS convention (container, contained)
static void STContainsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    args.data[0].Flatten(args.size());
    args.data[1].Flatten(args.size());

    auto geom_data = FlatVector::GetData<string_t>(args.data[0]);
    auto cell_data = FlatVector::GetData<uint64_t>(args.data[1]);
    auto result_data = FlatVector::GetData<bool>(result);

    for (idx_t i = 0; i < args.size(); i++) {
        auto geom = geom_data[i];
        auto cell = cell_data[i];

        // Get tile bounds
        int tile_x, tile_y, z;
        quadbin::cell_to_tile(cell, tile_x, tile_y, z);

        double tile_min_lon, tile_min_lat, tile_max_lon, tile_max_lat;
        quadbin::tile_to_bbox_wgs84(tile_x, tile_y, z, tile_min_lon, tile_min_lat, tile_max_lon, tile_max_lat);

        // Check if ALL four tile corners are inside the geometry
        bool contains = PointInGeometry(tile_min_lon, tile_min_lat, geom) &&  // SW corner
                        PointInGeometry(tile_max_lon, tile_min_lat, geom) &&  // SE corner
                        PointInGeometry(tile_min_lon, tile_max_lat, geom) &&  // NW corner
                        PointInGeometry(tile_max_lon, tile_max_lat, geom);    // NE corner

        result_data[i] = contains;
    }
}

// ============================================================================
// Spatial Format Functions (for DuckDB Spatial compatibility)
// ============================================================================

// Helper function to convert quadbin cell to WKT polygon
static std::string cell_to_wkt(uint64_t cell) {
    int x, y, z;
    quadbin::cell_to_tile(cell, x, y, z);

    double min_lon, min_lat, max_lon, max_lat;
    quadbin::tile_to_bbox_wgs84(x, y, z, min_lon, min_lat, max_lon, max_lat);

    std::ostringstream ss;
    ss << std::setprecision(15);
    ss << "POLYGON(("
       << min_lon << " " << min_lat << ", "
       << max_lon << " " << min_lat << ", "
       << max_lon << " " << max_lat << ", "
       << min_lon << " " << max_lat << ", "
       << min_lon << " " << min_lat << "))";

    return ss.str();
}

// Helper function to convert quadbin cell to GeoJSON
static std::string cell_to_geojson(uint64_t cell) {
    int x, y, z;
    quadbin::cell_to_tile(cell, x, y, z);

    double min_lon, min_lat, max_lon, max_lat;
    quadbin::tile_to_bbox_wgs84(x, y, z, min_lon, min_lat, max_lon, max_lat);

    std::ostringstream ss;
    ss << std::setprecision(15);
    ss << R"({"type":"Polygon","coordinates":[[[)"
       << min_lon << "," << min_lat << "],["
       << max_lon << "," << min_lat << "],["
       << max_lon << "," << max_lat << "],["
       << min_lon << "," << max_lat << "],["
       << min_lon << "," << min_lat << "]]]}";

    return ss.str();
}

// quadbin_to_wkt(cell) -> VARCHAR
// Convert a quadbin cell to WKT POLYGON for use with ST_GeomFromText
static void QuadbinToWktFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &cell_vec = args.data[0];

    UnaryExecutor::Execute<uint64_t, string_t>(cell_vec, result, args.size(),
        [&](uint64_t cell) {
            auto wkt = cell_to_wkt(cell);
            return StringVector::AddString(result, wkt);
        });
}

// quadbin_to_geojson(cell) -> VARCHAR
// Convert a quadbin cell to GeoJSON for use with ST_GeomFromGeoJSON
static void QuadbinToGeojsonFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &cell_vec = args.data[0];

    UnaryExecutor::Execute<uint64_t, string_t>(cell_vec, result, args.size(),
        [&](uint64_t cell) {
            auto json = cell_to_geojson(cell);
            return StringVector::AddString(result, json);
        });
}

// quadbin_boundary(cell) -> VARCHAR (alias for quadbin_to_wkt)
// Returns WKT representation - named for PostGIS compatibility
static void QuadbinBoundaryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    QuadbinToWktFunction(args, state, result);
}

// ============================================================================
// Hierarchical Functions (parent, children, siblings, kring)
// ============================================================================

// quadbin_to_parent(cell) -> UBIGINT
// Get parent cell at resolution - 1
static void QuadbinToParentFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &cell_vec = args.data[0];

    UnaryExecutor::Execute<uint64_t, uint64_t>(cell_vec, result, args.size(),
        [](uint64_t cell) {
            return quadbin::cell_to_parent(cell);
        });
}

// quadbin_to_parent(cell, resolution) -> UBIGINT
// Get parent cell at specified resolution
static void QuadbinToParentResFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &cell_vec = args.data[0];
    auto &res_vec = args.data[1];

    BinaryExecutor::Execute<uint64_t, int32_t, uint64_t>(cell_vec, res_vec, result, args.size(),
        [](uint64_t cell, int32_t resolution) {
            return quadbin::cell_to_parent(cell, resolution);
        });
}

// quadbin_to_children(cell) -> LIST(UBIGINT)
// Get 4 children cells at resolution + 1
static void QuadbinToChildrenFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &cell_vec = args.data[0];
    auto list_size = args.size();

    // Result is a list of UBIGINT
    auto &list_entries = ListVector::GetEntry(result);
    auto list_data = FlatVector::GetData<list_entry_t>(result);

    idx_t total_children = 0;
    for (idx_t i = 0; i < list_size; i++) {
        auto cell = FlatVector::GetData<uint64_t>(cell_vec)[i];

        uint64_t children[4];
        quadbin::cell_to_children(cell, children);

        list_data[i].offset = total_children;
        list_data[i].length = 4;

        ListVector::Reserve(result, total_children + 4);
        auto child_data = FlatVector::GetData<uint64_t>(list_entries);
        for (int j = 0; j < 4; j++) {
            child_data[total_children + j] = children[j];
        }
        total_children += 4;
    }
    ListVector::SetListSize(result, total_children);
}

// quadbin_to_children(cell, resolution) -> LIST(UBIGINT)
// Get all children cells at specified resolution
static void QuadbinToChildrenResFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &cell_vec = args.data[0];
    auto &res_vec = args.data[1];
    auto list_size = args.size();

    auto &list_entries = ListVector::GetEntry(result);
    auto list_data = FlatVector::GetData<list_entry_t>(result);

    idx_t total_children = 0;
    for (idx_t i = 0; i < list_size; i++) {
        auto cell = FlatVector::GetData<uint64_t>(cell_vec)[i];
        auto target_res = FlatVector::GetData<int32_t>(res_vec)[i];

        int current_res = quadbin::cell_to_resolution(cell);
        if (target_res <= current_res || target_res > 26) {
            list_data[i].offset = total_children;
            list_data[i].length = 0;
            continue;
        }

        int res_diff = target_res - current_res;
        int num_children = 1 << (2 * res_diff);  // 4^res_diff

        // Allocate space for children
        std::vector<uint64_t> children(num_children);
        int count;
        quadbin::cell_to_children(cell, target_res, children.data(), count);

        list_data[i].offset = total_children;
        list_data[i].length = count;

        ListVector::Reserve(result, total_children + count);
        auto child_data = FlatVector::GetData<uint64_t>(list_entries);
        for (int j = 0; j < count; j++) {
            child_data[total_children + j] = children[j];
        }
        total_children += count;
    }
    ListVector::SetListSize(result, total_children);
}

// quadbin_sibling(cell) -> LIST(UBIGINT)
// Get sibling cells (other children of the same parent)
static void QuadbinSiblingFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &cell_vec = args.data[0];
    auto list_size = args.size();

    auto &list_entries = ListVector::GetEntry(result);
    auto list_data = FlatVector::GetData<list_entry_t>(result);

    idx_t total_siblings = 0;
    for (idx_t i = 0; i < list_size; i++) {
        auto cell = FlatVector::GetData<uint64_t>(cell_vec)[i];

        uint64_t siblings[4];
        quadbin::cell_siblings(cell, siblings);

        list_data[i].offset = total_siblings;
        list_data[i].length = 4;

        ListVector::Reserve(result, total_siblings + 4);
        auto sibling_data = FlatVector::GetData<uint64_t>(list_entries);
        for (int j = 0; j < 4; j++) {
            sibling_data[total_siblings + j] = siblings[j];
        }
        total_siblings += 4;
    }
    ListVector::SetListSize(result, total_siblings);
}

// ============================================================================
// ST_GeomFromQuadbin - Convert quadbin to GEOMETRY
// ============================================================================

// Helper to create WKB polygon from tile bounds
static std::vector<uint8_t> CreateWKBPolygon(double min_lon, double min_lat, double max_lon, double max_lat) {
    std::vector<uint8_t> wkb;
    wkb.reserve(93);  // 1 + 4 + 4 + 4 + 5*16 = 93 bytes for a simple polygon

    // Byte order: little endian
    wkb.push_back(0x01);

    // Geometry type: Polygon (3)
    uint32_t geom_type = 3;
    wkb.insert(wkb.end(), reinterpret_cast<uint8_t*>(&geom_type), reinterpret_cast<uint8_t*>(&geom_type) + 4);

    // Number of rings: 1
    uint32_t num_rings = 1;
    wkb.insert(wkb.end(), reinterpret_cast<uint8_t*>(&num_rings), reinterpret_cast<uint8_t*>(&num_rings) + 4);

    // Number of points in ring: 5 (closed polygon)
    uint32_t num_points = 5;
    wkb.insert(wkb.end(), reinterpret_cast<uint8_t*>(&num_points), reinterpret_cast<uint8_t*>(&num_points) + 4);

    // Points: SW, SE, NE, NW, SW (closed)
    auto add_point = [&wkb](double x, double y) {
        wkb.insert(wkb.end(), reinterpret_cast<uint8_t*>(&x), reinterpret_cast<uint8_t*>(&x) + 8);
        wkb.insert(wkb.end(), reinterpret_cast<uint8_t*>(&y), reinterpret_cast<uint8_t*>(&y) + 8);
    };

    add_point(min_lon, min_lat);  // SW
    add_point(max_lon, min_lat);  // SE
    add_point(max_lon, max_lat);  // NE
    add_point(min_lon, max_lat);  // NW
    add_point(min_lon, min_lat);  // SW (close)

    return wkb;
}

// ST_GeomFromQuadbin(cell) -> GEOMETRY
// Returns the tile boundary as a GEOMETRY polygon
static void STGeomFromQuadbinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &cell_vec = args.data[0];

    UnaryExecutor::Execute<uint64_t, string_t>(cell_vec, result, args.size(),
        [&](uint64_t cell) {
            int x, y, z;
            quadbin::cell_to_tile(cell, x, y, z);

            double min_lon, min_lat, max_lon, max_lat;
            quadbin::tile_to_bbox_wgs84(x, y, z, min_lon, min_lat, max_lon, max_lat);

            auto wkb = CreateWKBPolygon(min_lon, min_lat, max_lon, max_lat);
            return StringVector::AddStringOrBlob(result, reinterpret_cast<const char*>(wkb.data()), wkb.size());
        });
}

// quadbin_kring(cell, k) -> LIST(UBIGINT)
// Get cells within k distance from center
static void QuadbinKringFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &cell_vec = args.data[0];
    auto &k_vec = args.data[1];
    auto list_size = args.size();

    auto &list_entries = ListVector::GetEntry(result);
    auto list_data = FlatVector::GetData<list_entry_t>(result);

    idx_t total_neighbors = 0;
    for (idx_t i = 0; i < list_size; i++) {
        auto cell = FlatVector::GetData<uint64_t>(cell_vec)[i];
        auto k = FlatVector::GetData<int32_t>(k_vec)[i];

        if (k < 0) {
            list_data[i].offset = total_neighbors;
            list_data[i].length = 0;
            continue;
        }

        // Maximum possible neighbors: (2k+1)^2
        int max_neighbors = (2 * k + 1) * (2 * k + 1);
        std::vector<uint64_t> neighbors(max_neighbors);
        int count;
        quadbin::cell_kring(cell, k, neighbors.data(), count);

        list_data[i].offset = total_neighbors;
        list_data[i].length = count;

        ListVector::Reserve(result, total_neighbors + count);
        auto neighbor_data = FlatVector::GetData<uint64_t>(list_entries);
        for (int j = 0; j < count; j++) {
            neighbor_data[total_neighbors + j] = neighbors[j];
        }
        total_neighbors += count;
    }
    ListVector::SetListSize(result, total_neighbors);
}

void RegisterQuadbinFunctions(ExtensionLoader &loader) {
    // quadbin_from_tile(x, y, z) -> UBIGINT
    ScalarFunction from_tile("quadbin_from_tile",
        {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER},
        LogicalType::UBIGINT,
        QuadbinFromTileFunction);
    loader.RegisterFunction(from_tile);

    // quadbin_to_tile(cell) -> STRUCT
    child_list_t<LogicalType> tile_struct;
    tile_struct.push_back(make_pair("x", LogicalType::INTEGER));
    tile_struct.push_back(make_pair("y", LogicalType::INTEGER));
    tile_struct.push_back(make_pair("z", LogicalType::INTEGER));

    ScalarFunction to_tile("quadbin_to_tile",
        {LogicalType::UBIGINT},
        LogicalType::STRUCT(tile_struct),
        QuadbinToTileFunction);
    loader.RegisterFunction(to_tile);

    // quadbin_from_lonlat(lon, lat, resolution) -> UBIGINT
    ScalarFunction from_lonlat("quadbin_from_lonlat",
        {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::INTEGER},
        LogicalType::UBIGINT,
        QuadbinFromLonLatFunction);
    loader.RegisterFunction(from_lonlat);

    // quadbin_to_lonlat(cell) -> STRUCT
    child_list_t<LogicalType> lonlat_struct;
    lonlat_struct.push_back(make_pair("lon", LogicalType::DOUBLE));
    lonlat_struct.push_back(make_pair("lat", LogicalType::DOUBLE));

    ScalarFunction to_lonlat("quadbin_to_lonlat",
        {LogicalType::UBIGINT},
        LogicalType::STRUCT(lonlat_struct),
        QuadbinToLonLatFunction);
    loader.RegisterFunction(to_lonlat);

    // quadbin_resolution(cell) -> INT
    ScalarFunction resolution("quadbin_resolution",
        {LogicalType::UBIGINT},
        LogicalType::INTEGER,
        QuadbinResolutionFunction);
    loader.RegisterFunction(resolution);

    // quadbin_to_bbox(cell) -> STRUCT(min_lon, min_lat, max_lon, max_lat)
    child_list_t<LogicalType> bbox_struct;
    bbox_struct.push_back(make_pair("min_lon", LogicalType::DOUBLE));
    bbox_struct.push_back(make_pair("min_lat", LogicalType::DOUBLE));
    bbox_struct.push_back(make_pair("max_lon", LogicalType::DOUBLE));
    bbox_struct.push_back(make_pair("max_lat", LogicalType::DOUBLE));

    ScalarFunction to_bbox("quadbin_to_bbox",
        {LogicalType::UBIGINT},
        LogicalType::STRUCT(bbox_struct),
        QuadbinToBboxFunction);
    loader.RegisterFunction(to_bbox);

    // quadbin_pixel_xy(lon, lat, resolution, tile_size) -> STRUCT(pixel_x, pixel_y)
    child_list_t<LogicalType> pixel_struct;
    pixel_struct.push_back(make_pair("pixel_x", LogicalType::INTEGER));
    pixel_struct.push_back(make_pair("pixel_y", LogicalType::INTEGER));

    ScalarFunction pixel_xy("quadbin_pixel_xy",
        {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::INTEGER, LogicalType::INTEGER},
        LogicalType::STRUCT(pixel_struct),
        QuadbinPixelXYFunction);
    loader.RegisterFunction(pixel_xy);

    // ========================================================================
    // Spatial Filtering Functions (require DuckDB Spatial extension)
    // ========================================================================

    // quadbin_contains(cell, point_geometry) -> BOOLEAN
    ScalarFunction contains("quadbin_contains",
        {LogicalType::UBIGINT, LogicalType::GEOMETRY()},
        LogicalType::BOOLEAN,
        QuadbinContainsFunction);
    loader.RegisterFunction(contains);

    // quadbin_intersects(cell, geometry) -> BOOLEAN
    ScalarFunction intersects("quadbin_intersects",
        {LogicalType::UBIGINT, LogicalType::GEOMETRY()},
        LogicalType::BOOLEAN,
        QuadbinIntersectsFunction);
    loader.RegisterFunction(intersects);

    // ========================================================================
    // PostGIS-like Spatial Predicate Functions
    // ========================================================================

    // ST_Intersects(block, geometry) -> BOOLEAN
    // PostGIS-like API for raster-vector intersection
    ScalarFunction st_intersects("ST_Intersects",
        {LogicalType::UBIGINT, LogicalType::GEOMETRY()},
        LogicalType::BOOLEAN,
        STIntersectsFunction);
    loader.RegisterFunction(st_intersects);

    // ST_Contains(geometry, block) -> BOOLEAN
    // PostGIS-like API: geometry contains tile
    ScalarFunction st_contains("ST_Contains",
        {LogicalType::GEOMETRY(), LogicalType::UBIGINT},
        LogicalType::BOOLEAN,
        STContainsFunction);
    loader.RegisterFunction(st_contains);

    // ========================================================================
    // Spatial Format Functions (DuckDB Spatial compatibility)
    // ========================================================================

    // quadbin_to_wkt(cell) -> VARCHAR
    // Returns WKT POLYGON for use with ST_GeomFromText
    ScalarFunction to_wkt("quadbin_to_wkt",
        {LogicalType::UBIGINT},
        LogicalType::VARCHAR,
        QuadbinToWktFunction);
    loader.RegisterFunction(to_wkt);

    // quadbin_to_geojson(cell) -> VARCHAR
    // Returns GeoJSON for use with ST_GeomFromGeoJSON
    ScalarFunction to_geojson("quadbin_to_geojson",
        {LogicalType::UBIGINT},
        LogicalType::VARCHAR,
        QuadbinToGeojsonFunction);
    loader.RegisterFunction(to_geojson);

    // quadbin_boundary(cell) -> VARCHAR
    // Alias for quadbin_to_wkt - named for PostGIS compatibility
    ScalarFunction boundary("quadbin_boundary",
        {LogicalType::UBIGINT},
        LogicalType::VARCHAR,
        QuadbinBoundaryFunction);
    loader.RegisterFunction(boundary);

    // ========================================================================
    // Hierarchical Functions (parent, children, siblings, kring)
    // ========================================================================

    // quadbin_to_parent(cell) -> UBIGINT
    // Get parent cell at resolution - 1
    ScalarFunction to_parent("quadbin_to_parent",
        {LogicalType::UBIGINT},
        LogicalType::UBIGINT,
        QuadbinToParentFunction);
    loader.RegisterFunction(to_parent);

    // quadbin_to_parent(cell, resolution) -> UBIGINT
    // Get parent cell at specified resolution
    ScalarFunctionSet to_parent_set("quadbin_to_parent");
    to_parent_set.AddFunction(ScalarFunction(
        {LogicalType::UBIGINT},
        LogicalType::UBIGINT,
        QuadbinToParentFunction));
    to_parent_set.AddFunction(ScalarFunction(
        {LogicalType::UBIGINT, LogicalType::INTEGER},
        LogicalType::UBIGINT,
        QuadbinToParentResFunction));
    loader.RegisterFunction(to_parent_set);

    // quadbin_to_children(cell) -> LIST(UBIGINT)
    // Get 4 children cells at resolution + 1
    ScalarFunctionSet to_children_set("quadbin_to_children");
    to_children_set.AddFunction(ScalarFunction(
        {LogicalType::UBIGINT},
        LogicalType::LIST(LogicalType::UBIGINT),
        QuadbinToChildrenFunction));
    to_children_set.AddFunction(ScalarFunction(
        {LogicalType::UBIGINT, LogicalType::INTEGER},
        LogicalType::LIST(LogicalType::UBIGINT),
        QuadbinToChildrenResFunction));
    loader.RegisterFunction(to_children_set);

    // quadbin_sibling(cell) -> LIST(UBIGINT)
    // Get sibling cells (other children of the same parent)
    ScalarFunction sibling("quadbin_sibling",
        {LogicalType::UBIGINT},
        LogicalType::LIST(LogicalType::UBIGINT),
        QuadbinSiblingFunction);
    loader.RegisterFunction(sibling);

    // quadbin_kring(cell, k) -> LIST(UBIGINT)
    // Get cells within k distance from center
    ScalarFunction kring("quadbin_kring",
        {LogicalType::UBIGINT, LogicalType::INTEGER},
        LogicalType::LIST(LogicalType::UBIGINT),
        QuadbinKringFunction);
    loader.RegisterFunction(kring);

    // ========================================================================
    // Geometry Conversion Functions
    // ========================================================================

    // ST_GeomFromQuadbin(cell) -> GEOMETRY
    // Returns the tile boundary as a GEOMETRY polygon
    ScalarFunction geom_from_quadbin("ST_GeomFromQuadbin",
        {LogicalType::UBIGINT},
        LogicalType::GEOMETRY(),
        STGeomFromQuadbinFunction);
    loader.RegisterFunction(geom_from_quadbin);
}

} // namespace duckdb
