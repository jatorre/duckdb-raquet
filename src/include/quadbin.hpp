#pragma once

#include <cstdint>
#include <cmath>
#include <stdexcept>

namespace duckdb {
namespace quadbin {

// Constants
constexpr int MAX_RESOLUTION = 26;
constexpr double EARTH_RADIUS = 6378137.0;
constexpr double MAX_LATITUDE = 85.051128779806604;
constexpr double PI = 3.14159265358979323846;

// QUADBIN encoding constants (from quadbin Python library)
constexpr uint64_t HEADER = 0x4000000000000000ULL;
constexpr uint64_t MODE = 0x0800000000000000ULL;  // Mode 1 for cells
constexpr uint64_t FOOTER = 0x000FFFFFFFFFFFFFULL;

// Magic numbers for Morton code interleaving
constexpr uint64_t B0 = 0x5555555555555555ULL;
constexpr uint64_t B1 = 0x3333333333333333ULL;
constexpr uint64_t B2 = 0x0F0F0F0F0F0F0F0FULL;
constexpr uint64_t B3 = 0x00FF00FF00FF00FFULL;
constexpr uint64_t B4 = 0x0000FFFF0000FFFFULL;
constexpr uint64_t B5 = 0x00000000FFFFFFFFULL;

// Extract resolution from quadbin cell
inline int cell_to_resolution(uint64_t cell) {
    return static_cast<int>((cell >> 52) & 0x1F);
}

// Convert tile coordinates to quadbin cell
inline uint64_t tile_to_cell(int x, int y, int z) {
    if (z < 0 || z > MAX_RESOLUTION) {
        throw std::invalid_argument("Resolution must be between 0 and 26");
    }

    // Shift x and y to 32-bit range based on resolution
    uint64_t ux = static_cast<uint64_t>(x) << (32 - z);
    uint64_t uy = static_cast<uint64_t>(y) << (32 - z);

    // Morton code interleaving (spread bits)
    ux = (ux | (ux << 16)) & B4;
    ux = (ux | (ux << 8)) & B3;
    ux = (ux | (ux << 4)) & B2;
    ux = (ux | (ux << 2)) & B1;
    ux = (ux | (ux << 1)) & B0;

    uy = (uy | (uy << 16)) & B4;
    uy = (uy | (uy << 8)) & B3;
    uy = (uy | (uy << 4)) & B2;
    uy = (uy | (uy << 2)) & B1;
    uy = (uy | (uy << 1)) & B0;

    // Combine: HEADER | MODE | resolution | interleaved_xy | trailing_ones
    return HEADER | MODE | (static_cast<uint64_t>(z) << 52) |
           ((ux | (uy << 1)) >> 12) | (FOOTER >> (z * 2));
}

// Convert quadbin cell to tile coordinates
inline void cell_to_tile(uint64_t cell, int &x, int &y, int &z) {
    z = cell_to_resolution(cell);

    // Extract the quadkey portion and shift to working position
    uint64_t q = (cell & FOOTER) << 12;

    // De-interleave x (even bits) and y (odd bits)
    uint64_t ux = q;
    uint64_t uy = q >> 1;

    // Compact the bits back (reverse Morton code)
    ux = ux & B0;
    uy = uy & B0;

    ux = (ux | (ux >> 1)) & B1;
    uy = (uy | (uy >> 1)) & B1;

    ux = (ux | (ux >> 2)) & B2;
    uy = (uy | (uy >> 2)) & B2;

    ux = (ux | (ux >> 4)) & B3;
    uy = (uy | (uy >> 4)) & B3;

    ux = (ux | (ux >> 8)) & B4;
    uy = (uy | (uy >> 8)) & B4;

    ux = (ux | (ux >> 16)) & B5;
    uy = (uy | (uy >> 16)) & B5;

    // Shift back based on resolution
    x = static_cast<int>(ux >> (32 - z));
    y = static_cast<int>(uy >> (32 - z));
}

// Convert longitude/latitude to tile coordinates at given resolution
inline void lonlat_to_tile(double lon, double lat, int z, int &x, int &y) {
    // Clamp latitude to valid range
    if (lat > MAX_LATITUDE) lat = MAX_LATITUDE;
    if (lat < -MAX_LATITUDE) lat = -MAX_LATITUDE;

    // Convert to tile coordinates
    double n = std::pow(2.0, z);
    x = static_cast<int>(std::floor((lon + 180.0) / 360.0 * n));

    double lat_rad = lat * PI / 180.0;
    y = static_cast<int>(std::floor((1.0 - std::asinh(std::tan(lat_rad)) / PI) / 2.0 * n));

    // Clamp to valid range
    if (x < 0) x = 0;
    if (x >= static_cast<int>(n)) x = static_cast<int>(n) - 1;
    if (y < 0) y = 0;
    if (y >= static_cast<int>(n)) y = static_cast<int>(n) - 1;
}

// Convert longitude/latitude to quadbin cell at given resolution
inline uint64_t lonlat_to_cell(double lon, double lat, int z) {
    int x, y;
    lonlat_to_tile(lon, lat, z, x, y);
    return tile_to_cell(x, y, z);
}

// Convert tile coordinates to longitude/latitude (returns center of tile)
inline void tile_to_lonlat(int x, int y, int z, double &lon, double &lat) {
    double n = std::pow(2.0, z);
    lon = x / n * 360.0 - 180.0;
    double lat_rad = std::atan(std::sinh(PI * (1.0 - 2.0 * y / n)));
    lat = lat_rad * 180.0 / PI;
}

// Convert quadbin cell to longitude/latitude (center of cell)
inline void cell_to_lonlat(uint64_t cell, double &lon, double &lat) {
    int x, y, z;
    cell_to_tile(cell, x, y, z);
    // Return center of tile
    double n = std::pow(2.0, z);
    lon = (x + 0.5) / n * 360.0 - 180.0;
    double lat_rad = std::atan(std::sinh(PI * (1.0 - 2.0 * (y + 0.5) / n)));
    lat = lat_rad * 180.0 / PI;
}

// Get tile bounds in Web Mercator (EPSG:3857)
inline void tile_to_bbox_mercator(int x, int y, int z,
                                   double &min_x, double &min_y,
                                   double &max_x, double &max_y) {
    double n = std::pow(2.0, z);
    double tile_size = 2.0 * PI * EARTH_RADIUS / n;

    min_x = x * tile_size - PI * EARTH_RADIUS;
    max_x = (x + 1) * tile_size - PI * EARTH_RADIUS;

    // Y is flipped in Web Mercator tiles
    max_y = PI * EARTH_RADIUS - y * tile_size;
    min_y = PI * EARTH_RADIUS - (y + 1) * tile_size;
}

// Get tile bounds in WGS84 (EPSG:4326)
inline void tile_to_bbox_wgs84(int x, int y, int z,
                                double &min_lon, double &min_lat,
                                double &max_lon, double &max_lat) {
    double n = std::pow(2.0, z);

    min_lon = x / n * 360.0 - 180.0;
    max_lon = (x + 1) / n * 360.0 - 180.0;

    double min_lat_rad = std::atan(std::sinh(PI * (1.0 - 2.0 * (y + 1) / n)));
    double max_lat_rad = std::atan(std::sinh(PI * (1.0 - 2.0 * y / n)));

    min_lat = min_lat_rad * 180.0 / PI;
    max_lat = max_lat_rad * 180.0 / PI;
}

// Get parent cell at lower resolution
inline uint64_t cell_to_parent(uint64_t cell, int parent_resolution) {
    int current_res = cell_to_resolution(cell);
    if (parent_resolution < 0 || parent_resolution > current_res) {
        throw std::invalid_argument("Parent resolution must be between 0 and current resolution");
    }
    if (parent_resolution == current_res) {
        return cell;
    }

    // Get tile coordinates and convert to parent resolution
    int x, y, z;
    cell_to_tile(cell, x, y, z);

    // Shift to parent resolution
    int shift = z - parent_resolution;
    int parent_x = x >> shift;
    int parent_y = y >> shift;

    return tile_to_cell(parent_x, parent_y, parent_resolution);
}

// Get parent cell at resolution - 1
inline uint64_t cell_to_parent(uint64_t cell) {
    int current_res = cell_to_resolution(cell);
    if (current_res == 0) {
        return cell; // Already at root
    }
    return cell_to_parent(cell, current_res - 1);
}

// Get children cells at higher resolution (returns 4 children)
inline void cell_to_children(uint64_t cell, int child_resolution,
                             uint64_t* children, int& count) {
    int current_res = cell_to_resolution(cell);
    if (child_resolution <= current_res || child_resolution > MAX_RESOLUTION) {
        throw std::invalid_argument("Child resolution must be greater than current and <= 26");
    }

    int x, y, z;
    cell_to_tile(cell, x, y, z);

    // Calculate number of children in each dimension
    int res_diff = child_resolution - current_res;
    int children_per_dim = 1 << res_diff;  // 2^res_diff
    count = children_per_dim * children_per_dim;

    // Base coordinates at child resolution
    int base_x = x << res_diff;
    int base_y = y << res_diff;

    // Generate all children
    int idx = 0;
    for (int dy = 0; dy < children_per_dim; dy++) {
        for (int dx = 0; dx < children_per_dim; dx++) {
            children[idx++] = tile_to_cell(base_x + dx, base_y + dy, child_resolution);
        }
    }
}

// Get immediate children (4 cells at resolution + 1)
inline void cell_to_children(uint64_t cell, uint64_t children[4]) {
    int dummy_count;
    cell_to_children(cell, cell_to_resolution(cell) + 1, children, dummy_count);
}

// Get k-ring neighbors (cells within distance k)
// Returns cells in a grid pattern around the center cell
inline void cell_kring(uint64_t cell, int k, uint64_t* neighbors, int& count) {
    if (k < 0) {
        throw std::invalid_argument("K must be non-negative");
    }

    int x, y, z;
    cell_to_tile(cell, x, y, z);

    int max_coord = (1 << z) - 1;  // Maximum valid coordinate at this resolution
    int diameter = 2 * k + 1;

    count = 0;
    for (int dy = -k; dy <= k; dy++) {
        for (int dx = -k; dx <= k; dx++) {
            int nx = x + dx;
            int ny = y + dy;

            // Skip cells outside valid range (handles edge wrapping)
            if (nx < 0 || nx > max_coord || ny < 0 || ny > max_coord) {
                continue;
            }

            neighbors[count++] = tile_to_cell(nx, ny, z);
        }
    }
}

// Get sibling cells (other children of the same parent)
inline void cell_siblings(uint64_t cell, uint64_t siblings[4]) {
    int z = cell_to_resolution(cell);
    if (z == 0) {
        // Root cell has no siblings
        siblings[0] = cell;
        siblings[1] = cell;
        siblings[2] = cell;
        siblings[3] = cell;
        return;
    }

    // Get parent and then its children
    uint64_t parent = cell_to_parent(cell);
    cell_to_children(parent, siblings);
}

// ─────────────────────────────────────────────
// Tile Matrix Set abstraction
//
// A QUADBIN cell is a pure quadkey (z/x/y packed into 64 bits) — the cell
// algebra above (tile_to_cell / cell_to_tile / parent / children / kring) is
// projection-agnostic and shared by every TMS. What differs per TMS is only
// the mapping between cells and geographic coordinates.
//
// The free functions above ARE the WebMercatorQuad mapping; WebMercatorQuad
// here delegates to them, so its output is byte-identical to the legacy code
// path (this is the P1 regression guarantee). GoogleCRS84Quad adds a
// single-root, square-grid, plate-carrée (CRS84) mapping over
// [-180,180]×[-90,90] — anisotropic pixels, but QUADBIN bit-compatible.
//
// Only the producer-side conversions needed by read_raster() are modelled
// here (lonlat→tile, tile→bbox, world span / max latitude / target SRS).
// Read-side helpers (lonlat_to_pixel, cell_to_lonlat, …) remain
// WebMercator-only for now; teaching them about the TMS is consumer-side work.
// ─────────────────────────────────────────────
enum class TmsId { WebMercatorQuad, GoogleCRS84Quad };

struct TileMatrixSet {
    TmsId id = TmsId::WebMercatorQuad;

    // Map a metadata tile_matrix_set string to a TMS. Unknown / empty values
    // fall back to WebMercatorQuad (back-compat with pre-0.6.0 files).
    static TileMatrixSet FromName(const std::string &name) {
        if (name == "GoogleCRS84Quad") return TileMatrixSet{TmsId::GoogleCRS84Quad};
        return TileMatrixSet{TmsId::WebMercatorQuad};
    }

    const char *name() const {
        return id == TmsId::GoogleCRS84Quad ? "GoogleCRS84Quad" : "WebMercatorQuad";
    }
    const char *uri() const {
        return id == TmsId::GoogleCRS84Quad
            ? "http://www.opengis.net/def/tilematrixset/OGC/1.0/GoogleCRS84Quad"
            : "http://www.opengis.net/def/tilematrixset/OGC/1.0/WebMercatorQuad";
    }
    // CRS the tile grid (cell coordinates / geotransform) is expressed in.
    const char *crs() const {
        return id == TmsId::GoogleCRS84Quad ? "OGC:CRS84" : "EPSG:3857";
    }
    // EPSG code used to build the GDAL warp-target SRS.
    int target_epsg() const {
        return id == TmsId::GoogleCRS84Quad ? 4326 : 3857;
    }
    double max_latitude() const {
        return id == TmsId::GoogleCRS84Quad ? 90.0 : MAX_LATITUDE;
    }
    // Span of the world along X in the grid CRS units — metres (full equatorial
    // circumference) for WebMercator, degrees (360) for CRS84. Drives zoom calc.
    double world_span_x() const {
        return id == TmsId::GoogleCRS84Quad ? 360.0 : (2.0 * PI * EARTH_RADIUS);
    }

    void lonlat_to_tile(double lon, double lat, int z, int &x, int &y) const {
        if (id == TmsId::WebMercatorQuad) {
            quadbin::lonlat_to_tile(lon, lat, z, x, y);
            return;
        }
        // GoogleCRS84Quad: plate carrée, single root, square 2^z × 2^z grid.
        double ml = max_latitude();
        if (lat > ml) lat = ml;
        if (lat < -ml) lat = -ml;
        double n = std::pow(2.0, z);
        x = static_cast<int>(std::floor((lon + 180.0) / 360.0 * n));
        y = static_cast<int>(std::floor((90.0 - lat) / 180.0 * n));
        if (x < 0) x = 0;
        if (x >= static_cast<int>(n)) x = static_cast<int>(n) - 1;
        if (y < 0) y = 0;
        if (y >= static_cast<int>(n)) y = static_cast<int>(n) - 1;
    }

    // Tile bounds in the grid CRS (metres for WebMercator, degrees for CRS84).
    // Used to set the warp tile geotransform.
    void tile_to_bbox_projected(int x, int y, int z,
                                double &min_x, double &min_y,
                                double &max_x, double &max_y) const {
        if (id == TmsId::WebMercatorQuad) {
            quadbin::tile_to_bbox_mercator(x, y, z, min_x, min_y, max_x, max_y);
            return;
        }
        double n = std::pow(2.0, z);
        min_x = x / n * 360.0 - 180.0;
        max_x = (x + 1) / n * 360.0 - 180.0;
        // Y increases downward in tile space; row 0 is the north edge (+90).
        max_y = 90.0 - y / n * 180.0;
        min_y = 90.0 - (y + 1) / n * 180.0;
    }

    // Tile bounds in WGS84 degrees (metadata bounds, intersection tests).
    void tile_to_bbox_wgs84(int x, int y, int z,
                            double &min_lon, double &min_lat,
                            double &max_lon, double &max_lat) const {
        if (id == TmsId::WebMercatorQuad) {
            quadbin::tile_to_bbox_wgs84(x, y, z, min_lon, min_lat, max_lon, max_lat);
            return;
        }
        // CRS84: the projected bbox is already in degrees.
        tile_to_bbox_projected(x, y, z, min_lon, min_lat, max_lon, max_lat);
    }
};

// Calculate pixel coordinates within a tile for a given lon/lat
inline void lonlat_to_pixel(double lon, double lat, int z, int tile_size,
                            int &pixel_x, int &pixel_y, int &tile_x, int &tile_y) {
    // Clamp latitude
    if (lat > MAX_LATITUDE) lat = MAX_LATITUDE;
    if (lat < -MAX_LATITUDE) lat = -MAX_LATITUDE;

    double n = std::pow(2.0, z);

    // Get fractional tile position
    double tile_x_frac = (lon + 180.0) / 360.0 * n;
    double lat_rad = lat * PI / 180.0;
    double tile_y_frac = (1.0 - std::asinh(std::tan(lat_rad)) / PI) / 2.0 * n;

    // Integer tile coordinates
    tile_x = static_cast<int>(std::floor(tile_x_frac));
    tile_y = static_cast<int>(std::floor(tile_y_frac));

    // Pixel within tile
    pixel_x = static_cast<int>((tile_x_frac - tile_x) * tile_size);
    pixel_y = static_cast<int>((tile_y_frac - tile_y) * tile_size);

    // Clamp to valid range
    if (pixel_x >= tile_size) pixel_x = tile_size - 1;
    if (pixel_y >= tile_size) pixel_y = tile_size - 1;
    if (pixel_x < 0) pixel_x = 0;
    if (pixel_y < 0) pixel_y = 0;
}

} // namespace quadbin
} // namespace duckdb
