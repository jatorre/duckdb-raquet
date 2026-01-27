# DuckDB Raquet Extension

A DuckDB extension for working with [Raquet](https://github.com/CartoDB/raquet) raster data stored in Apache Parquet format with QUADBIN spatial indexing.

## What is Raquet?

**Raquet** is a specification created by [CARTO](https://carto.com) for storing raster data efficiently using:

- **Apache Parquet** - Columnar storage for efficient compression and query performance
- **QUADBIN** - A spatial indexing scheme encoding Web Mercator tile coordinates into 64-bit integers
- **Binary Band Data** - Pixel values stored as compressed BLOBs (gzip or uncompressed)

This extension enables DuckDB to query Raquet files directly using SQL, with functions for spatial indexing, pixel extraction, and raster analytics.

## Features

- **QUADBIN Functions** - Convert between tiles, cells, and geographic coordinates
- **Raster Value Extraction** - PostGIS-style `ST_RasterValue` for pixel lookups
- **Summary Statistics** - Calculate min/max/mean/stddev for raster tiles
- **Region Statistics** - Aggregate raster statistics within arbitrary polygon regions
- **Polygon Fill** - Fill polygons with QUADBIN cells at any resolution
- **Band Decoding** - Decompress and read binary raster data
- **Spatial Filtering** - Point-in-tile and bbox intersection tests
- **PostGIS-like Spatial Predicates** - `ST_Intersects`, `ST_Contains` for raster-vector operations
- **Time-Series Support** - CF conventions for temporal rasters (NetCDF compatible)
- **Cloud-Native** - Query remote Parquet files on S3/GCS with predicate pushdown
- **DuckDB Spatial Integration** - Works with GEOMETRY types from DuckDB Spatial

## Installation

### From Community Extensions (Coming Soon)

```sql
INSTALL raquet FROM community;
LOAD raquet;
```

### Building from Source

**Prerequisites:**
- CMake 3.12+
- C++17 compatible compiler
- zlib (for gzip decompression)

```bash
# Clone the repository
git clone https://github.com/CartoDB/duckdb-raquet.git
cd duckdb-raquet

# Initialize DuckDB submodule
git submodule update --init --recursive

# Build release version
make release

# Run tests
make test
```

**Build Targets:**
- `make release` - Build optimized release version
- `make debug` - Build with debug symbols
- `make test` - Run basic functionality test
- `make test_sql` - Run SQL test suite
- `make clean` - Clean build artifacts
- `make format` - Run clang-format on source files

## Quick Start

```sql
-- Load the extension
LOAD raquet;

-- Read a Raquet file (it's just Parquet!)
SELECT * FROM read_parquet('raster.parquet') WHERE block != 0 LIMIT 10;

-- Get pixel value at a location
SELECT ST_RasterValue(
    block,            -- QUADBIN cell ID
    band_1,           -- Binary band data
    -73.9857,         -- Longitude
    40.7484,          -- Latitude
    'int16',          -- Data type
    256,              -- Tile size
    'gzip'            -- Compression
) AS elevation
FROM read_parquet('dem.parquet')
WHERE block = quadbin_from_lonlat(-73.9857, 40.7484, 13);
```

## Function Reference

### QUADBIN Functions

| Function | Description | Return Type |
|----------|-------------|-------------|
| `quadbin_from_tile(x, y, z)` | Convert tile coordinates to QUADBIN cell | `UBIGINT` |
| `quadbin_to_tile(cell)` | Convert QUADBIN cell to tile coordinates | `STRUCT(x, y, z)` |
| `quadbin_from_lonlat(lon, lat, resolution)` | Convert lon/lat to QUADBIN cell | `UBIGINT` |
| `quadbin_to_lonlat(cell)` | Get center lon/lat of QUADBIN cell | `STRUCT(lon, lat)` |
| `quadbin_resolution(cell)` | Get resolution level (zoom) from cell | `INTEGER` |
| `quadbin_to_bbox(cell)` | Get bounding box of cell | `STRUCT(min_lon, min_lat, max_lon, max_lat)` |
| `quadbin_pixel_xy(lon, lat, res, tile_size)` | Get pixel coordinates within tile | `STRUCT(pixel_x, pixel_y)` |
| `quadbin_cell_for_point(lon, lat, res)` | Alias for quadbin_from_lonlat | `UBIGINT` |

### Spatial Filtering Functions

| Function | Description | Return Type |
|----------|-------------|-------------|
| `quadbin_contains(cell, lon, lat)` | Check if point is within tile | `BOOLEAN` |
| `quadbin_intersects_bbox(cell, min_lon, min_lat, max_lon, max_lat)` | Check if tile intersects bbox | `BOOLEAN` |
| `ST_Intersects(block, geometry)` | Check if tile intersects geometry bbox | `BOOLEAN` |
| `ST_Contains(geometry, block)` | Check if geometry fully contains tile | `BOOLEAN` |

### Hierarchical Functions

| Function | Description | Return Type |
|----------|-------------|-------------|
| `quadbin_to_parent(cell)` | Get parent cell at resolution - 1 | `UBIGINT` |
| `quadbin_to_parent(cell, resolution)` | Get parent cell at specified resolution | `UBIGINT` |
| `quadbin_to_children(cell)` | Get 4 children cells at resolution + 1 | `LIST(UBIGINT)` |
| `quadbin_to_children(cell, resolution)` | Get all children at specified resolution | `LIST(UBIGINT)` |
| `quadbin_sibling(cell)` | Get sibling cells (same parent) | `LIST(UBIGINT)` |
| `quadbin_kring(cell, k)` | Get cells within k distance | `LIST(UBIGINT)` |
| `quadbin_polyfill(geometry, resolution)` | Fill polygon with cells (center mode) | `LIST(UBIGINT)` |
| `quadbin_polyfill(geometry, resolution, mode)` | Fill polygon with cells (mode: 'center', 'intersects', 'contains') | `LIST(UBIGINT)` |

### Spatial Format Functions

| Function | Description | Return Type |
|----------|-------------|-------------|
| `quadbin_to_wkt(cell)` | Convert cell to WKT POLYGON | `VARCHAR` |
| `quadbin_to_geojson(cell)` | Convert cell to GeoJSON | `VARCHAR` |
| `quadbin_boundary(cell)` | Alias for quadbin_to_wkt | `VARCHAR` |
| `ST_GeomFromQuadbin(cell)` | Convert cell to GEOMETRY polygon | `GEOMETRY` |

### Raster Functions

| Function | Description | Return Type |
|----------|-------------|-------------|
| `raquet_pixel(band, dtype, x, y, width, compression)` | Get pixel value at x,y | `DOUBLE` |
| `raquet_pixel(band, metadata, x, y)` | Get pixel using metadata | `DOUBLE` |
| `ST_RasterValue(block, band, lon, lat, dtype, width, compression)` | Get pixel at lon/lat | `DOUBLE` |
| `ST_RasterValue(block, band, lon, lat, metadata)` | Get pixel using metadata | `DOUBLE` |
| `ST_RasterValue(block, band, lon, lat, metadata, band_idx)` | Get specific band pixel | `DOUBLE` |
| `ST_RasterValue(block, band, point_geom, metadata)` | Get pixel using GEOMETRY | `DOUBLE` |
| `raquet_decode_band(band, dtype, width, height, compression)` | Decode entire band | `DOUBLE[]` |
| `ST_RasterSummaryStats(band, dtype, w, h, compression)` | Get tile statistics | `STRUCT(...)` |
| `ST_RasterSummaryStats(band, dtype, w, h, compression, nodata)` | Stats with nodata filter | `STRUCT(...)` |
| `ST_RegionStats(band, block, region, metadata)` | Aggregate stats for pixels within geometry | `STRUCT(...)` |
| `ST_RegionStats(band, block, region, metadata, nodata)` | Region stats with nodata filter | `STRUCT(...)` |
| `ST_Clip(band, block, geometry, metadata)` | Extract pixel values within geometry | `DOUBLE[]` |
| `ST_Clip(band, block, geometry, metadata, nodata)` | Clip with nodata filtering | `DOUBLE[]` |
| `ST_ClipMask(band, block, geometry, metadata, nodata)` | Full tile with outside pixels set to nodata | `DOUBLE[]` |

### Band Math Functions

| Function | Description | Return Type |
|----------|-------------|-------------|
| `ST_NormalizedDifference(band1, band2, metadata)` | Compute (band1-band2)/(band1+band2) per pixel | `DOUBLE[]` |
| `ST_NormalizedDifference(band1, band2, metadata, nodata)` | Normalized difference with nodata handling | `DOUBLE[]` |
| `ST_NDVI(nir_band, red_band, metadata)` | Vegetation index (alias for normalized difference) | `DOUBLE[]` |
| `ST_BandMath(band1, band2, operation, metadata)` | Generic band math: add, subtract, multiply, divide, ndiff | `DOUBLE[]` |
| `ST_NormalizedDifferenceStats(band1, band2, metadata)` | Statistics of normalized difference | `STRUCT(...)` |

### Metadata Functions

| Function | Description | Return Type |
|----------|-------------|-------------|
| `raquet_is_metadata_row(block)` | Check if block = 0 (metadata) | `BOOLEAN` |
| `raquet_is_data_row(block)` | Check if block != 0 (data) | `BOOLEAN` |
| `raquet_parse_metadata(json)` | Parse metadata JSON | `STRUCT(...)` |
| `raquet_band_type(metadata, band_idx)` | Get band data type | `VARCHAR` |
| `raquet_compression(metadata)` | Get compression type | `VARCHAR` |
| `raquet_block_size(metadata)` | Get tile size | `INTEGER` |

## Supported Data Types

| Type | Description |
|------|-------------|
| `uint8` | Unsigned 8-bit integer (0-255) |
| `int8` | Signed 8-bit integer |
| `uint16` | Unsigned 16-bit integer |
| `int16` | Signed 16-bit integer |
| `uint32` | Unsigned 32-bit integer |
| `int32` | Signed 32-bit integer |
| `uint64` | Unsigned 64-bit integer |
| `int64` | Signed 64-bit integer |
| `float32` | 32-bit floating point |
| `float64` | 64-bit floating point |

## Usage Examples

### Reading Raquet Files

```sql
-- Raquet files are standard Parquet, read directly
SELECT * FROM read_parquet('elevation.parquet') LIMIT 10;

-- Filter data rows (exclude metadata row where block = 0)
SELECT * FROM read_parquet('elevation.parquet') WHERE block != 0;

-- Get metadata row
SELECT metadata FROM read_parquet('elevation.parquet') WHERE block = 0;
```

### QUADBIN Spatial Indexing

```sql
-- Convert tile coordinates (x=4096, y=2048, zoom=13) to QUADBIN
SELECT quadbin_from_tile(4096, 2048, 13);
-- Result: 5234261499580514304

-- Convert back to tile coordinates
SELECT (quadbin_to_tile(5234261499580514304)).*;
-- Result: {x: 4096, y: 2048, z: 13}

-- Get QUADBIN cell for New York City at zoom 13
SELECT quadbin_from_lonlat(-73.9857, 40.7484, 13);

-- Get center coordinates of a cell
SELECT (quadbin_to_lonlat(5234261499580514304)).*;

-- Get bounding box of a cell
SELECT (quadbin_to_bbox(5234261499580514304)).*;

-- Get resolution/zoom level
SELECT quadbin_resolution(5234261499580514304);
-- Result: 13
```

### Extracting Raster Values

```sql
-- Get pixel value at specific coordinates
SELECT raquet_pixel(
    band_1,           -- Band data (BLOB)
    'int16',          -- Data type
    128,              -- Pixel X
    128,              -- Pixel Y
    256,              -- Tile width
    'gzip'            -- Compression
) FROM dem WHERE block = 5234261499580514304;

-- Get elevation at a geographic location
SELECT ST_RasterValue(
    block,
    band_1,
    -73.9857,         -- Longitude
    40.7484,          -- Latitude
    'int16',
    256,
    'gzip'
) AS elevation
FROM dem
WHERE block = quadbin_from_lonlat(-73.9857, 40.7484, 13);
```

### Point-Raster Joins

```sql
-- Get elevation for a table of points
SELECT
    p.id,
    p.name,
    ST_RasterValue(
        r.block, r.band_1,
        ST_X(p.geom), ST_Y(p.geom),
        'int16', 256, 'gzip'
    ) AS elevation
FROM points p
JOIN dem r ON quadbin_from_lonlat(ST_X(p.geom), ST_Y(p.geom), 13) = r.block
WHERE r.block != 0;
```

### Summary Statistics

```sql
-- Get statistics for each tile
SELECT
    block,
    (ST_RasterSummaryStats(band_1, 'int16', 256, 256, 'gzip')).*
FROM dem
WHERE block != 0
LIMIT 5;
-- Returns: count, sum, mean, min, max, stddev

-- Filter out nodata values
SELECT
    block,
    (ST_RasterSummaryStats(band_1, 'int16', 256, 256, 'gzip', -9999)).*
FROM dem
WHERE block != 0;
```

### Spatial Filtering

```sql
-- Find tiles that contain a specific point
SELECT block
FROM dem
WHERE quadbin_contains(block, -73.9857, 40.7484)
  AND block != 0;

-- Find tiles intersecting a bounding box
SELECT block
FROM dem
WHERE quadbin_intersects_bbox(block, -74.1, 40.6, -73.8, 40.9)
  AND block != 0;
```

### DuckDB Spatial Integration

```sql
LOAD spatial;
LOAD raquet;

-- Convert QUADBIN cell to geometry for spatial operations
SELECT
    block,
    ST_GeomFromText(quadbin_to_wkt(block)) AS geom
FROM dem
WHERE block != 0
LIMIT 5;

-- Or use the native GEOMETRY conversion
SELECT
    block,
    ST_GeomFromQuadbin(block) AS geom
FROM dem
WHERE block != 0
LIMIT 5;

-- Get elevation using GEOMETRY point
SELECT ST_RasterValue(
    r.block,
    r.band_1,
    ST_Point(-73.9857, 40.7484),
    metadata
) AS elevation
FROM dem r
WHERE block = quadbin_from_lonlat(-73.9857, 40.7484, 13);
```

### Polygon Fill (Polyfill)

```sql
LOAD spatial;
LOAD raquet;

-- Fill a polygon with QUADBIN cells at resolution 10
SELECT unnest(quadbin_polyfill(
    ST_GeomFromText('POLYGON((-74.1 40.6, -73.8 40.6, -73.8 40.9, -74.1 40.9, -74.1 40.6))'),
    10
)) AS cell;

-- Fill with different modes:
-- 'center' (default): cells whose center is inside the polygon
-- 'intersects': cells that intersect the polygon (complete coverage)
-- 'contains': cells fully contained within the polygon (conservative)
SELECT unnest(quadbin_polyfill(
    ST_GeomFromText('POLYGON((-74.1 40.6, -73.8 40.6, -73.8 40.9, -74.1 40.9, -74.1 40.6))'),
    10,
    'intersects'
)) AS cell;
```

### Region Statistics

```sql
LOAD spatial;
LOAD raquet;

-- Get aggregate statistics for pixels within a polygon region
-- First, get the metadata
WITH meta AS (
    SELECT metadata FROM read_parquet('dem.parquet') WHERE block = 0
)
SELECT (ST_RegionStats(
    band_1,
    block,
    ST_GeomFromText('POLYGON((-74.1 40.6, -73.8 40.6, -73.8 40.9, -74.1 40.9, -74.1 40.6))'),
    (SELECT metadata FROM meta)
)).*
FROM read_parquet('dem.parquet')
WHERE block != 0
  AND ST_Intersects(block, ST_GeomFromText('POLYGON((-74.1 40.6, -73.8 40.6, -73.8 40.9, -74.1 40.9, -74.1 40.6))'));
-- Returns: count, sum, mean, min, max, stddev

-- With nodata filtering
SELECT (ST_RegionStats(
    band_1,
    block,
    region_geom,
    metadata,
    -9999.0  -- nodata value to exclude
)).*
FROM raster_data;
```

### PostGIS-like Spatial Predicates

```sql
LOAD spatial;
LOAD raquet;

-- Find tiles that intersect a polygon (fast bbox check)
SELECT block
FROM dem
WHERE ST_Intersects(block, ST_GeomFromText('POLYGON((-74.1 40.6, -73.8 40.6, -73.8 40.9, -74.1 40.9, -74.1 40.6))'))
  AND block != 0;

-- Find tiles fully contained within a polygon
SELECT block
FROM dem
WHERE ST_Contains(
    ST_GeomFromText('POLYGON((-74.1 40.6, -73.8 40.6, -73.8 40.9, -74.1 40.9, -74.1 40.6))'),
    block
)
  AND block != 0;
```

### Clipping Rasters

```sql
LOAD spatial;
LOAD raquet;

-- Extract pixel values within a polygon
WITH meta AS (
    SELECT metadata FROM read_parquet('dem.parquet') WHERE block = 0
)
SELECT
    block,
    ST_Clip(band_1, block,
        ST_GeomFromText('POLYGON((-74.0 40.7, -73.9 40.7, -73.9 40.8, -74.0 40.8, -74.0 40.7))'),
        (SELECT metadata FROM meta)
    ) AS clipped_values
FROM read_parquet('dem.parquet')
WHERE block != 0
  AND ST_Intersects(block, ST_GeomFromText('POLYGON((-74.0 40.7, -73.9 40.7, -73.9 40.8, -74.0 40.8, -74.0 40.7))'));

-- Get full tile with outside pixels masked to nodata
SELECT ST_ClipMask(band_1, block, clip_geom, metadata, -9999.0) AS masked_tile
FROM raster_data;
```

### Band Math and Vegetation Indices

```sql
LOAD raquet;

-- Calculate NDVI (Normalized Difference Vegetation Index)
-- NDVI = (NIR - Red) / (NIR + Red)
WITH meta AS (
    SELECT metadata FROM read_parquet('satellite.parquet') WHERE block = 0
)
SELECT
    block,
    ST_NDVI(band_4, band_3, (SELECT metadata FROM meta)) AS ndvi_values
FROM read_parquet('satellite.parquet')
WHERE block != 0;

-- Get NDVI statistics directly (more efficient than computing array first)
SELECT
    block,
    (ST_NormalizedDifferenceStats(band_4, band_3, metadata)).*
FROM satellite_data
WHERE block != 0;
-- Returns: count, sum, mean, min, max, stddev

-- Generic band math operations
SELECT ST_BandMath(band_1, band_2, 'subtract', metadata) AS difference
FROM raster_data;

-- Supported operations: 'add', 'subtract', 'multiply', 'divide', 'ndiff'
```

### Time-Series Raster Queries

Raquet supports time-series rasters using the CF (Climate and Forecast) conventions, primarily for NetCDF data with temporal dimensions.

```sql
LOAD raquet;

-- Query time-series data structure (CFSR Sea Surface Temperature example)
SELECT
    COUNT(*) as total_rows,
    COUNT(DISTINCT block) as unique_tiles,
    MIN(time_ts) as earliest,
    MAX(time_ts) as latest
FROM read_parquet('cfsr_sst.parquet')
WHERE block != 0;
-- Result: 1296 rows, 3 tiles, 1980-01-01 to 2015-12-01

-- Filter by year using the derived timestamp (easy SQL)
SELECT
    block,
    time_ts,
    (ST_RasterSummaryStats(band_1, 'float64', 256, 256, 'gzip', -999000000.0)).mean as sst_mean
FROM read_parquet('cfsr_sst.parquet')
WHERE block != 0
  AND YEAR(time_ts) = 2010
ORDER BY time_ts
LIMIT 5;

-- Filter by date range
SELECT *
FROM read_parquet('cfsr_sst.parquet')
WHERE block != 0
  AND time_ts >= '2000-01-01' AND time_ts < '2010-01-01';

-- Calculate annual averages for climate analysis
SELECT
    YEAR(time_ts) as year,
    AVG((ST_RasterSummaryStats(band_1, 'float64', 256, 256, 'gzip', -999000000.0)).mean) as annual_sst
FROM read_parquet('cfsr_sst.parquet')
WHERE block = 5192650370358181887  -- specific tile
GROUP BY YEAR(time_ts)
ORDER BY year;

-- Use CF time value directly (for NetCDF compatibility)
-- CF units: "minutes since 1980-01-01 00:00"
SELECT time_cf, time_ts
FROM read_parquet('cfsr_sst.parquet')
WHERE block != 0 AND time_cf < 100000;  -- First ~2 months
```

**Why dual time columns?**

| Column | Purpose | Use Case |
|--------|---------|----------|
| `time_cf` | Authoritative CF value | NetCDF round-trip fidelity, scientific reproducibility |
| `time_ts` | Derived timestamp | Easy SQL filtering, Parquet predicate pushdown |

The `time_ts` column enables efficient queries like `WHERE YEAR(time_ts) = 2010` without needing to understand the CF epoch and units. For non-Gregorian calendars (360_day, noleap), `time_ts` will be NULL and you must use `time_cf`.

## Raquet File Format

A Raquet file is a Parquet file with specific conventions:

### Standard Raster Schema

| Column | Type | Description |
|--------|------|-------------|
| `block` | `UBIGINT` | QUADBIN cell ID (0 for metadata row) |
| `band_1`, `band_2`, ... | `BLOB` | Compressed pixel data |
| `metadata` | `VARCHAR` | JSON metadata (only in row where block=0) |

### Time-Series Raster Schema

For rasters with temporal dimensions (e.g., NetCDF files), two additional columns are added:

| Column | Type | Description |
|--------|------|-------------|
| `block` | `UBIGINT` | QUADBIN cell ID (0 for metadata row) |
| `metadata` | `VARCHAR` | JSON metadata |
| `time_cf` | `DOUBLE` | **Authoritative** CF numeric time value (e.g., minutes since reference) |
| `time_ts` | `TIMESTAMP` | **Derived** timestamp for convenience queries (nullable for non-Gregorian calendars) |
| `band_1` | `BLOB` | Compressed pixel data |

**Key points:**
- `time_cf` is authoritative - preserves exact CF convention values for NetCDF round-trip fidelity
- `time_ts` is derived - enables easy SQL queries with Parquet predicate pushdown
- `time_ts` may be NULL for non-Gregorian calendars (360_day, noleap, etc.)
- Each row represents one tile at one time step
- Rows are sorted by `(block, time_cf)` for efficient filtering

**Metadata JSON Structure:**
```json
{
  "compression": "gzip",
  "block_width": 256,
  "block_height": 256,
  "minresolution": 0,
  "maxresolution": 15,
  "bands": [
    {"name": "elevation", "type": "int16", "nodata": -9999}
  ]
}
```

**Time-Series Metadata (additional fields):**
```json
{
  "time": {
    "cf:units": "minutes since 1980-01-01 00:00",
    "cf:calendar": "standard",
    "interpretation": "period_start",
    "count": 432,
    "range": [0.0, 18889920.0]
  }
}
```

The `interpretation` field indicates how timestamps should be understood:
- `period_start` - timestamp marks the beginning of the period (e.g., Jan 1 for yearly data)
- `instant` - timestamp marks the exact acquisition time

## Coordinate System and Projections

**Important:** This extension works exclusively with **Web Mercator (EPSG:3857)** tiled rasters.

### How It Works

- **QUADBIN** encodes XYZ tile coordinates from the Web Mercator tile pyramid
- User queries use **WGS84 lon/lat (EPSG:4326)** which are converted internally
- All tiles are axis-aligned (no rotation or skew supported)

### Comparison with PostGIS Raster

| Feature | PostGIS Raster | DuckDB Raquet |
|---------|---------------|---------------|
| Projections | Any SRID (UTM, Albers, etc.) | Web Mercator only |
| Geotransform | Full 6-parameter affine | Implicit from QUADBIN |
| Rotation/Skew | Supported | Not supported |
| Reprojection | On-the-fly | Pre-tile to Web Mercator |
| Spatial Index | R-tree (explicit) | QUADBIN (implicit) |

### Why Web Mercator Only?

This is a deliberate design choice for cloud-native raster workflows:

1. **Simplicity** - No coordinate transformation overhead at query time
2. **Performance** - QUADBIN provides free hierarchical spatial indexing
3. **Compatibility** - Matches standard web mapping tile pyramids (XYZ, TMS)
4. **Cloud-Native** - Aligns with COG and other tiled formats

### Data Preparation

If your raster data is in a different projection, convert it to Web Mercator tiles before loading:

```bash
# Using GDAL to create Web Mercator tiles
gdalwarp -t_srs EPSG:3857 input.tif output_webmercator.tif
gdal2tiles.py output_webmercator.tif tiles/
```

Or use tools like [rio-tiler](https://github.com/cogeotiff/rio-tiler) or [titiler](https://github.com/developmentseed/titiler) for dynamic tiling.

## Dependencies

- **DuckDB** - Core database engine (included as submodule)
- **zlib** - For gzip decompression (system dependency)

## License

Apache 2.0

## Related Projects

- [Raquet Specification](https://github.com/CartoDB/raquet) - The Raquet format specification
- [DuckDB](https://duckdb.org) - Fast in-process analytical database
- [DuckDB Spatial](https://github.com/duckdb/duckdb-spatial) - Spatial extension for DuckDB
- [CARTO Analytics Toolbox](https://docs.carto.com/data-and-analysis/analytics-toolbox-for-bigquery/sql-reference/raster) - Raster functions for cloud data warehouses

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass (`make test_sql`)
5. Submit a pull request

## Sample Data

CARTO provides sample Raquet files for testing:

| File | Description | Size | URL |
|------|-------------|------|-----|
| `riyadh.parquet` | Satellite imagery of Riyadh (3 bands, uint16) | 809 MB | [Download](https://storage.googleapis.com/bq_ee_exports/raquet-test/riyadh.parquet) |
| `naip_test.parquet` | NAIP aerial imagery, NY area (3 bands, uint8) | 380 MB | [Download](https://storage.googleapis.com/bq_ee_exports/raquet-test/naip_test.parquet) |
| `europe.parquet` | European coverage | - | [Download](https://storage.googleapis.com/bq_ee_exports/raquet-test/europe.parquet) |
| `TCI.parquet` | Sentinel-2 True Color (3 bands, uint8) | 261 MB | [Download](https://storage.googleapis.com/sdsc_demo25/TCI.parquet) |

**Time-Series Sample Data:**

| File | Description | Time Range | Tiles × Time Steps |
|------|-------------|------------|-------------------|
| `cfsr_sst.parquet` | CFSR Sea Surface Temperature (monthly) | 1980-2015 | 3 × 432 = 1,296 rows |

**Quick test with sample data:**

```sql
-- With httpfs extension for remote access
INSTALL httpfs;
LOAD httpfs;
LOAD raquet;

-- Query Riyadh satellite data
SELECT
    block,
    quadbin_resolution(block) as zoom,
    (quadbin_to_bbox(block)).*
FROM read_parquet('https://storage.googleapis.com/bq_ee_exports/raquet-test/riyadh.parquet')
WHERE block != 0
LIMIT 5;

-- Get pixel value from NAIP imagery
SELECT ST_RasterValue(
    block, band_1, -73.22, 40.91, 'uint8', 256, 'gzip'
) as red_value
FROM read_parquet('https://storage.googleapis.com/bq_ee_exports/raquet-test/naip_test.parquet')
WHERE block = quadbin_from_lonlat(-73.22, 40.91, 18);
```

## Roadmap

**Implemented:**
- [x] `quadbin_to_parent()` - Get parent cell at lower resolution
- [x] `quadbin_to_children()` - Get child cells at higher resolution
- [x] `quadbin_kring()` - Get neighboring cells
- [x] `quadbin_sibling()` - Get sibling cells
- [x] `quadbin_polyfill()` - Fill polygon with QUADBIN cells
- [x] `ST_RegionStats()` - Aggregate raster statistics within polygon regions
- [x] `ST_Intersects()` / `ST_Contains()` - PostGIS-like spatial predicates
- [x] `ST_GeomFromQuadbin()` - Convert QUADBIN cell to GEOMETRY
- [x] `ST_Clip()` / `ST_ClipMask()` - Clip rasters to geometry boundaries
- [x] `ST_NDVI()` / `ST_NormalizedDifference()` - Vegetation and spectral indices
- [x] `ST_BandMath()` - Generic band arithmetic operations
- [x] Time-series support with CF conventions (NetCDF compatible)
- [x] Cloud-native access via httpfs (S3/GCS/HTTP)

**Planned:**
- [ ] `read_raquet()` - Dedicated table function with automatic metadata handling
- [ ] Time-aware filtering in `read_raquet()` macro
- [ ] Performance optimizations for batch pixel extraction
- [ ] Streaming support for large raster files
- [ ] `ST_Resample()` - Change raster resolution (nearest-neighbor)
