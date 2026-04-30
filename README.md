# DuckDB Raquet Extension

A DuckDB extension for working with [Raquet](https://github.com/CartoDB/raquet) raster data stored in Apache Parquet format with QUADBIN spatial indexing.

## What is Raquet?

**Raquet** is a specification created by [CARTO](https://carto.com) for storing raster data efficiently using:

- **Apache Parquet** - Columnar storage for efficient compression and query performance
- **QUADBIN** - A spatial indexing scheme encoding Web Mercator tile coordinates into 64-bit integers
- **Binary Band Data** - Pixel values stored as compressed BLOBs (gzip, JPEG, or WebP)

This extension enables DuckDB to query Raquet files directly using SQL, with functions for spatial indexing, pixel extraction, and raster analytics.

## Features

- **PostGIS-like API** - `ST_RasterValue`, `ST_RasterSummaryStats`, `ST_RegionStats`, `ST_Clip`
- **QUADBIN Functions** - Convert between tiles, cells, and geographic coordinates
- **Polygon Fill** - Fill polygons with QUADBIN cells at any resolution
- **Band Math** - `ST_NormalizedDifference`, `ST_BandMath` for spectral indices
- **Spatial Predicates** - `ST_Intersects`, `ST_Contains` for raster-vector operations
- **Time-Series Support** - CF conventions for temporal rasters (NetCDF compatible)
- **Raster Ingestion** - `read_raster()` converts GeoTIFF/NetCDF/any GDAL format to Raquet (requires GDAL)
- **Cloud-Native** - Query remote Parquet files on S3/GCS with predicate pushdown
- **Native GEOMETRY Support** - Uses DuckDB 1.5+ native GEOMETRY types
- **Interleaved Band Layout** - Band Interleaved by Pixel (BIP) format
- **Lossy Compression** - JPEG/WebP compressed tiles (up to 15x smaller files)

## Installation

### From Community Extensions (Coming with DuckDB 1.5)

```sql
INSTALL raquet FROM community;
LOAD raquet;
```

### Building from Source

**Prerequisites:** CMake 3.12+, C++17 compiler, zlib, libjpeg (optional), libwebp (optional), GDAL (optional, for `read_raster`)

```bash
git clone https://github.com/CartoDB/duckdb-raquet.git
cd duckdb-raquet
git submodule update --init --recursive
make release
make test
```

## Quick Start

```sql
LOAD raquet;

-- Read a Raquet file (metadata is propagated automatically)
SELECT * FROM read_raquet('raster.parquet') LIMIT 5;

-- Point query: get RGB values at a location
SELECT
    ST_RasterValue(block, band_1, ST_Point(-73.98, 40.75), metadata) AS red,
    ST_RasterValue(block, band_2, ST_Point(-73.98, 40.75), metadata) AS green,
    ST_RasterValue(block, band_3, ST_Point(-73.98, 40.75), metadata) AS blue
FROM read_raquet_at('imagery.parquet', -73.98, 40.75);

-- Tile statistics
SELECT
    block,
    (ST_RasterSummaryStats(band_1, metadata)).mean AS avg_value,
    (ST_RasterSummaryStats(band_1, metadata)).stddev AS std_value
FROM read_raquet('dem.parquet')
LIMIT 5;
```

### Raster Ingestion (GeoTIFF → Raquet)

```sql
LOAD raquet;

-- Convert any raster to Raquet format (requires GDAL)
COPY (SELECT * FROM read_raster('elevation.tif') ORDER BY block)
TO 'elevation.parquet' (FORMAT parquet);

-- With options: resampling, compression, zoom level
COPY (
    SELECT * FROM read_raster('satellite.tif',
        compression='gzip',
        resampling='bilinear',
        block_size=256,
        max_zoom=12
    ) ORDER BY block
) TO 'satellite.parquet' (FORMAT parquet);

-- With per-tile statistics
COPY (
    SELECT * FROM read_raster('dem.tif', statistics=true) ORDER BY block
) TO 'dem.parquet' (FORMAT parquet);
```

### Cloud-Native Queries

```sql
LOAD httpfs;
LOAD raquet;

-- Single point extraction from GCS (downloads only the needed tile, ~2KB)
SELECT
    ST_RasterValue(block, band_1, ST_Point(33.5, 16.85), metadata) AS red,
    ST_RasterValue(block, band_2, ST_Point(33.5, 16.85), metadata) AS green,
    ST_RasterValue(block, band_3, ST_Point(33.5, 16.85), metadata) AS blue
FROM read_raquet_at('https://storage.googleapis.com/sdsc_demo25/TCI.parquet', 33.5, 16.85);
```

## Function Reference

### ST_* Functions (Public API)

These are the primary functions for working with raster data.

#### Table Macros

| Function | Description |
|----------|-------------|
| `ST_Raster(tbl)` | Read raster data from an iceberg/table |
| `ST_Raster(tbl, geometry)` | Spatial filter with auto-detected resolution |
| `ST_Raster(tbl, geometry, resolution)` | Spatial filter with explicit resolution |
| `ST_RasterAt(tbl, point)` | Point query from table (auto resolution) |
| `ST_RasterAt(tbl, lon, lat)` | Point query from table with lon/lat |
| `ST_RasterAt(tbl, lon, lat, resolution)` | Point query with explicit resolution |

#### Pixel Value Extraction

| Function | Description | Return |
|----------|-------------|--------|
| `ST_RasterValue(block, band, point, metadata)` | Get pixel value at point | `DOUBLE` |
| `ST_RasterValue(block, band, point, metadata, band_name)` | Get pixel by band name (multi-band) | `DOUBLE` |

#### Tile Statistics

| Function | Description | Return |
|----------|-------------|--------|
| `ST_RasterSummaryStats(band, metadata)` | Summary statistics (auto nodata from metadata) | `STRUCT(count, sum, mean, min, max, stddev)` |
| `ST_RasterSummaryStats(band, metadata, nodata)` | Summary statistics with explicit nodata | `STRUCT(count, sum, mean, min, max, stddev)` |

#### Region Statistics (Aggregate)

| Function | Description | Return |
|----------|-------------|--------|
| `ST_RegionStats(band, block, region, metadata)` | Stats for pixels within geometry | `STRUCT(count, sum, mean, min, max, stddev)` |
| `ST_RegionStats(band, block, region, metadata, nodata)` | With nodata filtering | `STRUCT(...)` |
| `ST_RegionStats(band, block, region, metadata, resolution)` | With explicit resolution | `STRUCT(...)` |
| `ST_RegionStats(band, block, region, metadata, nodata, resolution)` | Full variant | `STRUCT(...)` |

#### Clipping

| Function | Description | Return |
|----------|-------------|--------|
| `ST_Clip(band, block, geometry, metadata)` | Extract pixels within geometry | `DOUBLE[]` |
| `ST_Clip(band, block, geometry, metadata, nodata)` | Clip with nodata filtering | `DOUBLE[]` |
| `ST_ClipMask(band, block, geometry, metadata, nodata)` | Full tile, outside set to nodata | `DOUBLE[]` |

#### Band Math

| Function | Description | Return |
|----------|-------------|--------|
| `ST_NormalizedDifference(band1, band2, metadata)` | (b1-b2)/(b1+b2) per pixel | `DOUBLE[]` |
| `ST_NormalizedDifference(band1, band2, metadata, nodata)` | With nodata handling | `DOUBLE[]` |
| `ST_BandMath(band1, band2, operation, metadata)` | Generic: add, subtract, multiply, divide | `DOUBLE[]` |
| `ST_NormalizedDifferenceStats(band1, band2, metadata)` | Stats of normalized difference | `STRUCT(...)` |

#### Spatial Predicates

| Function | Description | Return |
|----------|-------------|--------|
| `ST_Intersects(block, geometry)` | Tile intersects geometry bbox | `BOOLEAN` |
| `ST_Contains(geometry, block)` | Geometry fully contains tile | `BOOLEAN` |

#### Geometry Helpers

| Function | Description | Return |
|----------|-------------|--------|
| `ST_Point(lon, lat)` | Create POINT geometry | `GEOMETRY` |
| `ST_X(geometry)` | Extract longitude from point | `DOUBLE` |
| `ST_Y(geometry)` | Extract latitude from point | `DOUBLE` |
| `ST_GeomFromQuadbin(cell)` | Convert cell to GEOMETRY polygon | `GEOMETRY` |
| `ST_Band(metadata, band_name)` | Get band index by name | `INTEGER` |

### QUADBIN Functions

| Function | Description | Return |
|----------|-------------|--------|
| `quadbin_from_tile(x, y, z)` | Tile coordinates to QUADBIN | `UBIGINT` |
| `quadbin_to_tile(cell)` | QUADBIN to tile coordinates | `STRUCT(x, y, z)` |
| `quadbin_from_lonlat(lon, lat, resolution)` | Lon/lat to QUADBIN cell | `UBIGINT` |
| `quadbin_to_lonlat(cell)` | Cell center as lon/lat | `STRUCT(lon, lat)` |
| `quadbin_resolution(cell)` | Get resolution level | `INTEGER` |
| `quadbin_to_bbox(cell)` | Bounding box of cell | `STRUCT(...)` |
| `quadbin_pixel_xy(lon, lat, res, tile_size)` | Pixel coordinates within tile | `STRUCT(pixel_x, pixel_y)` |
| `quadbin_to_parent(cell)` | Parent cell (resolution - 1) | `UBIGINT` |
| `quadbin_to_parent(cell, resolution)` | Parent at specific resolution | `UBIGINT` |
| `quadbin_to_children(cell)` | 4 children at resolution + 1 | `LIST(UBIGINT)` |
| `quadbin_to_children(cell, resolution)` | Children at specific resolution | `LIST(UBIGINT)` |
| `quadbin_sibling(cell)` | Sibling cells (same parent) | `LIST(UBIGINT)` |
| `quadbin_kring(cell, k)` | Cells within k distance | `LIST(UBIGINT)` |
| `QUADBIN_POLYFILL(geometry, resolution)` | Fill geometry with cells | `LIST(UBIGINT)` |
| `QUADBIN_POLYFILL(geometry, resolution, mode)` | Fill with mode: center/intersects/contains | `LIST(UBIGINT)` |
| `quadbin_to_wkt(cell)` | Cell as WKT POLYGON | `VARCHAR` |
| `quadbin_to_geojson(cell)` | Cell as GeoJSON | `VARCHAR` |

### read_raquet* Functions (File I/O)

| Function | Description |
|----------|-------------|
| `read_raquet(file)` | Read all data rows (metadata propagated) |
| `read_raquet(file, geometry)` | Spatial filter with auto resolution |
| `read_raquet(file, geometry, resolution)` | Spatial filter with explicit resolution |
| `read_raquet_at(file, point)` | Point query (auto resolution) |
| `read_raquet_at(file, lon, lat)` | Point query with lon/lat |
| `read_raquet_at(file, lon, lat, resolution)` | Point query with explicit resolution |
| `read_raquet_metadata(file)` | Read metadata row only |

### read_raster (Raster Ingestion — requires GDAL)

Converts any GDAL-supported raster format (GeoTIFF, NetCDF, COG, etc.) into a Raquet table.
Handles CRS reprojection to Web Mercator, tiling, band compression, and overview pyramid generation.

```sql
SELECT * FROM read_raster(file, [named parameters...])
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `file` | `VARCHAR` | (required) | Path to raster file |
| `compression` | `VARCHAR` | `'gzip'` | Band compression: `gzip`, `jpeg`, `webp`, `none` |
| `resampling` | `VARCHAR` | `'nearest'` | Resampling: `nearest`, `bilinear`, `cubic`, `cubicspline`, `lanczos`, `average`, `mode` |
| `block_size` | `INTEGER` | `256` | Tile size in pixels: `256`, `512`, or `1024` |
| `max_zoom` | `INTEGER` | auto | Maximum zoom level (auto-detected from resolution) |
| `min_zoom` | `INTEGER` | auto | Minimum zoom level for overview pyramid |
| `overviews` | `VARCHAR` | `'auto'` | Overview mode: `auto` (full pyramid) or `none` (native zoom only) |
| `band_layout` | `VARCHAR` | `'sequential'` | Band layout: `sequential` or `interleaved` |
| `quality` | `INTEGER` | `85` | Compression quality for JPEG/WebP (1-100) |
| `statistics` | `BOOLEAN` | `false` | Compute per-tile statistics (count, min, max, sum, mean, stddev) |
| `zoom_strategy` | `VARCHAR` | `'auto'` | Zoom selection: `auto` (round), `lower` (floor, coarser), `upper` (ceil, finer) |
| `format` | `VARCHAR` | `'v0.5.0'` | Metadata format: `'v0.5.0'` (spec) or `'v0'` (legacy v0.1.0 shape consumed by raquet 0.2.5 / CARTO platform) |
| `approx` | `BOOLEAN` | `true` | Use GDAL's `approxOK=TRUE` overview-based statistics (fast). Set `false` for exact full-resolution stats |

**Output columns:** `block` (UBIGINT), `metadata` (VARCHAR), `band_1` ... `band_N` (BLOB).
When `statistics=true`, adds `band_N_count`, `band_N_min`, `band_N_max`, `band_N_sum`, `band_N_mean`, `band_N_stddev` columns.

**NetCDF time dimension:** When reading NetCDF files with CF time metadata, the time dimension info
(`cf:units`, `cf:calendar`) is automatically included in the output metadata JSON.

**Parallelism:** Native-zoom tiles are processed in parallel (each thread opens its own GDAL handle).
Use `overviews='none'` for maximum parallelism. Overview pyramid tiles are processed single-threaded.

**Typical workflow:**
```sql
-- Convert and write to Parquet (ORDER BY block for optimal spatial queries)
COPY (SELECT * FROM read_raster('input.tif') ORDER BY block)
TO 'output.parquet' (FORMAT parquet);

-- Then query with read_raquet
SELECT ST_RasterValue(block, band_1, ST_Point(lon, lat), metadata)
FROM read_raquet('output.parquet');
```

### Validation

| Function | Description | Return |
|----------|-------------|--------|
| `raquet_validate_metadata(json)` | Validate raquet metadata JSON | `STRUCT(is_valid, errors, warnings, num_blocks, num_bands, zoom_range)` |

Validates metadata structure: file_format, compression, tiling scheme, zoom ranges, band types.

```sql
-- Validate a raquet file's metadata
SELECT raquet_validate_metadata(metadata) FROM read_raquet_metadata('file.parquet');
```

### Advanced / Internal Functions

These are lower-level functions for specialized workflows.

| Function | Description | Return |
|----------|-------------|--------|
| `raquet_pixel(band, dtype, x, y, width, compression)` | Pixel by x,y with explicit params | `DOUBLE` |
| `raquet_pixel(band, metadata, x, y)` | Pixel by x,y with metadata | `DOUBLE` |
| `raquet_pixel(band, metadata, band_index, x, y)` | Multi-band pixel by index | `DOUBLE` |
| `raquet_decode_band(band, dtype, w, h, compression)` | Decode entire band | `DOUBLE[]` |
| `raquet_pixel_interleaved(pixels, metadata, band_idx, x, y)` | Interleaved layout pixel | `DOUBLE` |
| `raquet_parse_metadata(json)` | Parse metadata JSON | `STRUCT(...)` |
| `ST_RasterSummaryStats(band, dtype, w, h, compression)` | Stats with explicit params | `STRUCT(...)` |
| `ST_RasterSummaryStats(band, dtype, w, h, compression, nodata)` | Stats with explicit params + nodata | `STRUCT(...)` |

## Usage Examples

### Point Queries (Single Location)

```sql
LOAD raquet;

-- Simplest form: get all bands at a point
SELECT
    ST_RasterValue(block, band_1, ST_Point(33.5, 16.85), metadata) AS red,
    ST_RasterValue(block, band_2, ST_Point(33.5, 16.85), metadata) AS green,
    ST_RasterValue(block, band_3, ST_Point(33.5, 16.85), metadata) AS blue
FROM read_raquet_at('imagery.parquet', 33.5, 16.85);

-- Using lon/lat directly in read_raquet_at
SELECT *
FROM read_raquet_at('dem.parquet', -73.98, 40.75);
```

### Spatial Filtering (Regions)

```sql
LOAD raquet;

-- Read tiles within a polygon
SELECT count(*) AS tile_count
FROM read_raquet(
    'dem.parquet',
    'POLYGON((-74.1 40.6, -73.8 40.6, -73.8 40.9, -74.1 40.9, -74.1 40.6))'::GEOMETRY
);

-- Region statistics across tiles within a polygon
SELECT (ST_RegionStats(
    band_1, block,
    'POLYGON((-74.1 40.6, -73.8 40.6, -73.8 40.9, -74.1 40.9, -74.1 40.6))'::GEOMETRY,
    metadata
)).*
FROM read_raquet('dem.parquet');
```

### Tile Statistics

```sql
LOAD raquet;

-- Statistics per tile (auto nodata from metadata)
SELECT
    block,
    (ST_RasterSummaryStats(band_1, metadata)).mean AS avg_elevation,
    (ST_RasterSummaryStats(band_1, metadata)).stddev AS elevation_std
FROM read_raquet('dem.parquet')
LIMIT 10;

-- Override nodata value
SELECT
    block,
    (ST_RasterSummaryStats(band_1, metadata, -9999.0)).mean AS avg_temp
FROM read_raquet('temperature.parquet')
LIMIT 10;
```

### Band Math (Vegetation Indices)

```sql
LOAD raquet;

-- NDVI: (NIR - Red) / (NIR + Red)
SELECT
    block,
    ST_NormalizedDifference(band_4, band_3, metadata) AS ndvi_values
FROM read_raquet('satellite.parquet')
LIMIT 1;

-- Get NDVI statistics directly
SELECT
    block,
    (ST_NormalizedDifferenceStats(band_4, band_3, metadata)).*
FROM read_raquet('satellite.parquet')
LIMIT 5;
```

### Clipping

```sql
LOAD raquet;

-- Extract pixel values within a polygon
SELECT
    block,
    ST_Clip(band_1, block,
        'POLYGON((-74.0 40.7, -73.9 40.7, -73.9 40.8, -74.0 40.8, -74.0 40.7))'::GEOMETRY,
        metadata
    ) AS clipped_values
FROM read_raquet('dem.parquet');

-- Full tile with outside pixels masked to nodata
SELECT ST_ClipMask(band_1, block, clip_geom, metadata, -9999.0) AS masked_tile
FROM read_raquet('raster.parquet');
```

### Iceberg / Table Queries

```sql
LOAD raquet;

-- Read from an iceberg table
SELECT * FROM ST_Raster('catalog.schema.raster_table') LIMIT 5;

-- Point query on a table
SELECT
    ST_RasterValue(block, band_1, ST_Point(-3.7, 40.4), metadata) AS value
FROM ST_RasterAt('catalog.schema.raster_table', -3.7, 40.4);

-- Spatial filter on a table
SELECT count(*) AS tiles
FROM ST_Raster(
    'catalog.schema.raster_table',
    'POLYGON((-4 40, -3 40, -3 41, -4 41, -4 40))'::GEOMETRY
);
```

### Time-Series Rasters

```sql
LOAD raquet;

-- Explore time-series structure
SELECT
    COUNT(*) as total_rows,
    COUNT(DISTINCT block) as tiles,
    MIN(time_ts) as earliest,
    MAX(time_ts) as latest
FROM read_raquet('cfsr_sst.parquet');

-- Filter by year
SELECT
    time_ts,
    (ST_RasterSummaryStats(band_1, metadata, -999000000.0)).mean AS sst
FROM read_raquet('cfsr_sst.parquet')
WHERE YEAR(time_ts) = 2010
ORDER BY time_ts
LIMIT 5;

-- Decadal averages
SELECT
    (YEAR(time_ts) / 10) * 10 AS decade,
    AVG((ST_RasterSummaryStats(band_1, metadata, -999000000.0)).mean) AS avg_sst
FROM read_raquet('cfsr_sst.parquet')
GROUP BY (YEAR(time_ts) / 10) * 10
ORDER BY decade;
```

### QUADBIN Spatial Indexing

```sql
LOAD raquet;

-- Tile coordinates to QUADBIN
SELECT quadbin_from_tile(4096, 2048, 13);

-- Lon/lat to QUADBIN cell
SELECT quadbin_from_lonlat(-73.98, 40.75, 13);

-- Get cell properties
SELECT
    quadbin_resolution(5234261499580514304) AS zoom,
    (quadbin_to_lonlat(5234261499580514304)).* AS center,
    (quadbin_to_bbox(5234261499580514304)).* AS bbox;

-- Fill polygon with cells
SELECT unnest(QUADBIN_POLYFILL(
    'POLYGON((-74.1 40.6, -73.8 40.6, -73.8 40.9, -74.1 40.9, -74.1 40.6))'::GEOMETRY,
    10
)) AS cell;
```

## Raquet File Format

A Raquet file is a Parquet file with specific conventions:

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `block` | `UBIGINT` | QUADBIN cell ID (0 for metadata row) |
| `band_1`, `band_2`, ... | `BLOB` | Pixel data per band (sequential layout) |
| `pixels` | `BLOB` | All bands interleaved (interleaved layout) |
| `metadata` | `VARCHAR` | JSON metadata (only where block=0) |
| `time_cf` | `DOUBLE` | CF numeric time value (time-series only) |
| `time_ts` | `TIMESTAMP` | Derived timestamp (time-series only) |

### Metadata JSON

```json
{
  "file_format": "raquet",
  "version": "0.5.0",
  "compression": "gzip",
  "compression_quality": 85,
  "band_layout": "interleaved",
  "crs": "EPSG:3857",
  "bounds": [-180, -85.05, 180, 85.05],
  "bounds_crs": "EPSG:4326",
  "width": 64293,
  "height": 86760,
  "tiling": {
    "block_width": 256,
    "block_height": 256,
    "min_zoom": 0,
    "max_zoom": 15,
    "scheme": "quadbin"
  },
  "bands": [
    {"name": "red", "type": "uint8",
     "STATISTICS_MINIMUM": 0, "STATISTICS_MAXIMUM": 255,
     "STATISTICS_MEAN": 127.5, "STATISTICS_STDDEV": 50.0,
     "STATISTICS_VALID_PERCENT": 100.0},
    {"name": "green", "type": "uint8"},
    {"name": "blue", "type": "uint8", "nodata": 0,
     "colorinterp": "blue",
     "colortable": {"0": [0,0,0,0], "1": [0,0,255,255]}}
  ]
}
```

### Supported Data Types

`uint8`, `int8`, `uint16`, `int16`, `uint32`, `int32`, `uint64`, `int64`, `float32`, `float64`

## Coordinate System

This extension works with **Web Mercator (EPSG:3857)** tiled rasters. User queries use **WGS84 lon/lat (EPSG:4326)** which are converted internally.

## Performance

DuckDB Raquet provides **10-100x improvements** for analytical raster workloads versus PostGIS Raster:

| Operation | DuckDB Raquet | PostGIS Raster | Speedup |
|-----------|---------------|----------------|---------|
| Single point extraction | 0.030s | 0.048s | 1.6x |
| All tiles statistics | 0.15s | 2.2s | **14.6x** |
| Band math (NDVI) | 0.69s | 72.6s | **105x** |
| Spatial filter + stats | 0.06s | 0.41s | **6.8x** |

See [docs/PERFORMANCE_COMPARISON.md](docs/PERFORMANCE_COMPARISON.md) for full benchmarks.

## Sample Data

| File | Description | Size |
|------|-------------|------|
| [TCI.parquet](https://storage.googleapis.com/sdsc_demo25/TCI.parquet) | Sentinel-2 True Color (3 bands, uint8) | 261 MB |
| [riyadh.parquet](https://storage.googleapis.com/bq_ee_exports/raquet-test/riyadh.parquet) | Satellite imagery (3 bands, uint16) | 809 MB |
| [naip_test.parquet](https://storage.googleapis.com/bq_ee_exports/raquet-test/naip_test.parquet) | NAIP aerial imagery (3 bands, uint8) | 380 MB |

## Dependencies

- **DuckDB 1.5+** - Core database engine
- **zlib** - For gzip compression/decompression
- **libjpeg** (optional) - For JPEG lossy compression
- **libwebp** (optional) - For WebP lossy compression
- **GDAL** (optional) - For `read_raster()` raster ingestion (CRS reprojection, format support)

## License

Apache 2.0

## Related Projects

- [Raquet Specification](https://github.com/CartoDB/raquet)
- [DuckDB](https://duckdb.org)
- [DuckDB Spatial](https://github.com/duckdb/duckdb-spatial)
- [CARTO Analytics Toolbox](https://docs.carto.com/data-and-analysis/analytics-toolbox-for-bigquery/sql-reference/raster)
