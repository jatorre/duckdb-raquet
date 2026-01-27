# Performance Comparison: DuckDB Raquet vs PostGIS Raster vs GDAL

This document compares the performance of the DuckDB Raquet extension against PostGIS Raster and GDAL for common raster operations.

## Test Setup

### Dataset
- **Source**: Sentinel-2 TCI (True Color Image) - RGB, 10m resolution
- **Size**: ~10,980 x 10,980 pixels (~120 million pixels)
- **Location**: Sudan/Egypt region (Nile River area with agricultural fields)
- **Projection**: Web Mercator (EPSG:3857) for fair comparison

### Systems Tested
| System | Version | Storage Format |
|--------|---------|----------------|
| DuckDB Raquet | 0.1.0 | Parquet + QUADBIN tiles (256x256, gzip) |
| PostGIS Raster | 3.6.1 | PostgreSQL raster type (256x256 tiles) |
| GDAL | 3.x | GeoTIFF (single file) |

### Hardware
- Apple Silicon (M-series)
- Tests run locally (no network overhead for DuckDB/GDAL)

## Benchmark Results

### 1. Single Point Value Extraction

Extract pixel value at a single lon/lat coordinate.

| System | Time | Notes |
|--------|------|-------|
| **DuckDB Raquet** | **0.030s** | Includes Parquet read + gzip decompress |
| PostGIS Raster | 0.048s | Includes coordinate transform |
| GDAL | 0.138s | gdallocationinfo command |

**Winner**: DuckDB Raquet (1.6x faster than PostGIS, 4.6x faster than GDAL)

### 2. Multi-Band Single Point (RGB)

Extract all 3 band values at a single point.

| System | Time | Notes |
|--------|------|-------|
| DuckDB Raquet | 0.084s | 3 separate decompressions |
| **PostGIS Raster** | **0.038s** | Tile cached after first band |
| GDAL | 0.138s | All bands in single call |

**Winner**: PostGIS Raster (2.2x faster than DuckDB)

*Note: DuckDB decompresses each band separately. This is an optimization opportunity.*

### 3. Single Tile Statistics

Compute count, sum, mean, min, max, stddev for one 256x256 tile.

| System | Time | Notes |
|--------|------|-------|
| **DuckDB Raquet** | **0.015s** | Streaming Welford algorithm |
| PostGIS Raster | 0.040s | ST_SummaryStats |
| GDAL | N/A | No single-tile equivalent |

**Winner**: DuckDB Raquet (2.7x faster than PostGIS)

### 4. Aggregate Statistics (ALL Tiles)

Compute statistics across the entire raster.

| System | Tiles | Time | Throughput |
|--------|-------|------|------------|
| **DuckDB Raquet** | 3,225 | **0.151s** | 21,358 tiles/sec |
| PostGIS Raster | 1,892 | 2.203s | 859 tiles/sec |
| GDAL | 1 | 0.143s | N/A (single file) |

**Winner**: DuckDB Raquet (14.6x faster than PostGIS)

*Note: DuckDB uses parallel processing across CPU cores.*

### 5. Spatial Filter + Statistics

Query tiles intersecting a bounding box and compute statistics.

| System | Tiles Found | Time |
|--------|-------------|------|
| **DuckDB Raquet** | 438 | **0.061s** |
| PostGIS Raster | 324 | 0.412s |

**Winner**: DuckDB Raquet (6.8x faster than PostGIS)

### 6. Band Math - Normalized Difference

Compute `(Band2 - Band1) / (Band2 + Band1)` for all pixels.

| System | Tiles/Pixels | Time | Notes |
|--------|--------------|------|-------|
| **DuckDB Raquet** | 3,225 tiles | **0.687s** | ST_NormalizedDifferenceStats |
| PostGIS Raster | 1,892 tiles | 72.6s | ST_MapAlgebra (very slow) |
| GDAL | Full raster | 1.95s | gdal_calc.py |

**Winner**: DuckDB Raquet (105x faster than PostGIS, 2.8x faster than GDAL)

## Summary Chart

```
Operation                    DuckDB    PostGIS    GDAL      Best
─────────────────────────────────────────────────────────────────
Single point (1 band)        0.030s    0.048s     0.138s    DuckDB
Single point (3 bands)       0.084s    0.038s     0.138s    PostGIS
Single tile stats            0.015s    0.040s     N/A       DuckDB
ALL tiles stats              0.151s    2.203s     0.143s    DuckDB*
Spatial filter + stats       0.061s    0.412s     N/A       DuckDB
Band math (all tiles)        0.687s    72.60s     1.95s     DuckDB

* GDAL processes single file; DuckDB processes tiled data
```

## Cloud-Native Performance (Google Cloud Storage)

One of DuckDB Raquet's key advantages is its cloud-native architecture. Using Parquet's columnar format with predicate pushdown, queries can efficiently access remote data without downloading entire files.

### Test Setup
- **File**: TCI.parquet hosted on Google Cloud Storage
- **File Size**: 261 MB (3,226 tiles across 8 zoom levels)
- **URL**: `gs://sdsc_demo25/TCI.parquet`
- **Network**: Standard internet connection

### Data Transfer Efficiency

The Raquet format enables minimal data transfer through:

| Component | Size | Notes |
|-----------|------|-------|
| Full file | 261 MB | Total Parquet file size |
| Single tile (3 bands, compressed) | ~1.8 KB | Only fetched data for point query |
| Parquet metadata | ~50 KB | Cached after first access |
| Single band (compressed) | 300-800 bytes | Gzip-compressed 256x256 tile |

### Cloud Benchmark Results

#### Local vs GCS Performance

| Query | Local | GCS | Data Transfer | Overhead |
|-------|-------|-----|---------------|----------|
| Single point (3 bands) | 0.093s | 4.3s | ~2 KB | ~46x (network latency) |
| All z14 tiles stats (2,352 tiles) | 0.097s | 2.2s | ~165 MB | ~23x |
| Resolution filter (z13, 625 tiles) | ~0.06s | 2.4s | ~44 MB | ~40x |
| Spatial filter + stats | ~0.06s | 2.2s | Variable | ~37x |

#### Key Observations

1. **Predicate Pushdown**: Block/resolution filters pushed to Parquet reader
   - Only matching row groups are fetched
   - Single tile query downloads ~2KB, not 261MB

2. **Column Pruning**: Only requested bands are downloaded
   - Query on `band_1` only fetches that column
   - Metadata queries are extremely fast (~0.7s)

3. **Parallel Fetching**: Multiple tiles fetched concurrently
   - Full z14 scan (2,352 tiles) completes in 2.2s
   - Effective throughput: ~75 MB/s

### Cloud Query Examples

```sql
LOAD httpfs;
LOAD raquet;

-- Single point extraction from GCS (downloads ~2KB)
SELECT
    raquet_pixel(band_1, 'uint8',
        (quadbin_pixel_xy(33.5, 16.85, 14, 256)).pixel_x,
        (quadbin_pixel_xy(33.5, 16.85, 14, 256)).pixel_y,
        256, 'gzip') as red
FROM read_parquet('https://storage.googleapis.com/sdsc_demo25/TCI.parquet')
WHERE block = quadbin_from_lonlat(33.5, 16.85, 14);

-- Aggregate stats on specific zoom level
SELECT COUNT(*) as tiles,
       AVG((ST_RasterSummaryStats(band_1, 'uint8', 256, 256, 'gzip', 0)).mean) as avg
FROM read_parquet('https://storage.googleapis.com/sdsc_demo25/TCI.parquet')
WHERE quadbin_resolution(block) = 14;

-- Resolution-based filtering (only z13 tiles)
SELECT quadbin_resolution(block) as res,
       COUNT(*) as tiles,
       SUM((ST_RasterSummaryStats(band_1, 'uint8', 256, 256, 'gzip', 0)).count) as pixels
FROM read_parquet('https://storage.googleapis.com/sdsc_demo25/TCI.parquet')
WHERE quadbin_resolution(block) = 13
GROUP BY res;
```

### Cloud Performance Analysis

The network overhead (23-46x slower than local) is expected, but the architecture provides significant advantages:

1. **No Pre-download Required**: Query directly against cloud storage
2. **Minimal Data Transfer**: Only fetch what's needed
3. **Scalable**: Same query works on TB-scale datasets
4. **Cost Effective**: Pay only for data transferred

For comparison, downloading the full 261MB file would take ~20s on a 100Mbps connection, while a single-point query completes in 4.3s transferring only ~2KB.

## Performance Analysis

### Why DuckDB Raquet is Fast

1. **Columnar Storage (Parquet)**
   - Only reads required columns
   - Excellent compression ratios
   - Predicate pushdown to storage layer

2. **Parallel Processing**
   - Utilizes all CPU cores automatically
   - Each tile can be processed independently
   - No lock contention

3. **No Client-Server Overhead**
   - Runs in-process
   - No network round-trips
   - No query parsing/planning overhead for simple operations

4. **QUADBIN Spatial Indexing**
   - Implicit spatial index from tile IDs
   - O(1) tile lookup by coordinates
   - Efficient range queries

5. **Streaming Algorithms**
   - Welford's algorithm for statistics (single pass)
   - No intermediate arrays needed
   - Memory efficient

### Where PostGIS Excels

1. **Multi-band single-tile queries**
   - Tile caching benefits repeated access
   - All bands loaded together

2. **Complex spatial operations**
   - Full PostGIS spatial functions available
   - Mature R-tree indexing

3. **Transactional workloads**
   - ACID compliance
   - Concurrent writes

### Where GDAL Excels

1. **Single-file operations**
   - No tiling overhead
   - Direct pixel access

2. **Format conversion**
   - Extensive format support
   - Reprojection capabilities

## Recommendations

### Use DuckDB Raquet when:
- Processing many tiles (analytics, aggregations)
- Computing band math / spectral indices
- Running spatial filtered queries
- Building data pipelines
- Working with cloud storage (Parquet on S3/GCS)

### Use PostGIS Raster when:
- Need transactional guarantees
- Require complex spatial joins with vector data
- Working with small numbers of tiles repeatedly
- Existing PostgreSQL infrastructure

### Use GDAL when:
- Format conversion needed
- Reprojection required
- Single-file processing
- Command-line workflows

## Reproducing These Benchmarks

### DuckDB Raquet
```sql
LOAD raquet;

-- Single point
SELECT raquet_pixel(band_1, 'uint8',
    (quadbin_pixel_xy(33.5, 16.85, 14, 256)).pixel_x,
    (quadbin_pixel_xy(33.5, 16.85, 14, 256)).pixel_y,
    256, 'gzip')
FROM read_parquet('data.parquet')
WHERE block = quadbin_from_lonlat(33.5, 16.85, 14);

-- All tiles statistics
SELECT
    COUNT(*) as num_tiles,
    SUM((ST_RasterSummaryStats(band_1, 'uint8', 256, 256, 'gzip', 0)).count) as pixels,
    AVG((ST_RasterSummaryStats(band_1, 'uint8', 256, 256, 'gzip', 0)).mean) as mean
FROM read_parquet('data.parquet')
WHERE block != 0;

-- Band math
SELECT ST_NormalizedDifferenceStats(band_2, band_1, metadata) as stats
FROM read_parquet('data.parquet')
WHERE block != 0;
```

### PostGIS Raster
```sql
-- Single point
SELECT ST_Value(rast, 1, ST_Transform(ST_SetSRID(ST_Point(33.5, 16.85), 4326), 3857))
FROM raster_table
WHERE ST_Intersects(rast, ST_Transform(ST_SetSRID(ST_Point(33.5, 16.85), 4326), 3857));

-- All tiles statistics
SELECT
    COUNT(*) as num_tiles,
    SUM((ST_SummaryStats(rast, 1, true)).count) as pixels,
    AVG((ST_SummaryStats(rast, 1, true)).mean) as mean
FROM raster_table;
```

### GDAL
```bash
# Single point
gdallocationinfo -wgs84 data.tif 33.5 16.85

# Statistics
gdalinfo -stats data.tif

# Band math
gdal_calc.py -A data.tif --A_band=2 -B data.tif --B_band=1 \
    --calc="(A-B)/(A+B)" --outfile=ndiff.tif
```

## Conclusion

DuckDB Raquet provides **10-100x performance improvements** for analytical raster workloads compared to PostGIS Raster. The combination of columnar storage, parallel processing, and streaming algorithms makes it particularly well-suited for:

- Large-scale raster analytics
- Band math and spectral index computation
- Spatial filtering and aggregation
- Cloud-native data pipelines

For transactional workloads or complex spatial operations with vector data, PostGIS remains a strong choice. GDAL continues to excel at format conversion and single-file processing.
