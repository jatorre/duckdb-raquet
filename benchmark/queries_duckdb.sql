-- DuckDB Raquet Benchmark Queries
-- Run with: ./build/release/duckdb -c "LOAD raquet; .timer on; .read benchmark/queries_duckdb.sql"

-- ============================================================================
-- Configuration
-- ============================================================================
-- Test with GCS direct access (HTTPS URL)
-- For local files, replace with local path

.timer on

-- ============================================================================
-- Q1: Point Query - Get pixel value at a specific coordinate
-- ============================================================================
-- TCI.parquet - Sentinel-2 True Color
-- Location: Approx center of coverage area

SELECT '=== Q1: Point Query (ST_RasterValue) ===' as benchmark;

SELECT
    ST_RasterValue(block, band_1, 'POINT(-3.7 40.4)'::GEOMETRY, metadata) as red,
    ST_RasterValue(block, band_2, 'POINT(-3.7 40.4)'::GEOMETRY, metadata) as green,
    ST_RasterValue(block, band_3, 'POINT(-3.7 40.4)'::GEOMETRY, metadata) as blue
FROM read_raquet('https://storage.googleapis.com/sdsc_demo25/TCI.parquet')
WHERE quadbin_contains(block, 'POINT(-3.7 40.4)'::GEOMETRY);

-- ============================================================================
-- Q2: Tile Statistics - Compute stats for a single tile
-- ============================================================================

SELECT '=== Q2: Tile Statistics (ST_RasterSummaryStats) ===' as benchmark;

SELECT
    block,
    ST_RasterSummaryStats(band_1, 'uint8', 256, 256, 'gzip') as stats
FROM read_parquet('https://storage.googleapis.com/sdsc_demo25/TCI.parquet')
WHERE block != 0
LIMIT 1;

-- ============================================================================
-- Q3: Region Statistics - Stats over a geographic polygon
-- ============================================================================

SELECT '=== Q3: Region Statistics (Madrid area) ===' as benchmark;

-- Small region around Madrid
WITH madrid_region AS (
    SELECT 'POLYGON((-3.8 40.35, -3.6 40.35, -3.6 40.5, -3.8 40.5, -3.8 40.35))'::GEOMETRY as geom
),
intersecting_tiles AS (
    SELECT r.block, r.band_1, r.metadata
    FROM read_raquet('https://storage.googleapis.com/sdsc_demo25/TCI.parquet') r, madrid_region m
    WHERE quadbin_intersects(r.block, m.geom)
)
SELECT
    count(*) as tile_count,
    sum((ST_RasterSummaryStats(band_1, 'uint8', 256, 256, 'gzip')).count) as total_pixels,
    avg((ST_RasterSummaryStats(band_1, 'uint8', 256, 256, 'gzip')).mean) as avg_mean
FROM intersecting_tiles;

-- ============================================================================
-- Q4: Count tiles at each resolution
-- ============================================================================

SELECT '=== Q4: Resolution distribution ===' as benchmark;

SELECT
    quadbin_resolution(block) as resolution,
    count(*) as tile_count
FROM read_parquet('https://storage.googleapis.com/sdsc_demo25/TCI.parquet')
WHERE block != 0
GROUP BY quadbin_resolution(block)
ORDER BY resolution;

-- ============================================================================
-- Q5: Bounding box query - Find all tiles in an area
-- ============================================================================

SELECT '=== Q5: Bounding box query (Spain) ===' as benchmark;

WITH spain_bbox AS (
    SELECT 'POLYGON((-9.5 35.9, 3.3 35.9, 3.3 43.8, -9.5 43.8, -9.5 35.9))'::GEOMETRY as geom
)
SELECT count(*) as tiles_in_spain
FROM read_parquet('https://storage.googleapis.com/sdsc_demo25/TCI.parquet') r, spain_bbox s
WHERE block != 0
  AND quadbin_intersects(r.block, s.geom);

-- ============================================================================
-- Q6: Full table scan with aggregation
-- ============================================================================

SELECT '=== Q6: Full table aggregation ===' as benchmark;

SELECT
    count(*) as total_tiles,
    count(DISTINCT quadbin_resolution(block)) as resolution_levels,
    min(quadbin_resolution(block)) as min_resolution,
    max(quadbin_resolution(block)) as max_resolution
FROM read_parquet('https://storage.googleapis.com/sdsc_demo25/TCI.parquet')
WHERE block != 0;

SELECT '=== Benchmark Complete ===' as status;
