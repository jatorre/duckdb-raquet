-- BigQuery Raquet Benchmark Queries
-- Prerequisites:
--   1. Load TCI.parquet to BigQuery: bq load --source_format=PARQUET project:dataset.tci gs://sdsc_demo25/TCI.parquet
--   2. Deploy raquet UDFs from bigquery-raquet project
--
-- Run with: bq query --use_legacy_sql=false < benchmark/queries_bigquery.sql

-- ============================================================================
-- Configuration - Replace with your project/dataset
-- ============================================================================
-- DECLARE project_dataset STRING DEFAULT 'your-project.your-dataset';

-- ============================================================================
-- Q1: Point Query - Get pixel value at a specific coordinate
-- ============================================================================
-- Note: BigQuery requires explicit block lookup via QUADBIN_FROMGEOGPOINT

SELECT '=== Q1: Point Query (ST_RASTERVALUE) ===' as benchmark;

WITH metadata AS (
    SELECT metadata FROM `cartodb-on-gcp-backend-team.jatorre.tci` WHERE block = 0
),
point AS (
    SELECT -3.7 as lon, 40.4 as lat
)
SELECT
    `cartodb-on-gcp-backend-team.jatorre.ST_RASTERVALUE`(r.block, r.band_1, p.lon, p.lat, m.metadata, 0) as red,
    `cartodb-on-gcp-backend-team.jatorre.ST_RASTERVALUE`(r.block, r.band_2, p.lon, p.lat, m.metadata, 1) as green,
    `cartodb-on-gcp-backend-team.jatorre.ST_RASTERVALUE`(r.block, r.band_3, p.lon, p.lat, m.metadata, 2) as blue
FROM `cartodb-on-gcp-backend-team.jatorre.tci` r
CROSS JOIN metadata m
CROSS JOIN point p
WHERE r.block = `carto-un`.carto.QUADBIN_FROMGEOGPOINT(ST_GEOGPOINT(p.lon, p.lat), 14);

-- ============================================================================
-- Q2: Tile Statistics - Compute stats for a single tile
-- ============================================================================

SELECT '=== Q2: Tile Statistics (ST_RASTERSUMMARYSTATS) ===' as benchmark;

WITH metadata AS (
    SELECT metadata FROM `cartodb-on-gcp-backend-team.jatorre.tci` WHERE block = 0
)
SELECT
    r.block,
    `cartodb-on-gcp-backend-team.jatorre.ST_RASTERSUMMARYSTATS`(r.band_1, m.metadata, 0) as stats
FROM `cartodb-on-gcp-backend-team.jatorre.tci` r
CROSS JOIN metadata m
WHERE r.block != 0
LIMIT 1;

-- ============================================================================
-- Q3: Region Statistics - Stats over a geographic polygon
-- ============================================================================

SELECT '=== Q3: Region Statistics (Madrid area) ===' as benchmark;

WITH metadata AS (
    SELECT metadata FROM `cartodb-on-gcp-backend-team.jatorre.tci` WHERE block = 0
),
madrid_region AS (
    SELECT ST_GEOGFROMTEXT('POLYGON((-3.8 40.35, -3.6 40.35, -3.6 40.5, -3.8 40.5, -3.8 40.35))') as geom
),
params AS (
    SELECT
        m.metadata,
        CAST(JSON_VALUE(m.metadata, '$.tiling.pixel_zoom') AS INT64) AS pixel_zoom,
        r.geom
    FROM metadata m, madrid_region r
),
intersecting_blocks AS (
    SELECT block
    FROM params p,
    UNNEST(`carto-un`.carto.QUADBIN_POLYFILL_MODE(p.geom, p.pixel_zoom, 'intersects')) AS block
),
tile_stats AS (
    SELECT
        `cartodb-on-gcp-backend-team.jatorre.ST_RASTERSUMMARYSTATS`(r.band_1, p.metadata, 0) as stats
    FROM `cartodb-on-gcp-backend-team.jatorre.tci` r
    JOIN intersecting_blocks ib ON r.block = ib.block
    CROSS JOIN params p
)
SELECT
    COUNT(*) as tile_count,
    SUM((stats).count) as total_pixels,
    AVG((stats).mean) as avg_mean
FROM tile_stats;

-- ============================================================================
-- Q4: Count tiles at each resolution
-- ============================================================================

SELECT '=== Q4: Resolution distribution ===' as benchmark;

SELECT
    `carto-un`.carto.QUADBIN_RESOLUTION(block) as resolution,
    COUNT(*) as tile_count
FROM `cartodb-on-gcp-backend-team.jatorre.tci`
WHERE block != 0
GROUP BY `carto-un`.carto.QUADBIN_RESOLUTION(block)
ORDER BY resolution;

-- ============================================================================
-- Q5: Bounding box query - Find all tiles in an area
-- ============================================================================

SELECT '=== Q5: Bounding box query (Spain) ===' as benchmark;

WITH spain_bbox AS (
    SELECT ST_GEOGFROMTEXT('POLYGON((-9.5 35.9, 3.3 35.9, 3.3 43.8, -9.5 43.8, -9.5 35.9))') as geom
)
SELECT COUNT(*) as tiles_in_spain
FROM `cartodb-on-gcp-backend-team.jatorre.tci` r, spain_bbox s
WHERE block != 0
  AND ST_INTERSECTS(
      ST_BOUNDINGBOX(`carto-un`.carto.QUADBIN_BOUNDARY(r.block)),
      s.geom
  );

-- ============================================================================
-- Q6: Full table scan with aggregation
-- ============================================================================

SELECT '=== Q6: Full table aggregation ===' as benchmark;

SELECT
    COUNT(*) as total_tiles,
    COUNT(DISTINCT `carto-un`.carto.QUADBIN_RESOLUTION(block)) as resolution_levels,
    MIN(`carto-un`.carto.QUADBIN_RESOLUTION(block)) as min_resolution,
    MAX(`carto-un`.carto.QUADBIN_RESOLUTION(block)) as max_resolution
FROM `cartodb-on-gcp-backend-team.jatorre.tci`
WHERE block != 0;

SELECT '=== Benchmark Complete ===' as status;
