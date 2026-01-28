#!/bin/bash
# BigQuery Raquet Benchmark Runner
# Usage: ./benchmark/run_bigquery_benchmark.sh
#
# Prerequisites:
#   1. gcloud CLI authenticated
#   2. TCI.parquet loaded to BigQuery:
#      bq load --source_format=PARQUET project:dataset.tci gs://sdsc_demo25/TCI.parquet
#   3. Raquet UDFs deployed from bigquery-raquet project

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create results directory
mkdir -p "$RESULTS_DIR"

# Configuration
PROJECT="cartobq"
DATASET="raquet"
TABLE="tci"

# TCI.parquet covers: 30.9-36.6°E, 13.9-19.3°N (Sudan/Eritrea region)
TEST_LON=33.5
TEST_LAT=16.5

echo "=== BigQuery Raquet Benchmark ==="
echo "Timestamp: $TIMESTAMP"
echo "Project: $PROJECT"
echo "Dataset: $DATASET"
echo "Table: $TABLE"
echo "Test coordinates: ($TEST_LON, $TEST_LAT)"
echo ""

# Output file
OUTPUT_FILE="$RESULTS_DIR/bigquery_benchmark_${TIMESTAMP}.txt"

echo "Results will be saved to: $OUTPUT_FILE"
echo ""

# Run benchmark
{
    echo "=== BigQuery Raquet Benchmark ==="
    echo "Timestamp: $TIMESTAMP"
    echo "Project: $PROJECT.$DATASET.$TABLE"
    echo "Dataset: TCI.parquet (Sentinel-2 True Color, 261 MB)"
    echo "Coverage: 30.9-36.6°E, 13.9-19.3°N"
    echo "Test point: ($TEST_LON, $TEST_LAT)"
    echo ""

    echo "--- Checking Table ---"
    bq show "$PROJECT:$DATASET.$TABLE" 2>&1 | head -10 || echo "Table not found - load it first"
    echo ""

    echo "=== Q1: Point Query (ST_RASTERVALUE) ==="
    echo "Query: Get RGB values at ($TEST_LON, $TEST_LAT)"
    time bq query --use_legacy_sql=false "
    WITH metadata AS (
        SELECT metadata FROM \`$PROJECT.$DATASET.$TABLE\` WHERE block = 0
    ),
    point AS (
        SELECT $TEST_LON as lon, $TEST_LAT as lat
    )
    SELECT
        \`$PROJECT.$DATASET.ST_RASTERVALUE\`(r.block, r.band_1, p.lon, p.lat, m.metadata, 0) as red,
        \`$PROJECT.$DATASET.ST_RASTERVALUE\`(r.block, r.band_2, p.lon, p.lat, m.metadata, 1) as green,
        \`$PROJECT.$DATASET.ST_RASTERVALUE\`(r.block, r.band_3, p.lon, p.lat, m.metadata, 2) as blue
    FROM \`$PROJECT.$DATASET.$TABLE\` r
    CROSS JOIN metadata m
    CROSS JOIN point p
    WHERE r.block = \`carto-un\`.carto.QUADBIN_FROMGEOGPOINT(ST_GEOGPOINT(p.lon, p.lat), 14);
    "
    echo ""

    echo "=== Q2: Single Tile Statistics ==="
    echo "Query: Compute statistics for first tile"
    time bq query --use_legacy_sql=false "
    WITH metadata AS (
        SELECT metadata FROM \`$PROJECT.$DATASET.$TABLE\` WHERE block = 0
    )
    SELECT
        r.block,
        \`$PROJECT.$DATASET.ST_RASTERSUMMARYSTATS\`(r.band_1, m.metadata, 0) as stats
    FROM \`$PROJECT.$DATASET.$TABLE\` r
    CROSS JOIN metadata m
    WHERE r.block != 0
    LIMIT 1;
    "
    echo ""

    echo "=== Q3: Region Statistics (small area) ==="
    echo "Query: Aggregate stats over 0.5x0.5 degree polygon"
    time bq query --use_legacy_sql=false "
    WITH metadata AS (
        SELECT metadata FROM \`$PROJECT.$DATASET.$TABLE\` WHERE block = 0
    ),
    test_region AS (
        SELECT ST_GEOGFROMTEXT('POLYGON((33.0 16.0, 33.5 16.0, 33.5 16.5, 33.0 16.5, 33.0 16.0))') as geom
    ),
    params AS (
        SELECT
            m.metadata,
            CAST(JSON_VALUE(m.metadata, '\$.tiling.pixel_zoom') AS INT64) AS pixel_zoom,
            r.geom
        FROM metadata m, test_region r
    ),
    intersecting_blocks AS (
        SELECT block
        FROM params p,
        UNNEST(\`carto-un\`.carto.QUADBIN_POLYFILL_MODE(p.geom, p.pixel_zoom, 'intersects')) AS block
    ),
    tile_stats AS (
        SELECT
            \`$PROJECT.$DATASET.ST_RASTERSUMMARYSTATS\`(r.band_1, p.metadata, 0) as stats
        FROM \`$PROJECT.$DATASET.$TABLE\` r
        JOIN intersecting_blocks ib ON r.block = ib.block
        CROSS JOIN params p
    )
    SELECT
        COUNT(*) as tile_count,
        SUM((stats).count) as total_pixels,
        AVG((stats).mean) as avg_mean
    FROM tile_stats;
    "
    echo ""

    echo "=== Q4: Resolution Distribution ==="
    echo "Query: Count tiles at each zoom level"
    time bq query --use_legacy_sql=false "
    SELECT
        \`carto-un\`.carto.QUADBIN_RESOLUTION(block) as resolution,
        COUNT(*) as tile_count
    FROM \`$PROJECT.$DATASET.$TABLE\`
    WHERE block != 0
    GROUP BY \`carto-un\`.carto.QUADBIN_RESOLUTION(block)
    ORDER BY resolution;
    "
    echo ""

    echo "=== Q5: Bounding Box Query (full coverage) ==="
    echo "Query: Count tiles in coverage area"
    time bq query --use_legacy_sql=false "
    WITH data_bbox AS (
        SELECT ST_GEOGFROMTEXT('POLYGON((31 14, 36 14, 36 19, 31 19, 31 14))') as geom
    )
    SELECT COUNT(*) as tiles_in_bbox
    FROM \`$PROJECT.$DATASET.$TABLE\` r, data_bbox s
    WHERE block != 0
      AND ST_INTERSECTS(
          ST_BOUNDINGBOX(\`carto-un\`.carto.QUADBIN_BOUNDARY(r.block)),
          s.geom
      );
    "
    echo ""

    echo "=== Q6: Full Table Aggregation ==="
    echo "Query: Count all tiles and resolution stats"
    time bq query --use_legacy_sql=false "
    SELECT
        COUNT(*) as total_tiles,
        COUNT(DISTINCT \`carto-un\`.carto.QUADBIN_RESOLUTION(block)) as resolution_levels,
        MIN(\`carto-un\`.carto.QUADBIN_RESOLUTION(block)) as min_resolution,
        MAX(\`carto-un\`.carto.QUADBIN_RESOLUTION(block)) as max_resolution
    FROM \`$PROJECT.$DATASET.$TABLE\`
    WHERE block != 0;
    "
    echo ""

    echo "=== Q7: Multi-tile Statistics (10 tiles) ==="
    echo "Query: Compute statistics for 10 tiles"
    time bq query --use_legacy_sql=false "
    WITH metadata AS (
        SELECT metadata FROM \`$PROJECT.$DATASET.$TABLE\` WHERE block = 0
    )
    SELECT
        r.block,
        (stats).mean as mean,
        (stats).stddev as stddev
    FROM \`$PROJECT.$DATASET.$TABLE\` r
    CROSS JOIN metadata m,
    UNNEST([\`$PROJECT.$DATASET.ST_RASTERSUMMARYSTATS\`(r.band_1, m.metadata, 0)]) as stats
    WHERE r.block != 0
    LIMIT 10;
    "
    echo ""

    echo "=== Benchmark Complete ==="

} 2>&1 | tee "$OUTPUT_FILE"

echo ""
echo "Results saved to: $OUTPUT_FILE"
