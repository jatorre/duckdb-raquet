#!/bin/bash
# DuckDB Raquet Benchmark Runner
# Usage: ./benchmark/run_duckdb_benchmark.sh [local|gcs]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="$SCRIPT_DIR/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create results directory
mkdir -p "$RESULTS_DIR"

# DuckDB binary (use release build if available)
if [ -f "$PROJECT_DIR/build/release/duckdb" ]; then
    DUCKDB="$PROJECT_DIR/build/release/duckdb"
elif command -v duckdb &> /dev/null; then
    DUCKDB="duckdb"
else
    echo "Error: DuckDB not found. Build the project first or install duckdb."
    exit 1
fi

echo "=== DuckDB Raquet Benchmark ==="
echo "Timestamp: $TIMESTAMP"
echo "DuckDB: $DUCKDB"
echo ""

# Test data URLs
TCI_URL="https://storage.googleapis.com/sdsc_demo25/TCI.parquet"

# TCI.parquet covers: 30.9-36.6°E, 13.9-19.3°N (Sudan/Eritrea region)
# Use coordinates within this area
TEST_LON=33.5
TEST_LAT=16.5

# Output file
OUTPUT_FILE="$RESULTS_DIR/duckdb_benchmark_${TIMESTAMP}.txt"

echo "Results will be saved to: $OUTPUT_FILE"
echo "Test coordinates: ($TEST_LON, $TEST_LAT)"
echo ""

# Run benchmark
{
    echo "=== DuckDB Raquet Benchmark ==="
    echo "Timestamp: $TIMESTAMP"
    echo "Data source: GCS (HTTPS)"
    echo "Dataset: TCI.parquet (Sentinel-2 True Color, 261 MB)"
    echo "Coverage: 30.9-36.6°E, 13.9-19.3°N"
    echo "Test point: ($TEST_LON, $TEST_LAT)"
    echo ""

    echo "--- System Info ---"
    uname -a
    $DUCKDB -c "SELECT version();"
    echo ""

    echo "--- Loading Extension ---"
    $DUCKDB -c "LOAD raquet; SELECT 'Extension loaded successfully';"
    echo ""

    echo "=== Q1: Point Query (ST_RasterValue) ==="
    echo "Query: Get RGB values at ($TEST_LON, $TEST_LAT)"
    time $DUCKDB -c "
    LOAD raquet;
    SELECT
        ST_RasterValue(block, band_1, 'POINT($TEST_LON $TEST_LAT)'::GEOMETRY, metadata) as red,
        ST_RasterValue(block, band_2, 'POINT($TEST_LON $TEST_LAT)'::GEOMETRY, metadata) as green,
        ST_RasterValue(block, band_3, 'POINT($TEST_LON $TEST_LAT)'::GEOMETRY, metadata) as blue
    FROM read_raquet('$TCI_URL')
    WHERE quadbin_contains(block, 'POINT($TEST_LON $TEST_LAT)'::GEOMETRY);
    "
    echo ""

    echo "=== Q2: Single Tile Statistics ==="
    echo "Query: Compute statistics for first tile"
    time $DUCKDB -c "
    LOAD raquet;
    SELECT
        block,
        ST_RasterSummaryStats(band_1, 'uint8', 256, 256, 'gzip') as stats
    FROM read_parquet('$TCI_URL')
    WHERE block <> 0
    LIMIT 1;
    "
    echo ""

    echo "=== Q3: Region Statistics (small area) ==="
    echo "Query: Aggregate stats over 0.5x0.5 degree polygon"
    time $DUCKDB -c "
    LOAD raquet;
    WITH test_region AS (
        SELECT 'POLYGON((33.0 16.0, 33.5 16.0, 33.5 16.5, 33.0 16.5, 33.0 16.0))'::GEOMETRY as geom
    ),
    intersecting_tiles AS (
        SELECT r.block, r.band_1, r.metadata
        FROM read_raquet('$TCI_URL') r, test_region m
        WHERE quadbin_intersects(r.block, m.geom)
    )
    SELECT
        count(*) as tile_count,
        sum((ST_RasterSummaryStats(band_1, 'uint8', 256, 256, 'gzip')).count) as total_pixels,
        avg((ST_RasterSummaryStats(band_1, 'uint8', 256, 256, 'gzip')).mean) as avg_mean
    FROM intersecting_tiles;
    "
    echo ""

    echo "=== Q4: Resolution Distribution ==="
    echo "Query: Count tiles at each zoom level"
    time $DUCKDB -c "
    LOAD raquet;
    SELECT
        quadbin_resolution(block) as resolution,
        count(*) as tile_count
    FROM read_parquet('$TCI_URL')
    WHERE block <> 0
    GROUP BY quadbin_resolution(block)
    ORDER BY resolution;
    "
    echo ""

    echo "=== Q5: Bounding Box Query (full coverage) ==="
    echo "Query: Count tiles in coverage area"
    time $DUCKDB -c "
    LOAD raquet;
    WITH data_bbox AS (
        SELECT 'POLYGON((31 14, 36 14, 36 19, 31 19, 31 14))'::GEOMETRY as geom
    )
    SELECT count(*) as tiles_in_bbox
    FROM read_parquet('$TCI_URL') r, data_bbox s
    WHERE block <> 0
      AND quadbin_intersects(r.block, s.geom);
    "
    echo ""

    echo "=== Q6: Full Table Aggregation ==="
    echo "Query: Count all tiles and resolution stats"
    time $DUCKDB -c "
    LOAD raquet;
    SELECT
        count(*) as total_tiles,
        count(DISTINCT quadbin_resolution(block)) as resolution_levels,
        min(quadbin_resolution(block)) as min_resolution,
        max(quadbin_resolution(block)) as max_resolution
    FROM read_parquet('$TCI_URL')
    WHERE block <> 0;
    "
    echo ""

    echo "=== Q7: Multi-tile Statistics (10 tiles) ==="
    echo "Query: Compute statistics for 10 tiles"
    time $DUCKDB -c "
    LOAD raquet;
    SELECT
        block,
        (ST_RasterSummaryStats(band_1, 'uint8', 256, 256, 'gzip')).mean as mean,
        (ST_RasterSummaryStats(band_1, 'uint8', 256, 256, 'gzip')).stddev as stddev
    FROM read_parquet('$TCI_URL')
    WHERE block <> 0
    LIMIT 10;
    "
    echo ""

    echo "=== Benchmark Complete ==="

} 2>&1 | tee "$OUTPUT_FILE"

echo ""
echo "Results saved to: $OUTPUT_FILE"
