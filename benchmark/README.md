# DuckDB Raquet vs BigQuery Raquet Performance Benchmark

Performance comparison between DuckDB raquet extension (C++) and BigQuery raquet UDFs (JavaScript).

## Test Configuration

- **Dataset**: TCI.parquet (Sentinel-2 True Color, 3 bands, uint8)
- **Size**: 261 MB, 3,225 tiles
- **Resolution range**: z7 to z14
- **Coverage**: 30.9-36.6°E, 13.9-19.3°N (Sudan/Eritrea region)

### Systems

| System | Configuration |
|--------|---------------|
| **DuckDB** | Apple M3 Max, macOS 15.2, DuckDB v1.5.0-dev6345, local C++ extension |
| **BigQuery** | Cloud, JavaScript UDFs, `cartobq.raquet` dataset |

## Results Summary

### Three-Way Comparison: DuckDB vs BigQuery Native vs BigQuery GCS

| Query | Description | DuckDB (HTTPS) | BQ Native | BQ GCS External |
|-------|-------------|----------------|-----------|-----------------|
| **Q1** | Point Query | **4.0s** | 3.2s | 4.7s |
| **Q2** | Single Tile Statistics | **1.3s** | 2.4s | 2.5s |
| **Q3** | Region Stats (545 tiles) | **2.7s** | 6.0s | ~7s* |
| **Q4** | Resolution Distribution | **0.8s** | 2.0s | 2.4s |
| **Q6** | Full Table Aggregation | **0.7s** | 2.0s | 2.6s |

\* Estimated based on ~20% GCS overhead

### Key Observations

1. **DuckDB direct HTTPS** is fastest overall (no data loading required)
2. **BigQuery GCS external tables** add only ~20-30% overhead vs native tables
3. **BigQuery native tables** are ~2-3x slower than DuckDB for most queries
4. **All three** can query the same parquet files in GCS

**Note**: BigQuery region queries require `__RAQUET_REGION_BLOCKS(geom, min_zoom, max_zoom)` to find tiles across pyramid levels. Standard `QUADBIN_POLYFILL_MODE` only works at single zoom.

## Key Findings

### DuckDB Advantages

1. **Lower latency**: 2-4x faster for most queries
2. **Native C++ implementation**: No JavaScript UDF overhead
3. **Direct GCS access**: Works without loading data into tables first
4. **No cold start**: Extension loads in <1s
5. **Simpler spatial predicates**: `quadbin_intersects()` works across all resolutions

### BigQuery Observations

1. **Cold start overhead**: ~2-5s initial query latency
2. **JavaScript UDF performance**: ~2x slower than native C++ for raster stats
3. **Multi-resolution pyramids require `__RAQUET_REGION_BLOCKS`**: Standard `QUADBIN_POLYFILL_MODE` only works at single zoom level
4. **Competitive after optimization**: 6s vs 2.7s for region queries (2.2x gap)

### When to Use Each

| Use Case | Recommendation |
|----------|----------------|
| Interactive analysis | **DuckDB** - immediate response |
| Local file exploration | **DuckDB** - no data loading needed |
| Small to medium datasets (<10GB) | **DuckDB** - simpler, faster |
| Large-scale batch processing (TB+) | **BigQuery** - distributed compute |
| Team data sharing | **BigQuery** - centralized access |
| Integration with GCP ecosystem | **BigQuery** - native integration |

## Architecture Comparison

| Aspect | DuckDB Raquet | BigQuery Raquet |
|--------|--------------|-----------------|
| **Implementation** | C++ Extension | JavaScript UDFs |
| **QUADBIN Functions** | Native C++ (50+ functions) | CARTO Analytics Toolbox |
| **Raster Processing** | Native C++ with zlib | JavaScript with pako |
| **Data Access** | Local, HTTPS, S3, GCS | BigQuery tables, external tables |
| **Parallelism** | Single machine, multi-thread | Distributed, auto-scaling |
| **Cold Start** | <1s | ~2-5s |

## Detailed Results

### DuckDB (GCS via HTTPS)

```
Q1: Point Query          4.0s   (RGB values at 33.5°E, 16.5°N)
Q2: Single Tile Stats    1.3s   (65,536 pixels)
Q3: Region Stats         2.7s   (545 tiles, 35M pixels)
Q4: Resolution Dist      0.8s   (8 resolution levels)
Q5: Bounding Box         0.8s   (3,225 tiles)
Q6: Full Aggregation     0.7s   (3,225 tiles)
Q7: Multi-tile Stats     1.5s   (10 tiles)
```

### BigQuery (Native Table)

```
Q1: Point Query          9.3s   (single result row)
Q2: Single Tile Stats    2.5s   (65,536 pixels)
Q3: Region Stats         6.0s   (545 tiles - using __RAQUET_REGION_BLOCKS)
Q4: Resolution Dist      3.2s   (8 resolution levels)
```

**Fix applied**: Region queries now use `__RAQUET_REGION_BLOCKS(geom, min_zoom, max_zoom)`
which polyfills across all pyramid levels instead of single-zoom `QUADBIN_POLYFILL_MODE`.

## Running the Benchmarks

### DuckDB

```bash
# Build extension
GEN=ninja make release

# Run benchmark
./benchmark/run_duckdb_benchmark.sh
```

### BigQuery

```bash
# Ensure UDFs are deployed
bq ls --routines cartobq:raquet

# Run benchmark
./benchmark/run_bigquery_benchmark.sh
```

## Query Syntax Comparison

### Point Query

**DuckDB** (cleaner, automatic filtering):
```sql
SELECT ST_RasterValue(block, band_1, 'POINT(33.5 16.5)'::GEOMETRY, metadata)
FROM read_raquet('file.parquet')
WHERE quadbin_contains(block, 'POINT(33.5 16.5)'::GEOMETRY);
```

**BigQuery** (requires explicit block lookup):
```sql
WITH metadata AS (SELECT metadata FROM table WHERE block = 0)
SELECT ST_RASTERVALUE(r.block, r.band_1, 33.5, 16.5, m.metadata, 0)
FROM table r, metadata m
WHERE r.block = QUADBIN_FROMGEOGPOINT(ST_GEOGPOINT(33.5, 16.5), 14);
```

### Key API Differences

1. **Geometry handling**: DuckDB uses `::GEOMETRY` cast, BigQuery uses lon/lat
2. **Spatial filtering**: DuckDB has `quadbin_contains()`, BigQuery needs `QUADBIN_FROMGEOGPOINT()`
3. **Metadata**: DuckDB propagates automatically via `read_raquet()`, BigQuery needs explicit CTE
4. **Band index**: DuckDB infers from metadata, BigQuery requires explicit index

## Files

```
benchmark/
├── README.md                    # This documentation
├── queries_duckdb.sql           # DuckDB SQL queries
├── queries_bigquery.sql         # BigQuery SQL queries
├── run_duckdb_benchmark.sh      # DuckDB runner
├── run_bigquery_benchmark.sh    # BigQuery runner
└── results/
    ├── duckdb_benchmark_*.txt
    └── bigquery_benchmark_*.txt
```
