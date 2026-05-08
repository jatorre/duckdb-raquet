# CLAUDE.md

## Build & Test

```bash
# Build (requires CMake 3.12+, C++17 compiler, zlib)
make -j8

# Run tests
build/release/test/unittest --test-dir . "test/sql/*"

# Quick smoke test of read_raster (requires GDAL)
build/release/duckdb -c "SELECT count(*) FROM read_raster('path/to/file.tif', max_zoom=8, overviews='none');"
```

Test files use DuckDB `.test` format with `require raquet` + `require parquet`.

## Architecture

### Extension Entry Point
- `src/raquet_extension.cpp` — registers all functions and table macros
- Table macros: `read_raquet`, `read_raquet_at`, `read_raquet_metadata`, `ST_Raster`, `ST_RasterAt`
- Scalar functions registered via `Register*Functions()` calls from separate files

### Directory Structure
- `src/quadbin/` — QUADBIN spatial indexing functions (tile_to_cell, polyfill, etc.)
- `src/raster/` — Raster operations (pixel extraction, stats, clipping, band math, encoding/decoding)
- `src/metadata/` — Metadata parsing and validation
- `src/table_functions/` — Table function helpers (raquet_parse_metadata, raquet_pixel)
- `src/include/` — Headers (quadbin.hpp, band_decoder.hpp, band_encoder.hpp, raquet_metadata.hpp, read_raster.hpp)

### Key Files
- `src/include/quadbin.hpp` — All QUADBIN math (tile_to_cell, lonlat_to_tile, bbox, parent/children)
- `src/include/raquet_metadata.hpp` — `RaquetMetadata` struct, JSON parsing and serialization (`to_json()`)
- `src/include/band_decoder.hpp` — Pixel decoding, decompression (gzip/JPEG/WebP), `BandStats`
- `src/include/band_encoder.hpp` — Compression functions (inverse of decoder)
- `src/raster/read_raster.cpp` — GDAL-backed `read_raster()` table function (gated by `RAQUET_HAS_GDAL`)
- `src/metadata/raquet_metadata.cpp` — `raquet_validate_metadata()` function

### Raquet Format Convention
- Parquet files with QUADBIN spatial indexing
- `block=0` row holds metadata JSON in `metadata` column
- `block!=0` rows are data tiles with compressed band BLOBs
- Rows sorted by block (QUADBIN cell ID) for Parquet row group pruning

## Dependencies

Required: `zlib`
Optional (gated by compile flags):
- `libjpeg-turbo` → `RAQUET_HAS_JPEG` (JPEG compression)
- `libwebp` → `RAQUET_HAS_WEBP` (WebP compression)
- `GDAL` → `RAQUET_HAS_GDAL` (raster ingestion via `read_raster()`)

All managed via `vcpkg.json`. CMake uses `find_package(... QUIET)` with conditional linking.

## read_raster() Internals

The `read_raster()` table function converts raster files to raquet format:

1. **Bind**: Opens raster with GDAL, detects CRS/bands/resolution, calculates zoom, defines output schema
2. **InitGlobal**: Builds native-zoom tile list and overview frame list
3. **InitLocal**: Each thread opens its own `GDALDatasetH` handle (GDAL is not thread-safe per handle)
4. **Execute (Phase 1)**: Native-zoom tiles processed in parallel — threads pull from mutex-protected queue, warp from source base resolution, emit rows directly to the output DataChunk.
5. **Execute (Phase 2)**: Overview tiles processed in parallel via a work queue — one thread (the init winner) waits for Phase 1 stragglers and publishes `overview_frames`, then all workers pull from it. Uses COG fast path when source overviews exist (reads matching source overview level instead of re-warping from base). Results staged in `overview_results` so emission can span multiple Execute() calls.
6. **Execute (Phase 3)**: Drain staged overview rows into output DataChunks; finally emit the metadata row at `block=0` (exactly once, via CAS on `metadata_emitted`).

Set `RAQUET_DEBUG_TIMING=1` to emit per-phase wall-time markers to stderr (`[raquet-phase] ...`). See `src/raster/read_raster.cpp` near `ReadRasterGlobalState` for the detailed state-machine doc.

Pipeline per tile: `CreateTileDataset → WarpIntoTile → IsTileEmpty → ReadAndCompressBands → EmitTileRow`

## DuckDB API Notes

- `ExtensionUtil` was removed in DuckDB 1.5 — use `loader.RegisterFunction()` directly
- Extensions loaded with `RTLD_LOCAL` — no symbol conflicts between extensions (raquet and spatial can both bundle GDAL)
- Table function lifecycle: Bind → InitGlobal → InitLocal → Execute (repeated) → cleanup via destructors
- `LogicalType::GEOMETRY()` for native geometry support (DuckDB 1.5+)

## Gotchas

- Table macros can't distinguish types at parse time — 3-arg overloads (file, lon, lat) vs (file, point, resolution) cause ambiguity
- Subqueries can't be used in table function parameters — use `SET VARIABLE` + `getvariable()` as workaround
- `query_table('table_name')` used in macros for iceberg/table access (string param, not table ref)
- `block::UBIGINT` cast needed for iceberg tables (they store block as BIGINT)
- `Value::EMPTYLIST` doesn't exist in DuckDB 1.5 — use `Value::LIST(type, vector<Value>())`
