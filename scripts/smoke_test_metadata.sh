#!/usr/bin/env bash
# Smoke-test the metadata enrichment changes by loading the freshly built
# raquet extension and running the new metadata queries against the synthetic
# test fixtures. Exits non-zero on the first SQL that returns the wrong shape.
#
# Run inside the build container (so libgdal.so.36 matches):
#   docker run --rm -v <repo>:/workspace raquet-build:gdal-3.10.3 \
#     bash /workspace/scripts/smoke_test_metadata.sh
set -euo pipefail

cd /workspace
EXT=build/release/extension/raquet/raquet.duckdb_extension

# GDAL writes a per-raster .aux.xml stats sidecar by default after
# GDALComputeRasterStatistics. We don't want that polluting test/data/, since
# the stats are recomputed every run anyway.
export GDAL_PAM_ENABLED=NO

if [ ! -f "$EXT" ]; then
  echo "ERROR: extension not built at $EXT" >&2
  exit 1
fi

run() {
  local label="$1"; shift
  local sql="$1"; shift
  local expected="$1"; shift
  local actual
  actual="$(duckdb -unsigned -noheader -list -c "
    LOAD '$EXT';
    $sql
  " 2>&1)"
  if [ "$actual" = "$expected" ]; then
    echo "PASS  $label"
  else
    echo "FAIL  $label"
    echo "  sql:      $sql"
    echo "  expected: $expected"
    echo "  actual:   $actual"
    exit 1
  fi
}

# =============================================================================
# Group 1: parser round-trip of v0.1.0 metadata shape
# =============================================================================

V01_META='{"version":"0.1.0","compression":"gzip","block_width":512,"block_height":512,"minresolution":4,"maxresolution":13,"pixel_resolution":22,"num_blocks":53423,"bounds":[5.86,47.27,15.04,55.05],"center":[10.45,51.16,4],"width":64293,"height":86760,"num_pixels":5578063080,"block_resolution":13,"nodata":[0,null],"bands":[{"name":"band_1","type":"uint8","colorinterp":"palette","colortable":{"0":[0,0,0,0],"1":[255,0,0,255]},"stats":{"count":4123456,"min":0,"max":12,"mean":4.7,"stddev":2.3,"sum":19380243.2,"sum_squares":113194824.6,"approximated_stats":true}}]}'

run "v0.1.0 version field" \
  "SELECT json_extract_string('$V01_META', '\$.version');" \
  "0.1.0"

run "v0.1.0 width/height" \
  "SELECT json_extract('$V01_META', '\$.width')::INTEGER || '|' || json_extract('$V01_META', '\$.height')::INTEGER;" \
  "64293|86760"

run "v0.1.0 colortable[1] is 4-element RGBA" \
  "SELECT json_array_length(json_extract('$V01_META', '\$.bands[0].colortable.\"1\"'));" \
  "4"

run "v0.1.0 stats.approximated_stats=true" \
  "SELECT json_extract('$V01_META', '\$.bands[0].stats.approximated_stats')::BOOLEAN;" \
  "true"

run "v0.1.0 raquet_parse_metadata round-trip" \
  "SELECT (raquet_parse_metadata('$V01_META')).compression || '|' || (raquet_parse_metadata('$V01_META')).max_zoom || '|' || (raquet_parse_metadata('$V01_META')).num_bands;" \
  "gzip|13|1"

# =============================================================================
# Group 2: parser round-trip of v0.5.0 metadata shape with new fields
# =============================================================================

V05_META='{"file_format":"raquet","version":"0.5.0","crs":"EPSG:3857","bounds":[5.86,47.27,15.04,55.05],"bounds_crs":"EPSG:4326","width":64293,"height":86760,"compression":"gzip","tiling":{"scheme":"quadbin","block_width":256,"block_height":256,"min_zoom":4,"max_zoom":13,"pixel_zoom":21,"num_blocks":53423},"bands":[{"name":"band_1","type":"uint8","nodata":0,"colorinterp":"palette","colortable":{"0":[0,0,0,0],"1":[12,68,28,255]},"STATISTICS_MINIMUM":0,"STATISTICS_MAXIMUM":12,"STATISTICS_MEAN":4.7,"STATISTICS_STDDEV":2.3,"STATISTICS_VALID_PERCENT":98.4}]}'

run "v0.5.0 width/height (top-level)" \
  "SELECT json_extract('$V05_META', '\$.width')::INTEGER || '|' || json_extract('$V05_META', '\$.height')::INTEGER;" \
  "64293|86760"

run "v0.5.0 STATISTICS_VALID_PERCENT in [0,100]" \
  "SELECT json_extract('$V05_META', '\$.bands[0].STATISTICS_VALID_PERCENT')::DOUBLE BETWEEN 0 AND 100;" \
  "true"

run "v0.5.0 raquet_parse_metadata round-trip" \
  "SELECT (raquet_parse_metadata('$V05_META')).compression || '|' || (raquet_parse_metadata('$V05_META')).max_zoom;" \
  "gzip|13"

# =============================================================================
# Group 3: end-to-end via read_raster on the palette TIFF fixture
# =============================================================================

if [ ! -f test/data/test_palette.tif ]; then
  echo "ERROR: test fixture missing — run scripts/make_test_palette_tif.py" >&2
  exit 1
fi

run "read_raster v0.5.0: top-level width=16" \
  "SELECT json_extract(metadata, '\$.width')::INTEGER FROM read_raster('test/data/test_palette.tif') WHERE block = 0;" \
  "16"

run "read_raster v0.5.0: bands[0].colorinterp=palette" \
  "SELECT json_extract_string(metadata, '\$.bands[0].colorinterp') FROM read_raster('test/data/test_palette.tif') WHERE block = 0;" \
  "palette"

run "read_raster v0.5.0: bands[0].colortable.\"1\" is RGBA(12,68,28,255)" \
  "SELECT json_extract(metadata, '\$.bands[0].colortable.\"1\"[0]')::INTEGER || ',' || json_extract(metadata, '\$.bands[0].colortable.\"1\"[1]')::INTEGER || ',' || json_extract(metadata, '\$.bands[0].colortable.\"1\"[2]')::INTEGER || ',' || json_extract(metadata, '\$.bands[0].colortable.\"1\"[3]')::INTEGER FROM read_raster('test/data/test_palette.tif') WHERE block = 0;" \
  "12,68,28,255"

run "read_raster v0.5.0: STATISTICS_VALID_PERCENT > 0" \
  "SELECT json_extract(metadata, '\$.bands[0].STATISTICS_VALID_PERCENT')::DOUBLE > 0 FROM read_raster('test/data/test_palette.tif') WHERE block = 0;" \
  "true"

run "read_raster format='v0': version=0.1.0" \
  "SELECT json_extract_string(metadata, '\$.version') FROM read_raster('test/data/test_palette.tif', format='v0') WHERE block = 0;" \
  "0.1.0"

run "read_raster format='v0': num_pixels=256" \
  "SELECT json_extract(metadata, '\$.num_pixels')::INTEGER FROM read_raster('test/data/test_palette.tif', format='v0') WHERE block = 0;" \
  "256"

run "read_raster format='v0': nodata is scalar" \
  "SELECT json_extract(metadata, '\$.nodata')::DOUBLE FROM read_raster('test/data/test_palette.tif', format='v0') WHERE block = 0;" \
  "0.0"

run "read_raster format='v0': stats.approximated_stats=true (default)" \
  "SELECT json_extract(metadata, '\$.bands[0].stats.approximated_stats')::BOOLEAN FROM read_raster('test/data/test_palette.tif', format='v0') WHERE block = 0;" \
  "true"

run "read_raster format='v0', approx=false: approximated_stats=false" \
  "SELECT json_extract(metadata, '\$.bands[0].stats.approximated_stats')::BOOLEAN FROM read_raster('test/data/test_palette.tif', format='v0', approx=false) WHERE block = 0;" \
  "false"

echo
echo "all metadata enrichment smoke tests passed"
