#!/usr/bin/env bash
# Verify the metadata enrichment changes against a real production-shaped
# raster — ~/Downloads/raster/classification_germany_berlin.tif (UTM zone 32N,
# 4092×2972 single-band uint8 with a 256-entry palette and nodata=255).
#
# Run inside the build container with both the worktree and the raster mounted:
#   docker run --rm \
#     -v <worktree>:/workspace \
#     -v ~/Downloads/raster:/data:ro \
#     raquet-build:gdal-3.10.3 \
#     bash /workspace/scripts/verify_berlin_metadata.sh
set -euo pipefail

cd /workspace
EXT=build/release/extension/raquet/raquet.duckdb_extension
RASTER=/data/classification_germany_berlin.tif

if [ ! -f "$EXT" ];    then echo "extension missing: $EXT" >&2; exit 1; fi
if [ ! -f "$RASTER" ]; then echo "raster missing:    $RASTER" >&2; exit 1; fi

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
    echo "  expected: '$expected'"
    echo "  actual:   '$actual'"
    exit 1
  fi
}

show() {
  local label="$1"; shift
  local sql="$1"; shift
  local actual
  actual="$(duckdb -unsigned -noheader -list -c "
    LOAD '$EXT';
    $sql
  " 2>&1)"
  echo "INFO  $label: $actual"
}

echo "=== format='v0' (legacy v0.1.0 shape consumed by CARTO platform) ==="

V0="SELECT metadata FROM read_raster('$RASTER', format='v0', block_size=512) WHERE block = 0"

run "version='0.1.0'" \
  "SELECT json_extract_string(metadata, '\$.version') FROM ($V0) m;" \
  "0.1.0"

run "width = source XSize (4092)" \
  "SELECT json_extract(metadata, '\$.width')::INTEGER FROM ($V0) m;" \
  "4092"

run "height = source YSize (2972)" \
  "SELECT json_extract(metadata, '\$.height')::INTEGER FROM ($V0) m;" \
  "2972"

run "num_pixels = width*height" \
  "SELECT json_extract(metadata, '\$.num_pixels')::BIGINT FROM ($V0) m;" \
  "12161424"

run "bounds is 4-element array" \
  "SELECT json_array_length(json_extract(metadata, '\$.bounds')) FROM ($V0) m;" \
  "4"

run "center is 3-element array" \
  "SELECT json_array_length(json_extract(metadata, '\$.center')) FROM ($V0) m;" \
  "3"

run "single-band: nodata is scalar = 255" \
  "SELECT json_extract(metadata, '\$.nodata')::DOUBLE FROM ($V0) m;" \
  "255.0"

run "block_resolution = max_zoom" \
  "SELECT json_extract(metadata, '\$.block_resolution')::INTEGER = json_extract(metadata, '\$.maxresolution')::INTEGER FROM ($V0) m;" \
  "true"

run "block_width = 512 (override) " \
  "SELECT json_extract(metadata, '\$.block_width')::INTEGER FROM ($V0) m;" \
  "512"

run "bands[0].colorinterp = palette" \
  "SELECT json_extract_string(metadata, '\$.bands[0].colorinterp') FROM ($V0) m;" \
  "palette"

run "colortable has 256 entries" \
  "SELECT json_array_length(json_keys(json_extract(metadata, '\$.bands[0].colortable'))) FROM ($V0) m;" \
  "256"

run "colortable.\"0\" is 4-element RGBA" \
  "SELECT json_array_length(json_extract(metadata, '\$.bands[0].colortable.\"0\"')) FROM ($V0) m;" \
  "4"

run "stats.approximated_stats = true (default)" \
  "SELECT json_extract(metadata, '\$.bands[0].stats.approximated_stats')::BOOLEAN FROM ($V0) m;" \
  "true"

run "stats.count > 0" \
  "SELECT json_extract(metadata, '\$.bands[0].stats.count')::BIGINT > 0 FROM ($V0) m;" \
  "true"

run "stats.count <= width*height" \
  "SELECT json_extract(metadata, '\$.bands[0].stats.count')::BIGINT <= (json_extract(metadata, '\$.width')::BIGINT * json_extract(metadata, '\$.height')::BIGINT) FROM ($V0) m;" \
  "true"

run "stats.min/max within uint8 range" \
  "SELECT json_extract(metadata, '\$.bands[0].stats.min')::DOUBLE >= 0 AND json_extract(metadata, '\$.bands[0].stats.max')::DOUBLE <= 255 FROM ($V0) m;" \
  "true"

show "stats" \
  "SELECT json_extract(metadata, '\$.bands[0].stats') FROM ($V0) m;"

echo
echo "=== format='v0.5.0' (default — spec-compliant shape) ==="

V05="SELECT metadata FROM read_raster('$RASTER', block_size=512) WHERE block = 0"

run "v0.5.0 top-level width" \
  "SELECT json_extract(metadata, '\$.width')::INTEGER FROM ($V05) m;" \
  "4092"

run "v0.5.0 top-level height" \
  "SELECT json_extract(metadata, '\$.height')::INTEGER FROM ($V05) m;" \
  "2972"

run "v0.5.0 colortable has 256 entries" \
  "SELECT json_array_length(json_keys(json_extract(metadata, '\$.bands[0].colortable'))) FROM ($V05) m;" \
  "256"

run "v0.5.0 STATISTICS_VALID_PERCENT in [0, 100]" \
  "SELECT json_extract(metadata, '\$.bands[0].STATISTICS_VALID_PERCENT')::DOUBLE BETWEEN 0 AND 100 FROM ($V05) m;" \
  "true"

run "v0.5.0 STATISTICS_MEAN within uint8 range" \
  "SELECT json_extract(metadata, '\$.bands[0].STATISTICS_MEAN')::DOUBLE BETWEEN 0 AND 255 FROM ($V05) m;" \
  "true"

show "v0.5.0 STATISTICS_*" \
  "SELECT json_extract(metadata, '\$.bands[0].STATISTICS_MINIMUM') || '/' || json_extract(metadata, '\$.bands[0].STATISTICS_MAXIMUM') || '/' || json_extract(metadata, '\$.bands[0].STATISTICS_MEAN') || '/' || json_extract(metadata, '\$.bands[0].STATISTICS_STDDEV') || '/' || json_extract(metadata, '\$.bands[0].STATISTICS_VALID_PERCENT') FROM ($V05) m;"

echo
echo "=== approx=false flips approximated_stats ==="

V0E="SELECT metadata FROM read_raster('$RASTER', format='v0', block_size=512, approx=false) WHERE block = 0"

run "approx=false → approximated_stats=false" \
  "SELECT json_extract(metadata, '\$.bands[0].stats.approximated_stats')::BOOLEAN FROM ($V0E) m;" \
  "false"

show "exact stats (approx=false)" \
  "SELECT json_extract(metadata, '\$.bands[0].stats') FROM ($V0E) m;"

echo
echo "=== Berlin metadata enrichment verification: all checks passed ==="
