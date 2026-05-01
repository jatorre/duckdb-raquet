#pragma once

#ifdef RAQUET_HAS_GDAL

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include <gdal.h>

namespace duckdb {
namespace raquet {

// Per-band statistics shared by both v0.1.0 and v0.5.0 metadata output.
//
// The seven numeric fields (count, min, max, mean, stddev, sum,
// sum_squares) come from `GDALComputeRasterStatistics` with `approxOK`
// toggled by the caller's `approx` flag, plus arithmetic identities
// (count = STATISTICS_VALID_PERCENT × width × height, sum = mean×count,
// sum_squares = count × (stddev² + mean²)). The two extension fields
// (quantiles, top_values) are raster-loader-shape extensions used by
// CARTO Builder colormap UIs; they're emitted in both v0.1.0 and
// v0.5.0 output even though the raquet spec defines neither.
//
// In `approx` mode, quantiles/top_values come from a 1000-pixel
// random sample. In exact mode, they're derived from a streaming
// histogram of the full band — exact for fixed-bucket integer dtypes
// (uint8/int8/uint16/int16), and exact-up-to-bin-width for wider
// integer / float dtypes.
struct V01BandStatsResult {
    int64_t count = 0;
    double  min = 0.0;
    double  max = 0.0;
    double  mean = 0.0;
    double  stddev = 0.0;
    double  sum = 0.0;
    double  sum_squares = 0.0;
    double  valid_percent = 0.0;
    bool    approximated = true;
    bool    has_stats = false;
    std::string version;                              // producer tag
    std::map<int, std::vector<double>> quantiles;     // keys 3..19; empty if uncomputable
    std::map<double, int64_t> top_values;             // up to 10 entries; empty if uncomputable
};

// Compute per-band stats. When `approx == true` (default), this is
// fast: GDAL overview-based stats for the seven numeric fields, plus a
// 1000-pixel random sample for quantiles/top_values. When `approx ==
// false`, GDAL does a full-resolution scan AND we run a streaming
// pass over the band to build a histogram for exact quantiles /
// top_values.
V01BandStatsResult compute_v01_band_stats(
    GDALRasterBandH band,
    int raster_width, int raster_height,
    double nodata, bool has_nodata,
    GDALDataType dtype,
    bool approx);

}  // namespace raquet
}  // namespace duckdb

#endif  // RAQUET_HAS_GDAL
