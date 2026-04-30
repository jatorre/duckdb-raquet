#pragma once

#ifdef RAQUET_HAS_GDAL

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include <gdal.h>

namespace duckdb {
namespace raquet {

// Sample-based per-band stats matching the v0.1.0 metadata shape
// emitted by Python raster-loader (raster_loader/io/common.py).
//
// Reads up to 1000 pixels at random positions within the band's pixel
// extent; excludes nodata, NaN, and infinite values; computes
// accumulators, quantiles for N in 3..19, and top-10 most-common
// values. The producer tag is hardcoded to a "duckdb-raquet-..." string
// so downstream consumers can distinguish this output from
// raster-loader's.
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
    std::map<int, std::vector<double>> quantiles;     // keys 3..19
    std::map<double, int64_t> top_values;             // up to 10 entries
};

V01BandStatsResult compute_v01_band_stats(
    GDALRasterBandH band,
    int raster_width, int raster_height,
    double nodata, bool has_nodata,
    GDALDataType dtype);

}  // namespace raquet
}  // namespace duckdb

#endif  // RAQUET_HAS_GDAL
