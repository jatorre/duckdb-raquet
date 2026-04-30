#ifdef RAQUET_HAS_GDAL

#include "band_stats_v01.hpp"

#include <gdal.h>
#include <cpl_error.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <random>
#include <unordered_map>
#include <utility>
#include <vector>

namespace duckdb {
namespace raquet {

// Constants chosen to match raster_loader/io/common.py:
//   DEFAULT_SAMPLING_MAX_SAMPLES    = 1000
//   DEFAULT_SAMPLING_MAX_ITERATIONS = 10
//   DEFAULT_OVERVIEWS               = range(3, 20)   -> [3..19]
//   DEFAULT_MAX_MOST_COMMON         = 10
static constexpr int  kMaxSamples    = 1000;
static constexpr int  kMaxIterations = 10;
static constexpr int  kOverviewMin   = 3;
static constexpr int  kOverviewMax   = 19;  // inclusive
static constexpr int  kMaxMostCommon = 10;
static constexpr const char *kVersionTag = "duckdb-raquet-0.1.0";

static inline bool is_invalid(double v, double nodata, bool has_nodata) {
    if (std::isnan(v) || std::isinf(v)) return true;
    if (has_nodata) {
        if (std::isnan(nodata)) return std::isnan(v);
        return v == nodata;
    }
    return false;
}

V01BandStatsResult compute_v01_band_stats(
    GDALRasterBandH band,
    int raster_width, int raster_height,
    double nodata, bool has_nodata,
    GDALDataType dtype) {

    V01BandStatsResult r;
    r.version = kVersionTag;
    if (!band || raster_width <= 0 || raster_height <= 0) {
        return r;
    }

    // Deterministic seed so consecutive runs over the same source produce
    // identical stats. Distinct from std::random_device on purpose.
    std::mt19937_64 rng(0x6172717565742d76ULL);  // "raquet-v" in ASCII
    std::uniform_int_distribution<int> rx_dist(0, raster_width - 1);
    std::uniform_int_distribution<int> ry_dist(0, raster_height - 1);

    std::vector<double> samples;
    samples.reserve(kMaxSamples);

    int64_t attempted = 0;
    for (int iter = 0; iter < kMaxIterations; iter++) {
        const int needed = kMaxSamples - static_cast<int>(samples.size());
        if (needed <= 0) break;

        for (int i = 0; i < needed; i++) {
            const int px = rx_dist(rng);
            const int py = ry_dist(rng);
            attempted++;
            double val = 0.0;
            CPLErr err = GDALRasterIO(band, GF_Read, px, py, 1, 1,
                                       &val, 1, 1, GDT_Float64, 0, 0);
            if (err != CE_None) continue;
            if (is_invalid(val, nodata, has_nodata)) continue;
            samples.push_back(val);
        }
    }

    if (samples.empty()) {
        return r;  // version tag still set; has_stats stays false
    }

    // Single-pass accumulators
    double sum = 0.0;
    double sum_squares = 0.0;
    double smin = samples.front();
    double smax = samples.front();
    for (double v : samples) {
        sum += v;
        sum_squares += v * v;
        if (v < smin) smin = v;
        if (v > smax) smax = v;
    }
    const double n = static_cast<double>(samples.size());
    const double mean = sum / n;
    const double variance = (sum_squares / n) - (mean * mean);
    const double stddev = std::sqrt(std::max(0.0, variance));

    r.count = static_cast<int64_t>(samples.size());
    r.min = smin;
    r.max = smax;
    r.mean = mean;
    r.stddev = stddev;
    r.sum = sum;
    r.sum_squares = sum_squares;
    r.valid_percent = (attempted > 0)
        ? (100.0 * static_cast<double>(samples.size()) / static_cast<double>(attempted))
        : 100.0;
    r.approximated = true;
    r.has_stats = true;

    // Sort once for quantiles. raster-loader uses np.quantile(method="lower")
    // which floors the index — match by integer division.
    std::sort(samples.begin(), samples.end());
    const int64_t cnt = r.count;
    for (int N = kOverviewMin; N <= kOverviewMax; N++) {
        std::vector<double> qs;
        qs.reserve(N - 1);
        for (int j = 1; j < N; j++) {
            int64_t idx = (cnt * j) / N;
            if (idx >= cnt) idx = cnt - 1;
            qs.push_back(samples[idx]);
        }
        r.quantiles[N] = std::move(qs);
    }

    // top_values: bin samples by exact value, take top-K by frequency.
    // For integer-typed bands the cast to int64 is lossless; for float
    // bands the discretisation matters less than the relative ranking
    // at sample sizes ≤ 1000.
    const bool integer_typed = (dtype == GDT_Byte  || dtype == GDT_Int8 ||
                                dtype == GDT_UInt16 || dtype == GDT_Int16 ||
                                dtype == GDT_UInt32 || dtype == GDT_Int32 ||
                                dtype == GDT_UInt64 || dtype == GDT_Int64);
    std::vector<std::pair<double, int64_t>> ranked;
    if (integer_typed) {
        std::unordered_map<int64_t, int64_t> freq;
        for (double v : samples) freq[static_cast<int64_t>(v)]++;
        ranked.reserve(freq.size());
        for (auto &kv : freq) ranked.emplace_back(static_cast<double>(kv.first), kv.second);
    } else {
        std::unordered_map<double, int64_t> freq;
        for (double v : samples) freq[v]++;
        ranked.reserve(freq.size());
        for (auto &kv : freq) ranked.emplace_back(kv.first, kv.second);
    }
    std::sort(ranked.begin(), ranked.end(),
              [](const std::pair<double, int64_t> &a,
                 const std::pair<double, int64_t> &b) {
                  if (a.second != b.second) return a.second > b.second;
                  return a.first < b.first;
              });
    const int K = std::min<int>(kMaxMostCommon, static_cast<int>(ranked.size()));
    for (int i = 0; i < K; i++) {
        if (ranked[i].second > 0) {
            r.top_values[ranked[i].first] = ranked[i].second;
        }
    }

    return r;
}

}  // namespace raquet
}  // namespace duckdb

#endif  // RAQUET_HAS_GDAL
