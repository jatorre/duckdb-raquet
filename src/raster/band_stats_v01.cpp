#ifdef RAQUET_HAS_GDAL

#include "band_stats_v01.hpp"

#include <gdal.h>
#include <cpl_error.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
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
static constexpr int  kFloatBins     = 2048;
static constexpr int  kWideIntBins   = 8192;
static constexpr const char *kVersionTag = "duckdb-raquet-0.1.0";

static inline bool is_invalid(double v, double nodata, bool has_nodata) {
    if (std::isnan(v) || std::isinf(v)) return true;
    if (has_nodata) {
        if (std::isnan(nodata)) return std::isnan(v);
        return v == nodata;
    }
    return false;
}

static inline bool is_integer_dtype(GDALDataType dt) {
    return dt == GDT_Byte   || dt == GDT_Int8 ||
           dt == GDT_UInt16 || dt == GDT_Int16 ||
           dt == GDT_UInt32 || dt == GDT_Int32 ||
           dt == GDT_UInt64 || dt == GDT_Int64;
}

// Fits-in-fixed-bucket integer (uint8/int8/uint16/int16) — value cast
// to int64 is its own bucket key, no quantisation, no min/max needed.
static inline bool is_fixed_bucket_int(GDALDataType dt) {
    return dt == GDT_Byte || dt == GDT_Int8 ||
           dt == GDT_UInt16 || dt == GDT_Int16;
}

// ─────────────────────────────────────────────
// Step 1: pull basic stats from GDAL.
// ─────────────────────────────────────────────
//
// Populates: count, min, max, mean, stddev, sum, sum_squares,
//   valid_percent, has_stats, approximated.
//
// Returns false if GDAL couldn't produce stats (e.g. all-nodata band);
// the caller should still emit the result with has_stats=false (basic
// fields zero) plus empty quantiles/top_values.
static bool extract_basic_stats(
    GDALRasterBandH band,
    int raster_width, int raster_height,
    bool approx,
    V01BandStatsResult &out) {

    double smin = 0, smax = 0, smean = 0, sstddev = 0;
    CPLErr err = GDALComputeRasterStatistics(
        band, approx ? TRUE : FALSE,
        &smin, &smax, &smean, &sstddev, nullptr, nullptr);
    if (err != CE_None) {
        return false;
    }
    const char *vp = GDALGetMetadataItem(band, "STATISTICS_VALID_PERCENT", nullptr);
    double valid_pct = vp ? std::stod(vp) : 100.0;
    int64_t total = static_cast<int64_t>(raster_width) *
                    static_cast<int64_t>(raster_height);
    int64_t count = static_cast<int64_t>(std::floor(valid_pct / 100.0 * total));

    out.count        = count;
    out.min          = smin;
    out.max          = smax;
    out.mean         = smean;
    out.stddev       = sstddev;
    out.sum          = smean * static_cast<double>(count);
    out.sum_squares  = static_cast<double>(count) * (sstddev * sstddev + smean * smean);
    out.valid_percent = valid_pct;
    out.approximated = approx;
    out.has_stats    = true;
    return true;
}

// ─────────────────────────────────────────────
// Step 2a (approx): collect 1000-pixel random sample
// ─────────────────────────────────────────────
static std::vector<double> sample_band(
    GDALRasterBandH band,
    int raster_width, int raster_height,
    double nodata, bool has_nodata) {

    std::mt19937_64 rng(0x6172717565742d76ULL);  // "raquet-v" in ASCII — deterministic
    std::uniform_int_distribution<int> rx_dist(0, raster_width - 1);
    std::uniform_int_distribution<int> ry_dist(0, raster_height - 1);

    std::vector<double> samples;
    samples.reserve(kMaxSamples);

    for (int iter = 0; iter < kMaxIterations; iter++) {
        const int needed = kMaxSamples - static_cast<int>(samples.size());
        if (needed <= 0) break;
        for (int i = 0; i < needed; i++) {
            const int px = rx_dist(rng);
            const int py = ry_dist(rng);
            double val = 0.0;
            CPLErr err = GDALRasterIO(band, GF_Read, px, py, 1, 1,
                                       &val, 1, 1, GDT_Float64, 0, 0);
            if (err != CE_None) continue;
            if (is_invalid(val, nodata, has_nodata)) continue;
            samples.push_back(val);
        }
    }
    return samples;
}

// Quantiles and top_values from a flat sample vector (raster-loader's
// approx path: sort once, index by integer floor; bucket integer-cast
// values for top-K).
static void quantiles_and_top_values_from_sample(
    std::vector<double> samples, GDALDataType dtype,
    std::map<int, std::vector<double>> &quantiles,
    std::map<double, int64_t> &top_values) {

    if (samples.empty()) return;

    std::sort(samples.begin(), samples.end());
    const int64_t cnt = static_cast<int64_t>(samples.size());
    for (int N = kOverviewMin; N <= kOverviewMax; N++) {
        std::vector<double> qs;
        qs.reserve(N - 1);
        for (int j = 1; j < N; j++) {
            int64_t idx = (cnt * j) / N;
            if (idx >= cnt) idx = cnt - 1;
            qs.push_back(samples[idx]);
        }
        quantiles[N] = std::move(qs);
    }

    const bool integer_typed = is_integer_dtype(dtype);
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
            top_values[ranked[i].first] = ranked[i].second;
        }
    }
}

// ─────────────────────────────────────────────
// Step 2b (exact): histogram-based quantiles + top_values.
// ─────────────────────────────────────────────
//
// Streams the full band block-by-block, accumulating an ordered
// histogram (std::map<int64_t, int64_t>). For fixed-bucket integer
// dtypes the histogram is exact (one bucket per distinct value). For
// wider ints / floats, values bucket into N equal-width bins between
// the min/max already known from the GDAL call, and bucket centers
// represent the value when emitting top_values.
static void quantiles_and_top_values_from_histogram(
    GDALRasterBandH band,
    int raster_width, int raster_height,
    double nodata, bool has_nodata,
    GDALDataType dtype, double dmin, double dmax,
    std::map<int, std::vector<double>> &quantiles,
    std::map<double, int64_t> &top_values) {

    // Bucketing strategy — see helpers above.
    const bool fixed_int = is_fixed_bucket_int(dtype);
    const bool integer_typed = is_integer_dtype(dtype);
    const int  n_bins = integer_typed ? kWideIntBins : kFloatBins;
    const double bin_span = (dmax > dmin) ? (dmax - dmin) : 1.0;
    const double bin_width = bin_span / static_cast<double>(n_bins);

    auto bucket_for = [&](double v) -> int64_t {
        if (fixed_int) {
            return static_cast<int64_t>(v);
        }
        if (integer_typed) {
            // adaptive integer bins
            double rel = (v - dmin) / bin_span;
            int64_t b = static_cast<int64_t>(std::floor(rel * n_bins));
            if (b < 0) b = 0;
            if (b >= n_bins) b = n_bins - 1;
            return b;
        }
        // float
        double rel = (v - dmin) / bin_span;
        int64_t b = static_cast<int64_t>(std::floor(rel * n_bins));
        if (b < 0) b = 0;
        if (b >= n_bins) b = n_bins - 1;
        return b;
    };
    auto value_for = [&](int64_t bucket) -> double {
        if (fixed_int) {
            return static_cast<double>(bucket);
        }
        // bucket center
        return dmin + (static_cast<double>(bucket) + 0.5) * bin_width;
    };

    // Accumulate via GDALReadBlock for efficient chunked I/O.
    int block_xsize = 0, block_ysize = 0;
    GDALGetBlockSize(band, &block_xsize, &block_ysize);
    if (block_xsize <= 0) block_xsize = 256;
    if (block_ysize <= 0) block_ysize = 256;
    const int blocks_x = (raster_width  + block_xsize - 1) / block_xsize;
    const int blocks_y = (raster_height + block_ysize - 1) / block_ysize;

    // Buffer for one block at the band's native dtype.
    const int dtype_size = GDALGetDataTypeSizeBytes(dtype);
    std::vector<uint8_t> block_buf(static_cast<size_t>(block_xsize) *
                                    static_cast<size_t>(block_ysize) *
                                    static_cast<size_t>(dtype_size));

    std::map<int64_t, int64_t> histogram;

    auto extract_one = [&](const uint8_t *p, GDALDataType dt) -> double {
        switch (dt) {
            case GDT_Byte:    return static_cast<double>(*reinterpret_cast<const uint8_t *>(p));
            case GDT_Int8:    return static_cast<double>(*reinterpret_cast<const int8_t  *>(p));
            case GDT_UInt16:  return static_cast<double>(*reinterpret_cast<const uint16_t *>(p));
            case GDT_Int16:   return static_cast<double>(*reinterpret_cast<const int16_t  *>(p));
            case GDT_UInt32:  return static_cast<double>(*reinterpret_cast<const uint32_t *>(p));
            case GDT_Int32:   return static_cast<double>(*reinterpret_cast<const int32_t  *>(p));
            case GDT_UInt64:  return static_cast<double>(*reinterpret_cast<const uint64_t *>(p));
            case GDT_Int64:   return static_cast<double>(*reinterpret_cast<const int64_t  *>(p));
            case GDT_Float32: return static_cast<double>(*reinterpret_cast<const float    *>(p));
            case GDT_Float64: return *reinterpret_cast<const double *>(p);
            default:          return std::numeric_limits<double>::quiet_NaN();
        }
    };

    for (int by = 0; by < blocks_y; by++) {
        const int valid_ysize = std::min(block_ysize, raster_height - by * block_ysize);
        for (int bx = 0; bx < blocks_x; bx++) {
            const int valid_xsize = std::min(block_xsize, raster_width - bx * block_xsize);
            CPLErr err = GDALReadBlock(band, bx, by, block_buf.data());
            if (err != CE_None) continue;
            for (int y = 0; y < valid_ysize; y++) {
                const uint8_t *row = block_buf.data() +
                    static_cast<size_t>(y) * static_cast<size_t>(block_xsize) * dtype_size;
                for (int x = 0; x < valid_xsize; x++) {
                    double v = extract_one(row + static_cast<size_t>(x) * dtype_size, dtype);
                    if (is_invalid(v, nodata, has_nodata)) continue;
                    histogram[bucket_for(v)]++;
                }
            }
        }
    }

    if (histogram.empty()) return;

    // Quantiles via CDF walk over the ordered histogram. method="lower"
    // matches raster-loader's numpy.quantile call.
    int64_t total = 0;
    for (auto &kv : histogram) total += kv.second;
    if (total <= 0) return;

    for (int N = kOverviewMin; N <= kOverviewMax; N++) {
        std::vector<double> qs;
        qs.reserve(N - 1);
        // Targets are floor(total*j/N) for j in 1..N-1 — same formula as
        // the sample path's index calculation.
        std::vector<int64_t> targets;
        targets.reserve(N - 1);
        for (int j = 1; j < N; j++) {
            targets.push_back((total * j) / N);
        }
        // Walk the CDF; for each target index, take the value of the
        // bucket where the running count first crosses the target.
        size_t target_idx = 0;
        int64_t running = 0;
        for (auto &kv : histogram) {
            running += kv.second;
            while (target_idx < targets.size() && running > targets[target_idx]) {
                qs.push_back(value_for(kv.first));
                target_idx++;
            }
            if (target_idx >= targets.size()) break;
        }
        // Pad in case of edge cases at the boundary.
        while (qs.size() < static_cast<size_t>(N - 1)) {
            qs.push_back(value_for(histogram.rbegin()->first));
        }
        quantiles[N] = std::move(qs);
    }

    // top_values: top-K buckets by frequency.
    std::vector<std::pair<int64_t, int64_t>> ranked(histogram.begin(), histogram.end());
    std::sort(ranked.begin(), ranked.end(),
              [](const std::pair<int64_t, int64_t> &a,
                 const std::pair<int64_t, int64_t> &b) {
                  if (a.second != b.second) return a.second > b.second;
                  return a.first < b.first;
              });
    const int K = std::min<int>(kMaxMostCommon, static_cast<int>(ranked.size()));
    for (int i = 0; i < K; i++) {
        if (ranked[i].second > 0) {
            top_values[value_for(ranked[i].first)] = ranked[i].second;
        }
    }
}

// ─────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────
V01BandStatsResult compute_v01_band_stats(
    GDALRasterBandH band,
    int raster_width, int raster_height,
    double nodata, bool has_nodata,
    GDALDataType dtype,
    bool approx) {

    V01BandStatsResult r;
    r.version = kVersionTag;
    if (!band || raster_width <= 0 || raster_height <= 0) {
        return r;
    }

    // Step 1: basic stats from GDAL (count/min/max/mean/stddev/sum/sum_squares
    // + valid_percent + approximated_stats).
    if (!extract_basic_stats(band, raster_width, raster_height, approx, r)) {
        // GDAL couldn't produce stats (probably all-nodata). Leave basic
        // fields at zero, has_stats=false, quantiles/top_values empty.
        return r;
    }

    // Step 2: extensions (quantiles, top_values).
    if (approx) {
        auto samples = sample_band(band, raster_width, raster_height, nodata, has_nodata);
        quantiles_and_top_values_from_sample(std::move(samples), dtype,
                                             r.quantiles, r.top_values);
    } else {
        quantiles_and_top_values_from_histogram(
            band, raster_width, raster_height,
            nodata, has_nodata, dtype, r.min, r.max,
            r.quantiles, r.top_values);
    }

    // Empty quantiles/top_values are emitted as `{}` by the serializer
    // — no fallback ladder. Callers should treat them as best-effort
    // extensions, not load-bearing fields.
    return r;
}

}  // namespace raquet
}  // namespace duckdb

#endif  // RAQUET_HAS_GDAL
