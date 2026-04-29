#ifdef RAQUET_HAS_GDAL

#include "read_raster.hpp"
#include "band_encoder.hpp"
#include "band_decoder.hpp"
#include "band_stats_v01.hpp"
#include "raquet_metadata.hpp"
#include "quadbin.hpp"
#include "proj_embed.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

#include <gdal.h>
#include <gdal_alg.h>
#include <gdalwarper.h>
#include <gdal_utils.h>
#include <ogr_srs_api.h>
#include <cpl_conv.h>
#include <cpl_string.h>
#include <cpl_vsi.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace duckdb {

// ─────────────────────────────────────────────
// GDAL type → raquet BandDataType string mapping
// ─────────────────────────────────────────────
static std::string GDALTypeToRaquetType(GDALDataType dt) {
    switch (dt) {
        case GDT_Byte:    return "uint8";
        case GDT_Int8:    return "int8";
        case GDT_UInt16:  return "uint16";
        case GDT_Int16:   return "int16";
        case GDT_UInt32:  return "uint32";
        case GDT_Int32:   return "int32";
        case GDT_UInt64:  return "uint64";
        case GDT_Int64:   return "int64";
        case GDT_Float32: return "float32";
        case GDT_Float64: return "float64";
        // Complex types map to their real component type
        case GDT_CInt16:   return "int16";
        case GDT_CInt32:   return "int32";
        case GDT_CFloat32: return "float32";
        case GDT_CFloat64: return "float64";
        default:
            throw InvalidInputException("Unsupported GDAL data type: %d", static_cast<int>(dt));
    }
}

static int GDALTypeSize(GDALDataType dt) {
    return GDALGetDataTypeSizeBytes(dt);
}

// Parse resampling algorithm string → GDAL enum
static GDALResampleAlg ParseResampling(const std::string &s) {
    if (s == "nearest")      return GRA_NearestNeighbour;
    if (s == "bilinear")     return GRA_Bilinear;
    if (s == "cubic")        return GRA_Cubic;
    if (s == "cubicspline")  return GRA_CubicSpline;
    if (s == "lanczos")      return GRA_Lanczos;
    if (s == "average")      return GRA_Average;
    if (s == "mode")         return GRA_Mode;
    if (s == "max")          return GRA_Max;
    if (s == "min")          return GRA_Min;
    if (s == "med")          return GRA_Med;
    if (s == "q1")           return GRA_Q1;
    if (s == "q3")           return GRA_Q3;
    if (s == "sum")          return GRA_Sum;
    if (s == "rms")          return GRA_RMS;
    throw InvalidInputException("Unknown resampling algorithm: '%s'", s);
}

// ─────────────────────────────────────────────
// Tile structure
// ─────────────────────────────────────────────
struct RasterTile {
    int x, y, z;
};

// ─────────────────────────────────────────────
// Bind data — holds everything discovered at bind time
// ─────────────────────────────────────────────
struct ReadRasterBindData : public TableFunctionData {
    std::string filename;

    // GDAL-detected raster properties
    int raster_band_count = 0;
    GDALDataType gdal_dtype = GDT_Byte;
    std::string raquet_dtype;
    int dtype_bytes = 1;

    // Source raster dimensions in source pixel space (top-level width/height).
    int raster_width = 0;
    int raster_height = 0;

    // Per-band metadata
    std::vector<double> band_nodatas;
    std::vector<bool> band_has_nodata;
    std::vector<std::string> band_color_interps;
    std::vector<std::string> band_descriptions;
    std::vector<std::string> band_units;
    std::vector<double> band_scales;
    std::vector<double> band_offsets;
    std::vector<bool> band_has_scale;
    std::vector<bool> band_has_offset;
    // Per-band palette + GDAL-derived stats — empty entries when unavailable.
    std::vector<std::vector<std::array<int, 4>>> band_colortables;
    std::vector<raquet::BandInfo::Stats> band_stats;

    // Tile statistics
    bool statistics = false;
    // Use approxOK=TRUE for GDAL stats (faster — overview-based when available).
    bool approx_stats = true;

    // Spatial info
    double bounds_minlon = 0, bounds_minlat = 0, bounds_maxlon = 0, bounds_maxlat = 0;

    // Zoom / tiling
    int max_zoom = 0;
    int min_zoom = 0;
    int block_size = 256;  // 256 or 512
    int block_zoom = 8;    // log2(block_size)

    // User parameters
    std::string compression = "gzip";
    int compression_quality = 85;
    GDALResampleAlg resampling = GRA_NearestNeighbour;
    std::string band_layout = "sequential";
    std::string overviews = "auto";
    std::string zoom_strategy = "auto"; // auto, lower, upper
    std::string output_format = "v0.5.0"; // v0 or v0.5.0

    // CF time dimension (NetCDF)
    bool has_cf_time = false;
    std::string cf_units_string;        // e.g., "minutes since 1980-01-01 00:00:00"
    std::string cf_calendar;            // e.g., "standard"
    std::vector<double> cf_time_values; // time value per band

    // Source CRS WKT for coordinate transformation
    std::string src_wkt;
    bool src_is_web_mercator = false;
    int overview_count = 0;

    // Estimated tile count (for cardinality estimation)
    idx_t estimated_tiles = 0;

    // Output column names
    std::vector<std::string> column_names;
};

// ─────────────────────────────────────────────
// Result from reading and compressing bands
// ─────────────────────────────────────────────
struct TileData {
    std::vector<std::vector<uint8_t>> compressed;  // compressed band buffers
    std::vector<raquet::BandStats> stats;           // per-band statistics (empty if not requested)
};

// One overview tile fully prepared for emission. Phase 2 stages these into a
// shared queue (single-producer, multi-consumer) instead of writing directly
// to the output DataChunk, so emission can span multiple Execute calls
// without losing tiles past STANDARD_VECTOR_SIZE.
struct OverviewResult {
    uint64_t block;
    TileData tile_data;
};

// ─────────────────────────────────────────────
// Overview tile (flat list, processed single-threaded)
// ─────────────────────────────────────────────
struct OverviewFrame {
    RasterTile tile;
};

// ─────────────────────────────────────────────
// Global state — two-phase: parallel native zoom, then single-thread overviews
// ─────────────────────────────────────────────
struct ReadRasterGlobalState : public GlobalTableFunctionState {
    // Phase 1: Native-zoom tiles (parallel-safe, mutex-protected queue)
    std::vector<RasterTile> native_tiles;
    std::mutex tile_mutex;
    idx_t next_tile_idx = 0;

    // Phase 2 work queue: every thread pulls overview frames from this list
    // via next_overview_idx and warps them with its own per-thread GDAL handle
    // (local.src_ds), then pushes the result into overview_results under a
    // mutex. The last thread to finish publishes phase2_staged; afterwards
    // any thread can drain overview_results lock-free.
    std::vector<OverviewFrame> overview_frames;
    std::atomic<idx_t> next_overview_idx{0};            // work pull pointer
    std::atomic<idx_t> overview_frames_processed{0};    // completion counter

    // One-shot Phase 2 init: the first thread to enter post-native work waits
    // for Phase 1 stragglers, then publishes phase2_init_done so siblings can
    // start pulling from the queue.
    std::atomic<bool> phase2_init_claimed{false};
    std::atomic<bool> phase2_init_done{false};

    // Phase 2 staged results — filled in parallel by every Phase 2 worker
    // (push_back guarded by overview_results_mutex), drained lock-free via
    // overview_drain_idx after phase2_staged is set. Staging into a queue
    // instead of writing directly into the output DataChunk is what avoids
    // the silent row-cap drop that capped Phase 2 emission at
    // STANDARD_VECTOR_SIZE rows (for Germany at default zoom: ~11k overview
    // tiles were silently lost).
    std::vector<OverviewResult> overview_results;
    std::mutex overview_results_mutex;
    std::atomic<idx_t> overview_drain_idx{0};
    std::atomic<bool> phase2_staged{false};

    // Shared config
    GDALResampleAlg source_resampling = GRA_NearestNeighbour;
    double nodata_value = 0;
    bool has_nodata = false;
    std::string web_mercator_wkt_str;

    // Tracking
    std::atomic<int> total_blocks{0};
    std::atomic<bool> metadata_emitted{false};
    std::atomic<bool> finished{false};

    // Phase 1 completion counter — incremented after each native tile is fully
    // processed (emitted or skipped-empty). Used by the Phase 2 init winner
    // to wait for in-flight workers before any thread reads total_blocks.
    std::atomic<idx_t> phase1_finished{0};

    // Whether we need overviews at all
    bool has_overviews = false;

    idx_t MaxThreads() const override {
        return GlobalTableFunctionState::MAX_THREADS;
    }

};

// ─────────────────────────────────────────────
// Local state — per-thread GDAL handles
// ─────────────────────────────────────────────
struct ReadRasterLocalState : public LocalTableFunctionState {
    GDALDatasetH src_ds = nullptr;
    GDALDriverH gtiff_driver = nullptr;
    char *web_mercator_wkt = nullptr;
    bool initialized = false;

    ~ReadRasterLocalState() {
        if (web_mercator_wkt) CPLFree(web_mercator_wkt);
        if (src_ds) GDALClose(src_ds);
    }
};

// ─────────────────────────────────────────────
// Helper: Enumerate tiles at a given zoom that intersect bounds
// ─────────────────────────────────────────────
static std::vector<RasterTile> EnumerateTiles(double minlon, double minlat,
                                               double maxlon, double maxlat, int zoom) {
    std::vector<RasterTile> tiles;
    int min_tx, min_ty, max_tx, max_ty;
    quadbin::lonlat_to_tile(minlon, maxlat, zoom, min_tx, min_ty); // NW corner
    quadbin::lonlat_to_tile(maxlon, minlat, zoom, max_tx, max_ty); // SE corner

    for (int ty = min_ty; ty <= max_ty; ty++) {
        for (int tx = min_tx; tx <= max_tx; tx++) {
            tiles.push_back({tx, ty, zoom});
        }
    }
    return tiles;
}

// ─────────────────────────────────────────────
// Helper: Create an in-memory tile dataset for warping into
// Thread-safe counter for unique /vsimem/ paths
static std::atomic<uint64_t> vsimem_counter{0};

static GDALDatasetH CreateTileDataset(GDALDriverH driver, const char *wkt_3857,
                                       const RasterTile &tile, int tile_size,
                                       int band_count, GDALDataType dtype,
                                       double nodata, bool has_nodata,
                                       std::string &path_out) {
    // Unique virtual path (atomic counter avoids collisions across threads)
    uint64_t id = vsimem_counter++;
    char path[256];
    snprintf(path, sizeof(path), "/vsimem/raquet-%llu.tif", static_cast<unsigned long long>(id));
    path_out = path;

    GDALDatasetH ds = GDALCreate(driver, path, tile_size, tile_size, band_count, dtype, nullptr);
    if (!ds) {
        throw IOException("Failed to create in-memory tile dataset for %d/%d/%d", tile.z, tile.x, tile.y);
    }

    // Set CRS
    GDALSetProjection(ds, wkt_3857);

    // Set geotransform from tile bounds in Web Mercator
    double xmin, ymin, xmax, ymax;
    quadbin::tile_to_bbox_mercator(tile.x, tile.y, tile.z, xmin, ymin, xmax, ymax);
    double px_width = (xmax - xmin) / tile_size;
    double px_height = (ymax - ymin) / tile_size;
    double gt[6] = {xmin, px_width, 0, ymax, 0, -px_height};
    GDALSetGeoTransform(ds, gt);

    // Fill bands with nodata
    for (int b = 1; b <= band_count; b++) {
        GDALRasterBandH band = GDALGetRasterBand(ds, b);
        if (has_nodata) {
            GDALSetRasterNoDataValue(band, nodata);
            GDALFillRaster(band, nodata, 0);
        }
    }

    return ds;
}

// ─────────────────────────────────────────────
// Helper: Warp source dataset into tile dataset
// ─────────────────────────────────────────────
static void WarpIntoTile(GDALDatasetH src_ds, GDALDatasetH tile_ds,
                          GDALResampleAlg resample, double nodata, bool has_nodata,
                          int overview_level = -1) {
    GDALWarpOptions *wo = GDALCreateWarpOptions();
    wo->hSrcDS = src_ds;
    wo->hDstDS = tile_ds;
    wo->eResampleAlg = resample;
    wo->nBandCount = GDALGetRasterCount(src_ds);

    // Set up band mapping
    wo->panSrcBands = static_cast<int *>(CPLMalloc(sizeof(int) * wo->nBandCount));
    wo->panDstBands = static_cast<int *>(CPLMalloc(sizeof(int) * wo->nBandCount));
    for (int i = 0; i < wo->nBandCount; i++) {
        wo->panSrcBands[i] = i + 1;
        wo->panDstBands[i] = i + 1;
    }

    if (has_nodata) {
        wo->padfSrcNoDataReal = static_cast<double *>(CPLMalloc(sizeof(double) * wo->nBandCount));
        wo->padfDstNoDataReal = static_cast<double *>(CPLMalloc(sizeof(double) * wo->nBandCount));
        for (int i = 0; i < wo->nBandCount; i++) {
            wo->padfSrcNoDataReal[i] = nodata;
            wo->padfDstNoDataReal[i] = nodata;
        }
    }

    // Build transformer options. OVERVIEW_LEVEL belongs on the transformer
    // (consumed by GDALCreateGenImgProjTransformer2 as SRC_OVERVIEW_LEVEL),
    // not on GDALWarpOptions::papszWarpOptions.
    char **transformer_options = nullptr;
    if (overview_level >= 0) {
        char ovr_str[32];
        snprintf(ovr_str, sizeof(ovr_str), "%d", overview_level);
        transformer_options = CSLSetNameValue(transformer_options, "SRC_OVERVIEW_LEVEL", ovr_str);
    }

    // Create coordinate transformation (v2 form so SRC_OVERVIEW_LEVEL is honored)
    wo->pTransformerArg = GDALCreateGenImgProjTransformer2(src_ds, tile_ds, transformer_options);
    CSLDestroy(transformer_options);
    if (!wo->pTransformerArg) {
        GDALDestroyWarpOptions(wo);
        throw IOException("Failed to create image projection transformer");
    }
    wo->pfnTransformer = GDALGenImgProjTransform;

    GDALWarpOperation warp_op;
    CPLErr err = warp_op.Initialize(wo);
    if (err != CE_None) {
        GDALDestroyGenImgProjTransformer(wo->pTransformerArg);
        GDALDestroyWarpOptions(wo);
        throw IOException("Warp initialization failed");
    }

    err = warp_op.ChunkAndWarpImage(0, 0,
                                     GDALGetRasterXSize(tile_ds),
                                     GDALGetRasterYSize(tile_ds));

    GDALDestroyGenImgProjTransformer(wo->pTransformerArg);
    wo->pTransformerArg = nullptr;
    GDALDestroyWarpOptions(wo);

    if (err != CE_None) {
        throw IOException("Warp execution failed for tile");
    }
}

// ─────────────────────────────────────────────
// Helper: Check if a tile is entirely nodata (empty)
// ─────────────────────────────────────────────
static bool IsTileEmpty(GDALDatasetH ds, double nodata, bool has_nodata) {
    if (!has_nodata) return false;

    int width = GDALGetRasterXSize(ds);
    int height = GDALGetRasterYSize(ds);
    GDALRasterBandH band = GDALGetRasterBand(ds, 1);

    // Read first band and check if all pixels are nodata
    size_t num_pixels = static_cast<size_t>(width) * height;
    GDALDataType dt = GDALGetRasterDataType(band);
    int dt_size = GDALGetDataTypeSizeBytes(dt);

    std::vector<uint8_t> buf(num_pixels * dt_size);
    CPLErr err = GDALRasterIO(band, GF_Read, 0, 0, width, height,
                               buf.data(), width, height, dt, 0, 0);
    if (err != CE_None) return false;

    bool is_nan_nodata = std::isnan(nodata);

    for (size_t i = 0; i < num_pixels; i++) {
        double val = 0;
        switch (dt) {
            case GDT_Byte:    val = static_cast<double>(buf[i]); break;
            case GDT_Int16:   { int16_t v; memcpy(&v, buf.data() + i * 2, 2); val = v; break; }
            case GDT_UInt16:  { uint16_t v; memcpy(&v, buf.data() + i * 2, 2); val = v; break; }
            case GDT_Int32:   { int32_t v; memcpy(&v, buf.data() + i * 4, 4); val = v; break; }
            case GDT_UInt32:  { uint32_t v; memcpy(&v, buf.data() + i * 4, 4); val = v; break; }
            case GDT_Float32: { float v; memcpy(&v, buf.data() + i * 4, 4); val = v; break; }
            case GDT_Float64: { memcpy(&val, buf.data() + i * 8, 8); break; }
            default: return false;
        }

        if (is_nan_nodata) {
            if (!std::isnan(val)) return false;
        } else {
            if (val != nodata) return false;
        }
    }
    return true;
}

// ─────────────────────────────────────────────
// Helper: Read and compress band data from a warped tile dataset
// Optionally computes per-band statistics from raw data before compression
// ─────────────────────────────────────────────
static TileData ReadAndCompressBands(
    GDALDatasetH ds, const std::string &compression, int quality,
    const std::string &band_layout, bool compute_stats,
    const std::string &dtype_str, bool has_nodata, double nodata_val) {

    TileData result;
    int width = GDALGetRasterXSize(ds);
    int height = GDALGetRasterYSize(ds);
    int band_count = GDALGetRasterCount(ds);
    GDALDataType dt = GDALGetRasterDataType(GDALGetRasterBand(ds, 1));
    int dt_size = GDALGetDataTypeSizeBytes(dt);
    size_t band_bytes = static_cast<size_t>(width) * height * dt_size;

    // Read all bands as raw bytes
    std::vector<std::vector<uint8_t>> raw_bands(band_count);
    for (int b = 0; b < band_count; b++) {
        raw_bands[b].resize(band_bytes);
        GDALRasterBandH band = GDALGetRasterBand(ds, b + 1);
        CPLErr err = GDALRasterIO(band, GF_Read, 0, 0, width, height,
                                   raw_bands[b].data(), width, height, dt, 0, 0);
        if (err != CE_None) {
            throw IOException("Failed to read band %d from tile", b + 1);
        }

        // Compute stats from raw (uncompressed) data if requested
        if (compute_stats) {
            auto stats = raquet::compute_band_stats(
                raw_bands[b].data(), raw_bands[b].size(),
                dtype_str, width, height,
                false, // data is already uncompressed
                has_nodata, nodata_val);
            result.stats.push_back(stats);
        }
    }

    if (band_layout == "interleaved") {
        auto interleaved = raquet::interleave_bands(raw_bands, width, height, dt_size);

        if (compression == "gzip") {
            result.compressed.push_back(raquet::compress_gzip(interleaved.data(), interleaved.size()));
        } else if (compression == "jpeg") {
            result.compressed.push_back(raquet::encode_jpeg(interleaved.data(), width, height,
                                                             band_count, quality));
        } else if (compression == "webp") {
            result.compressed.push_back(raquet::encode_webp(interleaved.data(), width, height,
                                                             band_count, quality));
        } else {
            result.compressed.push_back(std::move(interleaved));
        }
    } else {
        for (int b = 0; b < band_count; b++) {
            if (compression == "gzip") {
                result.compressed.push_back(raquet::compress_gzip(raw_bands[b].data(), raw_bands[b].size()));
            } else if (compression == "none" || compression.empty()) {
                result.compressed.push_back(std::move(raw_bands[b]));
            } else {
                throw InvalidInputException("Compression '%s' requires interleaved band layout",
                                             compression);
            }
        }
    }

    return result;
}

// ─────────────────────────────────────────────
// Pure-math WGS84 ↔ Web Mercator conversions (no PROJ needed)
// ─────────────────────────────────────────────
static double LonToMercatorX(double lon) {
    return lon * quadbin::EARTH_RADIUS * quadbin::PI / 180.0;
}

static double LatToMercatorY(double lat) {
    double lat_rad = lat * quadbin::PI / 180.0;
    return quadbin::EARTH_RADIUS * std::log(std::tan(quadbin::PI / 4.0 + lat_rad / 2.0));
}

static double MercatorXToLon(double x) {
    return x * 180.0 / (quadbin::EARTH_RADIUS * quadbin::PI);
}

static double MercatorYToLat(double y) {
    return (2.0 * std::atan(std::exp(y / quadbin::EARTH_RADIUS)) - quadbin::PI / 2.0) * 180.0 / quadbin::PI;
}

// ─────────────────────────────────────────────
// Helper: Calculate resolution in meters/pixel via coordinate transformation
// Follows CLI's find_resolution() logic
// ─────────────────────────────────────────────
static double CalculateResolution(GDALDatasetH ds, OGRCoordinateTransformationH tx) {
    double gt[6];
    GDALGetGeoTransform(ds, gt);
    double xoff = gt[0], xres = gt[1], yoff = gt[3], yres = gt[5];
    int xdim = GDALGetRasterXSize(ds);
    int ydim = GDALGetRasterYSize(ds);

    double x1 = xoff, y1 = yoff;
    double x2 = xoff + xdim * xres, y2 = yoff + ydim * yres;

    OCTTransform(tx, 1, &x1, &y1, nullptr);
    OCTTransform(tx, 1, &x2, &y2, nullptr);

    return std::hypot(x2 - x1, y2 - y1) / std::hypot(static_cast<double>(xdim),
                                                        static_cast<double>(ydim));
}

// Resolution in Web Mercator meters/pixel using pure math (WGS84 source)
static double CalculateResolutionFromWGS84(GDALDatasetH ds) {
    double gt[6];
    GDALGetGeoTransform(ds, gt);
    double xoff = gt[0], xres = gt[1], yoff = gt[3], yres = gt[5];
    int xdim = GDALGetRasterXSize(ds);
    int ydim = GDALGetRasterYSize(ds);

    double x1 = LonToMercatorX(xoff);
    double y1 = LatToMercatorY(yoff);
    double x2 = LonToMercatorX(xoff + xdim * xres);
    double y2 = LatToMercatorY(yoff + ydim * yres);

    return std::hypot(x2 - x1, y2 - y1) / std::hypot(static_cast<double>(xdim),
                                                        static_cast<double>(ydim));
}

// Resolution in Web Mercator meters/pixel (source already in 3857)
static double CalculateResolutionFromMercator(GDALDatasetH ds) {
    double gt[6];
    GDALGetGeoTransform(ds, gt);
    double xoff = gt[0], xres = gt[1], yoff = gt[3], yres = gt[5];
    int xdim = GDALGetRasterXSize(ds);
    int ydim = GDALGetRasterYSize(ds);

    double x1 = xoff, y1 = yoff;
    double x2 = xoff + xdim * xres, y2 = yoff + ydim * yres;

    return std::hypot(x2 - x1, y2 - y1) / std::hypot(static_cast<double>(xdim),
                                                        static_cast<double>(ydim));
}

// ─────────────────────────────────────────────
// Helper: Calculate zoom from resolution (matches CLI find_zoom)
// ─────────────────────────────────────────────
static int CalculateZoom(double resolution, int block_zoom, const std::string &strategy = "auto") {
    constexpr double CE = 2.0 * quadbin::PI * quadbin::EARTH_RADIUS; // ~40075016.68
    double tile_dim = std::pow(2.0, block_zoom);
    double raw_zoom = std::log(CE / tile_dim / resolution) / std::log(2.0);
    if (strategy == "lower") {
        return static_cast<int>(std::floor(raw_zoom));
    } else if (strategy == "upper") {
        return static_cast<int>(std::ceil(raw_zoom));
    }
    return static_cast<int>(std::round(raw_zoom)); // "auto" = round
}

// ─────────────────────────────────────────────
// Helper: Calculate bounds in WGS84
// ─────────────────────────────────────────────
static void CalculateBounds(GDALDatasetH ds, OGRCoordinateTransformationH tx4326,
                             double &minlon, double &minlat, double &maxlon, double &maxlat) {
    double gt[6];
    GDALGetGeoTransform(ds, gt);
    double xoff = gt[0], xres = gt[1], yoff = gt[3], yres = gt[5];
    int xdim = GDALGetRasterXSize(ds);
    int ydim = GDALGetRasterYSize(ds);

    // Transform all 4 corners
    double xs[4] = {xoff, xoff, xoff + xdim * xres, xoff + xdim * xres};
    double ys[4] = {yoff, yoff + ydim * yres, yoff, yoff + ydim * yres};

    for (int i = 0; i < 4; i++) {
        OCTTransform(tx4326, 1, &xs[i], &ys[i], nullptr);
    }

    minlon = *std::min_element(xs, xs + 4);
    maxlon = *std::max_element(xs, xs + 4);
    minlat = *std::min_element(ys, ys + 4);
    maxlat = *std::max_element(ys, ys + 4);
}

// Bounds directly from geotransform (source already in WGS84)
static void CalculateBoundsFromWGS84(GDALDatasetH ds,
                                      double &minlon, double &minlat, double &maxlon, double &maxlat) {
    double gt[6];
    GDALGetGeoTransform(ds, gt);
    int xdim = GDALGetRasterXSize(ds);
    int ydim = GDALGetRasterYSize(ds);

    double xs[4] = {gt[0], gt[0], gt[0] + xdim * gt[1], gt[0] + xdim * gt[1]};
    double ys[4] = {gt[3], gt[3] + ydim * gt[5], gt[3], gt[3] + ydim * gt[5]};

    minlon = *std::min_element(xs, xs + 4);
    maxlon = *std::max_element(xs, xs + 4);
    minlat = *std::min_element(ys, ys + 4);
    maxlat = *std::max_element(ys, ys + 4);
}

// Bounds from Web Mercator source via pure math
static void CalculateBoundsFromMercator(GDALDatasetH ds,
                                         double &minlon, double &minlat, double &maxlon, double &maxlat) {
    double gt[6];
    GDALGetGeoTransform(ds, gt);
    int xdim = GDALGetRasterXSize(ds);
    int ydim = GDALGetRasterYSize(ds);

    double xs[4] = {gt[0], gt[0], gt[0] + xdim * gt[1], gt[0] + xdim * gt[1]};
    double ys[4] = {gt[3], gt[3] + ydim * gt[5], gt[3], gt[3] + ydim * gt[5]};

    // Convert 3857 corners to 4326
    for (int i = 0; i < 4; i++) {
        xs[i] = MercatorXToLon(xs[i]);
        ys[i] = MercatorYToLat(ys[i]);
    }

    minlon = *std::min_element(xs, xs + 4);
    maxlon = *std::max_element(xs, xs + 4);
    minlat = *std::min_element(ys, ys + 4);
    maxlat = *std::max_element(ys, ys + 4);
}

// ─────────────────────────────────────────────
// Helper: Calculate min zoom for reasonable overview size
// Matches CLI find_minzoom()
// ─────────────────────────────────────────────
static int CalculateMinZoom(double minlon, double minlat, double maxlon, double maxlat,
                             int max_zoom, int block_zoom) {
    constexpr int TARGET_MIN_SIZE = 128;
    constexpr int BIG_ZOOM = 32;

    int ul_x, ul_y, lr_x, lr_y;
    quadbin::lonlat_to_tile(minlon, maxlat, BIG_ZOOM, ul_x, ul_y);
    quadbin::lonlat_to_tile(maxlon, minlat, BIG_ZOOM, lr_x, lr_y);

    double high_hypot = std::hypot(static_cast<double>(lr_x - ul_x),
                                    static_cast<double>(lr_y - ul_y));
    double target_hypot = std::hypot(static_cast<double>(TARGET_MIN_SIZE),
                                      static_cast<double>(TARGET_MIN_SIZE));

    double min_zoom_raw = BIG_ZOOM - std::log(high_hypot / target_hypot) / std::log(2.0) - block_zoom;
    int min_zoom = static_cast<int>(std::round(min_zoom_raw));
    return std::max(0, std::min(max_zoom, min_zoom));
}

// ─────────────────────────────────────────────
// BIND
// ─────────────────────────────────────────────
static unique_ptr<FunctionData> ReadRasterBind(ClientContext &context,
                                                TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types,
                                                vector<string> &names) {
    auto bind_data = make_uniq<ReadRasterBindData>();
    bind_data->filename = input.inputs[0].GetValue<string>();

    // Parse named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "compression") {
            bind_data->compression = StringUtil::Lower(kv.second.GetValue<string>());
        } else if (kv.first == "resampling") {
            auto resample_str = StringUtil::Lower(kv.second.GetValue<string>());
            bind_data->resampling = ParseResampling(resample_str);
        } else if (kv.first == "block_size") {
            bind_data->block_size = kv.second.GetValue<int32_t>();
            if (bind_data->block_size != 256 && bind_data->block_size != 512 &&
                bind_data->block_size != 1024) {
                throw InvalidInputException("block_size must be 256, 512, or 1024");
            }
            bind_data->block_zoom = static_cast<int>(std::log2(bind_data->block_size));
        } else if (kv.first == "max_zoom") {
            bind_data->max_zoom = kv.second.GetValue<int32_t>();
        } else if (kv.first == "min_zoom") {
            bind_data->min_zoom = kv.second.GetValue<int32_t>();
        } else if (kv.first == "overviews") {
            bind_data->overviews = StringUtil::Lower(kv.second.GetValue<string>());
        } else if (kv.first == "band_layout") {
            bind_data->band_layout = StringUtil::Lower(kv.second.GetValue<string>());
        } else if (kv.first == "quality") {
            bind_data->compression_quality = kv.second.GetValue<int32_t>();
        } else if (kv.first == "statistics") {
            bind_data->statistics = kv.second.GetValue<bool>();
        } else if (kv.first == "zoom_strategy") {
            bind_data->zoom_strategy = StringUtil::Lower(kv.second.GetValue<string>());
            if (bind_data->zoom_strategy != "auto" && bind_data->zoom_strategy != "lower" &&
                bind_data->zoom_strategy != "upper") {
                throw InvalidInputException("zoom_strategy must be 'auto', 'lower', or 'upper'");
            }
        } else if (kv.first == "format") {
            bind_data->output_format = StringUtil::Lower(kv.second.GetValue<string>());
            if (bind_data->output_format != "v0" && bind_data->output_format != "v0.5.0") {
                throw InvalidInputException("format must be 'v0' or 'v0.5.0'");
            }
        } else if (kv.first == "approx") {
            bind_data->approx_stats = kv.second.GetValue<bool>();
        }
    }

    // Initialize embedded PROJ database and GDAL
    raquet::InitEmbeddedProj();
    GDALAllRegister();

    // Open the raster
    GDALDatasetH ds = GDALOpen(bind_data->filename.c_str(), GA_ReadOnly);
    if (!ds) {
        // Try with ASSUME_LONGLAT=YES for files without CRS (e.g., some NetCDFs)
        char **open_options = CSLSetNameValue(nullptr, "ASSUME_LONGLAT", "YES");
        ds = GDALOpenEx(bind_data->filename.c_str(), GDAL_OF_RASTER | GDAL_OF_READONLY,
                         nullptr, const_cast<const char *const *>(open_options), nullptr);
        CSLDestroy(open_options);
    }
    if (!ds) {
        throw IOException("Failed to open raster file: %s", bind_data->filename);
    }

    // Read raster properties
    bind_data->raster_band_count = GDALGetRasterCount(ds);
    bind_data->raster_width = GDALGetRasterXSize(ds);
    bind_data->raster_height = GDALGetRasterYSize(ds);
    if (bind_data->raster_band_count == 0) {
        GDALClose(ds);
        throw InvalidInputException("Raster file has no bands: %s", bind_data->filename);
    }

    GDALRasterBandH first_band = GDALGetRasterBand(ds, 1);
    bind_data->gdal_dtype = GDALGetRasterDataType(first_band);
    bind_data->raquet_dtype = GDALTypeToRaquetType(bind_data->gdal_dtype);
    bind_data->dtype_bytes = GDALTypeSize(bind_data->gdal_dtype);

    // Per-band metadata
    for (int b = 1; b <= bind_data->raster_band_count; b++) {
        GDALRasterBandH band = GDALGetRasterBand(ds, b);
        int has_nd = 0;
        double nd = GDALGetRasterNoDataValue(band, &has_nd);
        bind_data->band_nodatas.push_back(has_nd ? nd : 0);
        bind_data->band_has_nodata.push_back(has_nd != 0);

        GDALColorInterp ci = GDALGetRasterColorInterpretation(band);
        bind_data->band_color_interps.push_back(
            StringUtil::Lower(std::string(GDALGetColorInterpretationName(ci))));

        const char *desc = GDALGetDescription(band);
        bind_data->band_descriptions.push_back(desc ? desc : "");

        const char *unit = GDALGetRasterUnitType(band);
        bind_data->band_units.push_back(unit ? unit : "");

        int has_scale = 0;
        double scale = GDALGetRasterScale(band, &has_scale);
        bind_data->band_scales.push_back(scale);
        bind_data->band_has_scale.push_back(has_scale != 0 && scale != 1.0);

        int has_offset = 0;
        double offset = GDALGetRasterOffset(band, &has_offset);
        bind_data->band_offsets.push_back(offset);
        bind_data->band_has_offset.push_back(has_offset != 0 && offset != 0.0);

        // Color table — only meaningful for palette bands (renderers need RGBA per index).
        std::vector<std::array<int, 4>> entries;
        if (ci == GCI_PaletteIndex) {
            GDALColorTableH ct = GDALGetRasterColorTable(band);
            if (ct) {
                int n = GDALGetColorEntryCount(ct);
                entries.reserve(n);
                for (int i = 0; i < n; i++) {
                    const GDALColorEntry *e = GDALGetColorEntry(ct, i);
                    if (!e) continue;
                    entries.push_back({e->c1, e->c2, e->c3, e->c4});
                }
            }
        }
        bind_data->band_colortables.push_back(std::move(entries));

        // Stats. Two paths, gated on output_format:
        //
        //   format='v0'       — sample 1000 random pixels and compute the
        //                       v0.1.0 stats shape used by raster-loader
        //                       (count = sample size, plus quantiles/
        //                       top_values/version extensions for Builder).
        //
        //   format='v0.5.0'   — GDAL approx stats (overview-based, fast).
        //                       count derived from STATISTICS_VALID_PERCENT
        //                       × total pixels per the v0.5.0 shape.
        raquet::BandInfo::Stats st;
        if (bind_data->output_format == "v0") {
            auto v01 = raquet::compute_v01_band_stats(
                band,
                bind_data->raster_width, bind_data->raster_height,
                has_nd ? nd : 0.0, has_nd != 0,
                bind_data->gdal_dtype);
            if (v01.has_stats) {
                st.count         = v01.count;
                st.min           = v01.min;
                st.max           = v01.max;
                st.mean          = v01.mean;
                st.stddev        = v01.stddev;
                st.sum           = v01.sum;
                st.sum_squares   = v01.sum_squares;
                st.valid_percent = v01.valid_percent;
                st.approximated  = v01.approximated;
                st.has_stats     = true;
                st.version       = std::move(v01.version);
                st.quantiles     = std::move(v01.quantiles);
                st.top_values    = std::move(v01.top_values);
            }
        } else {
            double smin = 0, smax = 0, smean = 0, sstddev = 0;
            CPLErr err = GDALComputeRasterStatistics(
                band, bind_data->approx_stats ? TRUE : FALSE,
                &smin, &smax, &smean, &sstddev, nullptr, nullptr);
            if (err == CE_None) {
                const char *vp = GDALGetMetadataItem(band, "STATISTICS_VALID_PERCENT", nullptr);
                double valid_pct = vp ? std::stod(vp) : 100.0;
                int64_t total = static_cast<int64_t>(bind_data->raster_width) *
                                static_cast<int64_t>(bind_data->raster_height);
                int64_t count = static_cast<int64_t>(std::floor(valid_pct / 100.0 * total));
                st.count         = count;
                st.min           = smin;
                st.max           = smax;
                st.mean          = smean;
                st.stddev        = sstddev;
                st.sum           = smean * static_cast<double>(count);
                st.sum_squares   = (sstddev * sstddev + smean * smean) * static_cast<double>(count);
                st.valid_percent = valid_pct;
                st.approximated  = bind_data->approx_stats;
                st.has_stats     = true;
            }
        }
        bind_data->band_stats.push_back(st);
    }

    // CF time dimension extraction (NetCDF)
    {
        char **md = GDALGetMetadata(ds, nullptr);
        if (md) {
            const char *time_units = CSLFetchNameValue(md, "time#units");
            if (!time_units) {
                // Try alternate metadata keys
                for (int i = 0; md[i]; i++) {
                    std::string entry(md[i]);
                    auto eq = entry.find('=');
                    if (eq != std::string::npos) {
                        std::string key = entry.substr(0, eq);
                        if (key.find("units") != std::string::npos &&
                            StringUtil::Lower(key).find("time") != std::string::npos) {
                            time_units = md[i] + eq + 1;
                            break;
                        }
                    }
                }
            }
            if (time_units) {
                bind_data->has_cf_time = true;
                bind_data->cf_units_string = time_units;
                const char *cal = CSLFetchNameValue(md, "time#calendar");
                bind_data->cf_calendar = cal ? cal : "standard";

                // Extract time values from NETCDF_DIM_time_VALUES
                const char *tv = CSLFetchNameValue(md, "NETCDF_DIM_time_VALUES");
                if (tv) {
                    std::string vals(tv);
                    // Format: "{val1,val2,val3,...}"
                    size_t start = vals.find('{');
                    size_t end = vals.find('}');
                    if (start != std::string::npos && end != std::string::npos) {
                        std::string inner = vals.substr(start + 1, end - start - 1);
                        size_t pos = 0;
                        while (pos < inner.size()) {
                            size_t comma = inner.find(',', pos);
                            if (comma == std::string::npos) comma = inner.size();
                            try {
                                bind_data->cf_time_values.push_back(
                                    std::stod(inner.substr(pos, comma - pos)));
                            } catch (...) {}
                            pos = comma + 1;
                        }
                    }
                }
            }
        }
    }

    // CRS detection
    OGRSpatialReferenceH src_srs = nullptr;
    const char *proj_wkt = GDALGetProjectionRef(ds);
    bool src_is_wgs84 = false;
    if (proj_wkt && strlen(proj_wkt) > 0) {
        src_srs = OSRNewSpatialReference(proj_wkt);
        bind_data->src_wkt = proj_wkt;
    } else {
        // No CRS — assume WGS84
        src_srs = OSRNewSpatialReference(nullptr);
        OSRImportFromEPSG(src_srs, 4326);
        char *wkt = nullptr;
        OSRExportToWkt(src_srs, &wkt);
        bind_data->src_wkt = wkt;
        CPLFree(wkt);
        src_is_wgs84 = true;
    }
    OSRSetAxisMappingStrategy(src_srs, OAMS_TRADITIONAL_GIS_ORDER);

    // Detect well-known CRS to enable PROJ-free fast path
    bind_data->overview_count = GDALGetOverviewCount(first_band);
    if (!src_is_wgs84) {
        // Check via EPSG authority code
        const char *auth_name = OSRGetAuthorityName(src_srs, nullptr);
        const char *auth_code = OSRGetAuthorityCode(src_srs, nullptr);
        if (auth_name && auth_code && std::string(auth_name) == "EPSG") {
            int epsg = std::atoi(auth_code);
            if (epsg == 4326) {
                src_is_wgs84 = true;
            } else if (epsg == 3857 || epsg == 900913) {
                bind_data->src_is_web_mercator = true;
            }
        }
        // Fallback: check if geographic (likely 4326-compatible)
        if (!src_is_wgs84 && !bind_data->src_is_web_mercator && OSRIsGeographic(src_srs)) {
            OGRSpatialReferenceH wgs84_check = OSRNewSpatialReference(nullptr);
            OSRImportFromEPSG(wgs84_check, 4326);
            if (OSRIsSame(src_srs, wgs84_check)) {
                src_is_wgs84 = true;
            }
            OSRDestroySpatialReference(wgs84_check);
        }
    }

    // Fast path: WGS84 and Web Mercator use pure math (no PROJ database needed)
    if (src_is_wgs84 || bind_data->src_is_web_mercator) {
        double resolution;
        if (src_is_wgs84) {
            resolution = CalculateResolutionFromWGS84(ds);
            CalculateBoundsFromWGS84(ds, bind_data->bounds_minlon, bind_data->bounds_minlat,
                                     bind_data->bounds_maxlon, bind_data->bounds_maxlat);
        } else {
            resolution = CalculateResolutionFromMercator(ds);
            CalculateBoundsFromMercator(ds, bind_data->bounds_minlon, bind_data->bounds_minlat,
                                        bind_data->bounds_maxlon, bind_data->bounds_maxlat);
        }
        if (bind_data->max_zoom == 0) {
            bind_data->max_zoom = CalculateZoom(resolution, bind_data->block_zoom, bind_data->zoom_strategy);
            bind_data->max_zoom = std::max(0, std::min(26, bind_data->max_zoom));
        }
        if (bind_data->overviews == "none") {
            bind_data->min_zoom = bind_data->max_zoom;
        } else if (bind_data->min_zoom == 0) {
            bind_data->min_zoom = CalculateMinZoom(
                bind_data->bounds_minlon, bind_data->bounds_minlat,
                bind_data->bounds_maxlon, bind_data->bounds_maxlat,
                bind_data->max_zoom, bind_data->block_zoom);
        }
        OSRDestroySpatialReference(src_srs);
        GDALClose(ds);
    } else {
        // General path: use PROJ-based coordinate transformations
        OGRSpatialReferenceH merc_srs = OSRNewSpatialReference(nullptr);
        OSRImportFromEPSG(merc_srs, 3857);
        bind_data->src_is_web_mercator = OSRIsSame(src_srs, merc_srs);

        OGRSpatialReferenceH wgs84_srs = OSRNewSpatialReference(nullptr);
        OSRImportFromEPSG(wgs84_srs, 4326);
        OSRSetAxisMappingStrategy(wgs84_srs, OAMS_TRADITIONAL_GIS_ORDER);

        OGRCoordinateTransformationH tx3857 = OCTNewCoordinateTransformation(src_srs, merc_srs);
        OGRCoordinateTransformationH tx4326 = OCTNewCoordinateTransformation(src_srs, wgs84_srs);

        if (!tx3857 || !tx4326) {
            if (tx3857) OCTDestroyCoordinateTransformation(tx3857);
            if (tx4326) OCTDestroyCoordinateTransformation(tx4326);
            OSRDestroySpatialReference(src_srs);
            OSRDestroySpatialReference(merc_srs);
            OSRDestroySpatialReference(wgs84_srs);
            GDALClose(ds);
            throw IOException("Failed to create coordinate transformations for CRS. "
                              "If PROJ database is not available, use WGS84 (EPSG:4326) or "
                              "Web Mercator (EPSG:3857) source data which do not require PROJ.");
        }

        double resolution = CalculateResolution(ds, tx3857);
        if (bind_data->max_zoom == 0) {
            bind_data->max_zoom = CalculateZoom(resolution, bind_data->block_zoom, bind_data->zoom_strategy);
            bind_data->max_zoom = std::max(0, std::min(26, bind_data->max_zoom));
        }
        CalculateBounds(ds, tx4326, bind_data->bounds_minlon, bind_data->bounds_minlat,
                        bind_data->bounds_maxlon, bind_data->bounds_maxlat);
        if (bind_data->overviews == "none") {
            bind_data->min_zoom = bind_data->max_zoom;
        } else if (bind_data->min_zoom == 0) {
            bind_data->min_zoom = CalculateMinZoom(
                bind_data->bounds_minlon, bind_data->bounds_minlat,
                bind_data->bounds_maxlon, bind_data->bounds_maxlat,
                bind_data->max_zoom, bind_data->block_zoom);
        }

        OCTDestroyCoordinateTransformation(tx3857);
        OCTDestroyCoordinateTransformation(tx4326);
        OSRDestroySpatialReference(src_srs);
        OSRDestroySpatialReference(merc_srs);
        OSRDestroySpatialReference(wgs84_srs);
        GDALClose(ds);
    }

    // Define output schema
    // Column 1: block (UBIGINT) — QUADBIN cell ID
    names.push_back("block");
    return_types.push_back(LogicalType::UBIGINT);

    // Column 2: metadata (VARCHAR) — JSON metadata string
    names.push_back("metadata");
    return_types.push_back(LogicalType::VARCHAR);

    // Band columns
    if (bind_data->band_layout == "interleaved") {
        names.push_back("pixels");
        return_types.push_back(LogicalType::BLOB);
    } else {
        for (int b = 0; b < bind_data->raster_band_count; b++) {
            names.push_back("band_" + std::to_string(b + 1));
            return_types.push_back(LogicalType::BLOB);
        }
    }

    // Statistics columns (optional)
    if (bind_data->statistics) {
        for (int b = 0; b < bind_data->raster_band_count; b++) {
            std::string prefix = "band_" + std::to_string(b + 1) + "_";
            names.push_back(prefix + "count");  return_types.push_back(LogicalType::BIGINT);
            names.push_back(prefix + "min");    return_types.push_back(LogicalType::DOUBLE);
            names.push_back(prefix + "max");    return_types.push_back(LogicalType::DOUBLE);
            names.push_back(prefix + "sum");    return_types.push_back(LogicalType::DOUBLE);
            names.push_back(prefix + "mean");   return_types.push_back(LogicalType::DOUBLE);
            names.push_back(prefix + "stddev"); return_types.push_back(LogicalType::DOUBLE);
        }
    }

    // Estimate tile count for cardinality (enables DuckDB parallelism)
    auto est_tiles = EnumerateTiles(bind_data->bounds_minlon, bind_data->bounds_minlat,
                                     bind_data->bounds_maxlon, bind_data->bounds_maxlat,
                                     bind_data->max_zoom);
    bind_data->estimated_tiles = est_tiles.size() + 1; // +1 for metadata row

    bind_data->column_names = names;
    return std::move(bind_data);
}

// ─────────────────────────────────────────────
// Helper: Open GDAL dataset (with ASSUME_LONGLAT fallback)
// ─────────────────────────────────────────────
static GDALDatasetH OpenGDALDataset(const std::string &filename) {
    GDALAllRegister();
    GDALDatasetH ds = GDALOpen(filename.c_str(), GA_ReadOnly);
    if (!ds) {
        char **open_options = CSLSetNameValue(nullptr, "ASSUME_LONGLAT", "YES");
        ds = GDALOpenEx(filename.c_str(), GDAL_OF_RASTER | GDAL_OF_READONLY,
                        nullptr, const_cast<const char *const *>(open_options), nullptr);
        CSLDestroy(open_options);
    }
    return ds;
}

// ─────────────────────────────────────────────
// INIT GLOBAL
// ─────────────────────────────────────────────
static unique_ptr<GlobalTableFunctionState> ReadRasterInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<ReadRasterBindData>();
    auto state = make_uniq<ReadRasterGlobalState>();

    state->source_resampling = bind_data.resampling;
    state->has_overviews = (bind_data.min_zoom < bind_data.max_zoom);

    // Nodata from first band
    if (!bind_data.band_nodatas.empty() && bind_data.band_has_nodata[0]) {
        state->nodata_value = bind_data.band_nodatas[0];
        state->has_nodata = true;
    }

    // Cache Web Mercator WKT for local state initialization
    OGRSpatialReferenceH merc = OSRNewSpatialReference(nullptr);
    OSRImportFromEPSG(merc, 3857);
    char *wkt = nullptr;
    OSRExportToWkt(merc, &wkt);
    state->web_mercator_wkt_str = wkt;
    CPLFree(wkt);
    OSRDestroySpatialReference(merc);

    // Build native-zoom tile list (these can be processed in parallel)
    state->native_tiles = EnumerateTiles(
        bind_data.bounds_minlon, bind_data.bounds_minlat,
        bind_data.bounds_maxlon, bind_data.bounds_maxlat,
        bind_data.max_zoom);

    // Build overview frames if needed. Each thread will warp its share using
    // its own per-thread GDAL handle (local.src_ds), so no global handle is
    // opened here.
    if (state->has_overviews) {
        for (int z = bind_data.min_zoom; z < bind_data.max_zoom; z++) {
            auto tiles_at_zoom = EnumerateTiles(
                bind_data.bounds_minlon, bind_data.bounds_minlat,
                bind_data.bounds_maxlon, bind_data.bounds_maxlat, z);
            for (auto &tile : tiles_at_zoom) {
                OverviewFrame frame;
                frame.tile = tile;
                state->overview_frames.push_back(std::move(frame));
            }
        }
    }

    return std::move(state);
}

// ─────────────────────────────────────────────
// INIT LOCAL — per-thread GDAL handles
// ─────────────────────────────────────────────
static unique_ptr<LocalTableFunctionState> ReadRasterInitLocal(ExecutionContext &context,
                                                                TableFunctionInitInput &input,
                                                                GlobalTableFunctionState *global_state) {
    return make_uniq<ReadRasterLocalState>();
}

// ─────────────────────────────────────────────
// Helper: Emit a tile row into the output DataChunk
// ─────────────────────────────────────────────
static void EmitTileRow(DataChunk &output, idx_t row_count,
                         const ReadRasterBindData &bind_data,
                         uint64_t block, const TileData &tile_data) {
    idx_t col = 0;

    // block column
    FlatVector::GetData<uint64_t>(output.data[col])[row_count] = block;
    col++;

    // metadata column (NULL for data rows)
    FlatVector::SetNull(output.data[col], row_count, true);
    col++;

    // band data columns
    for (size_t b = 0; b < tile_data.compressed.size(); b++) {
        auto &blob = tile_data.compressed[b];
        auto str = StringVector::AddStringOrBlob(output.data[col],
            string_t(reinterpret_cast<const char *>(blob.data()), blob.size()));
        FlatVector::GetData<string_t>(output.data[col])[row_count] = str;
        col++;
    }

    // Statistics columns
    if (bind_data.statistics) {
        for (size_t b = 0; b < tile_data.stats.size(); b++) {
            auto &s = tile_data.stats[b];
            FlatVector::GetData<int64_t>(output.data[col])[row_count] = s.count;  col++;
            FlatVector::GetData<double>(output.data[col])[row_count] = s.min;     col++;
            FlatVector::GetData<double>(output.data[col])[row_count] = s.max;     col++;
            FlatVector::GetData<double>(output.data[col])[row_count] = s.sum;     col++;
            FlatVector::GetData<double>(output.data[col])[row_count] = s.mean;    col++;
            FlatVector::GetData<double>(output.data[col])[row_count] = s.stddev;  col++;
        }
    }
}

// ─────────────────────────────────────────────
// EXECUTE — two-phase: parallel native zoom, then single-thread overviews
// ─────────────────────────────────────────────
static void ReadRasterExecute(ClientContext &context, TableFunctionInput &data,
                               DataChunk &output) {
    auto &bind_data = data.bind_data->Cast<ReadRasterBindData>();
    auto &state = data.global_state->Cast<ReadRasterGlobalState>();
    auto &local = data.local_state->Cast<ReadRasterLocalState>();

    if (state.finished) {
        output.SetCardinality(0);
        return;
    }

    // Lazy-init per-thread GDAL handles
    if (!local.initialized) {
        local.src_ds = OpenGDALDataset(bind_data.filename);
        if (!local.src_ds) {
            throw IOException("Thread failed to open raster: %s", bind_data.filename);
        }
        local.gtiff_driver = GDALGetDriverByName("GTiff");
        OGRSpatialReferenceH srs = OSRNewSpatialReference(nullptr);
        OSRImportFromEPSG(srs, 3857);
        OSRExportToWkt(srs, &local.web_mercator_wkt);
        OSRDestroySpatialReference(srs);
        local.initialized = true;
    }

    idx_t row_count = 0;
    idx_t max_rows = STANDARD_VECTOR_SIZE;

    // ── Phase 1: Native-zoom tiles (parallel) ──
    while (row_count < max_rows) {
        // Grab next tile from shared queue
        idx_t my_idx;
        {
            std::lock_guard<std::mutex> lock(state.tile_mutex);
            if (state.next_tile_idx >= state.native_tiles.size()) {
                break; // No more native tiles
            }
            my_idx = state.next_tile_idx++;
        }

        auto &tile = state.native_tiles[my_idx];

        // Create tile dataset and warp
        std::string tile_path;
        GDALDatasetH tile_ds = CreateTileDataset(
            local.gtiff_driver, local.web_mercator_wkt,
            tile, bind_data.block_size,
            bind_data.raster_band_count, bind_data.gdal_dtype,
            state.nodata_value, state.has_nodata, tile_path);

        WarpIntoTile(local.src_ds, tile_ds, state.source_resampling,
                     state.nodata_value, state.has_nodata);

        bool empty = IsTileEmpty(tile_ds, state.nodata_value, state.has_nodata);

        if (!empty) {
            auto tile_data = ReadAndCompressBands(
                tile_ds, bind_data.compression, bind_data.compression_quality,
                bind_data.band_layout, bind_data.statistics,
                bind_data.raquet_dtype, state.has_nodata, state.nodata_value);

            uint64_t block = quadbin::tile_to_cell(tile.x, tile.y, tile.z);
            EmitTileRow(output, row_count, bind_data, block, tile_data);
            state.total_blocks++;
            row_count++;
        }

        GDALClose(tile_ds);
        VSIUnlink(tile_path.c_str());

        // Mark this tile as fully processed (emitted or skipped-empty)
        state.phase1_finished.fetch_add(1, std::memory_order_release);
    }

    // If we emitted rows from native tiles, return them
    if (row_count > 0) {
        output.SetCardinality(row_count);
        return;
    }

    // ── Phase 2 staging (parallel): every thread pulls overview frames from
    //    state.overview_frames via next_overview_idx and warps them with its
    //    own per-thread GDAL handle (local.src_ds). Non-empty results are
    //    pushed into state.overview_results under a brief mutex; the last
    //    thread to finish (overview_frames_processed == size) publishes
    //    phase2_staged. Threads that arrive after staging is already in
    //    progress simply join the work pull. Threads that arrive after
    //    staging is finished skip ahead to the drain block.
    if (!state.phase2_staged.load(std::memory_order_acquire)) {
        // One-shot Phase 2 init: wait for Phase 1 stragglers exactly once so
        // total_blocks reflects every emitted native tile before any thread
        // reads it. The init winner also short-circuits phase2_staged when
        // there are no overview frames at all.
        if (!state.phase2_init_done.load(std::memory_order_acquire)) {
            bool expected = false;
            if (state.phase2_init_claimed.compare_exchange_strong(expected, true)) {
                while (state.phase1_finished.load(std::memory_order_acquire) < state.native_tiles.size()) {
                    std::this_thread::yield();
                }
                if (state.overview_frames.empty()) {
                    // Nothing to stage; skip straight to metadata.
                    state.phase2_staged.store(true, std::memory_order_release);
                } else {
                    state.overview_results.reserve(state.overview_frames.size());
                }
                state.phase2_init_done.store(true, std::memory_order_release);
            } else {
                while (!state.phase2_init_done.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
            }
        }

        // Pull-and-warp loop. Every thread runs this; fetch_add hands out
        // non-overlapping work indices. When a thread can't get more work
        // it falls through; the thread that increments overview_frames_processed
        // to the total publishes phase2_staged.
        const idx_t total_frames = state.overview_frames.size();
        while (true) {
            idx_t my_idx = state.next_overview_idx.fetch_add(1, std::memory_order_acq_rel);
            if (my_idx >= total_frames) {
                break;
            }

            auto &frame = state.overview_frames[my_idx];

            std::string ovr_path;
            GDALDatasetH tile_ds = CreateTileDataset(
                local.gtiff_driver, local.web_mercator_wkt,
                frame.tile, bind_data.block_size,
                bind_data.raster_band_count, bind_data.gdal_dtype,
                state.nodata_value, state.has_nodata, ovr_path);

            // Try COG overview fast path: read directly from a source overview
            // level with a matching reduction factor instead of re-warping
            // from the base resolution. Geometrically valid for any source
            // CRS — the destination tile is in Web Mercator regardless of
            // source, and the warper reprojects from the chosen overview just
            // as it would from the base. The tolerance check on the reduction
            // factor is what validates suitability.
            bool used_cog = false;
            if (bind_data.overview_count > 0) {
                int zoom_diff = bind_data.max_zoom - frame.tile.z;
                int reduction_factor = 1 << zoom_diff;
                GDALRasterBandH src_band = GDALGetRasterBand(local.src_ds, 1);
                int src_xsize = GDALGetRasterBandXSize(src_band);
                for (int i = 0; i < bind_data.overview_count; i++) {
                    GDALRasterBandH ovr = GDALGetOverview(src_band, i);
                    if (ovr) {
                        int ovr_xsize = GDALGetRasterBandXSize(ovr);
                        double ovr_reduction = static_cast<double>(src_xsize) / ovr_xsize;
                        if (std::abs(ovr_reduction - reduction_factor) / reduction_factor < 0.1) {
                            WarpIntoTile(local.src_ds, tile_ds, GRA_NearestNeighbour,
                                         state.nodata_value, state.has_nodata, i);
                            used_cog = true;
                            break;
                        }
                    }
                }
            }

            if (!used_cog) {
                // Fallback: warp from base resolution
                WarpIntoTile(local.src_ds, tile_ds, GRA_Average,
                             state.nodata_value, state.has_nodata);
            }

            bool empty = IsTileEmpty(tile_ds, state.nodata_value, state.has_nodata);

            if (!empty) {
                auto tile_data = ReadAndCompressBands(
                    tile_ds, bind_data.compression, bind_data.compression_quality,
                    bind_data.band_layout, bind_data.statistics,
                    bind_data.raquet_dtype, state.has_nodata, state.nodata_value);

                uint64_t block = quadbin::tile_to_cell(frame.tile.x, frame.tile.y, frame.tile.z);
                {
                    std::lock_guard<std::mutex> lock(state.overview_results_mutex);
                    state.overview_results.push_back({block, std::move(tile_data)});
                }
                state.total_blocks++;
            }

            GDALClose(tile_ds);
            VSIUnlink(ovr_path.c_str());

            idx_t completed = state.overview_frames_processed.fetch_add(1, std::memory_order_acq_rel) + 1;
            if (completed >= total_frames) {
                // We finished the last overview tile — publish the staged queue.
                state.phase2_staged.store(true, std::memory_order_release);
            }
        }

        // After the work-pull loop: phase2_staged is either set already (we
        // were the last finisher, or another thread was) or still pending
        // because some sibling is still warping its current tile. In the
        // latter case yield and let DuckDB call us back; we'll fall through
        // to the drain on the next entry.
        if (!state.phase2_staged.load(std::memory_order_acquire)) {
            std::this_thread::yield();
            output.SetCardinality(0);
            return;
        }
        // Fall through to drain.
    }

    // ── Phase 2 drain: any thread can pull from the staged queue, capped at
    //    max_rows - 1 so there's still room for the metadata row in this chunk
    //    if the queue runs out here. Lock-free fetch_add: threads may
    //    over-increment past .size() (those return without emitting), which is
    //    fine — we only need >= size() to mean "drained" for the metadata gate.
    const idx_t overview_total = state.overview_results.size();
    while (row_count + 1 < max_rows) {
        idx_t my_idx = state.overview_drain_idx.fetch_add(1, std::memory_order_acq_rel);
        if (my_idx >= overview_total) {
            break;
        }
        auto &result = state.overview_results[my_idx];
        EmitTileRow(output, row_count, bind_data, result.block, result.tile_data);
        row_count++;
    }

    // If the overview queue still has tiles to drain, leave metadata for a
    // later Execute call.
    if (state.overview_drain_idx.load(std::memory_order_acquire) < overview_total) {
        output.SetCardinality(row_count);
        return;
    }

    // ── Phase 3: Emit metadata row (exactly once, thread-safe) ──
    bool expected = false;
    if (state.metadata_emitted.compare_exchange_strong(expected, true)) {
        // Build metadata
        raquet::RaquetMetadata meta;
        meta.file_format = "raquet";
        meta.crs = "EPSG:3857";
        meta.compression = bind_data.compression;
        meta.compression_quality = bind_data.compression_quality;
        meta.band_layout = bind_data.band_layout;
        meta.scheme = "quadbin";
        meta.block_width = bind_data.block_size;
        meta.block_height = bind_data.block_size;
        meta.min_zoom = bind_data.min_zoom;
        meta.max_zoom = bind_data.max_zoom;
        meta.pixel_zoom = bind_data.max_zoom +
            static_cast<int>(std::log2(bind_data.block_size) * 2);
        meta.num_blocks = state.total_blocks;
        meta.bounds_minlon = bind_data.bounds_minlon;
        meta.bounds_minlat = bind_data.bounds_minlat;
        meta.bounds_maxlon = bind_data.bounds_maxlon;
        meta.bounds_maxlat = bind_data.bounds_maxlat;
        meta.width = bind_data.raster_width;
        meta.height = bind_data.raster_height;

        // Tile statistics metadata
        if (bind_data.statistics) {
            meta.tile_statistics = true;
            meta.tile_statistics_columns = {"count", "min", "max", "sum", "mean", "stddev"};
        }

        // CF time metadata
        if (bind_data.has_cf_time) {
            meta.has_time = true;
            meta.time_cf_units = bind_data.cf_units_string;
            meta.time_calendar = bind_data.cf_calendar;
        }

        // Band info with extended metadata
        for (int b = 0; b < bind_data.raster_band_count; b++) {
            raquet::BandInfo bi;
            bi.name = "band_" + std::to_string(b + 1);
            bi.type = bind_data.raquet_dtype;
            if (bind_data.band_has_nodata[b]) {
                bi.nodata = bind_data.band_nodatas[b];
                bi.has_nodata = true;
            }
            if (b < static_cast<int>(bind_data.band_descriptions.size())) {
                bi.description = bind_data.band_descriptions[b];
            }
            if (b < static_cast<int>(bind_data.band_units.size())) {
                bi.unit = bind_data.band_units[b];
            }
            if (b < static_cast<int>(bind_data.band_color_interps.size())) {
                bi.colorinterp = bind_data.band_color_interps[b];
            }
            if (b < static_cast<int>(bind_data.band_has_scale.size()) && bind_data.band_has_scale[b]) {
                bi.scale = bind_data.band_scales[b];
                bi.has_scale = true;
            }
            if (b < static_cast<int>(bind_data.band_has_offset.size()) && bind_data.band_has_offset[b]) {
                bi.offset = bind_data.band_offsets[b];
                bi.has_offset = true;
            }
            if (b < static_cast<int>(bind_data.band_colortables.size())) {
                bi.colortable = bind_data.band_colortables[b];
                bi.has_colortable = !bi.colortable.empty();
            }
            if (b < static_cast<int>(bind_data.band_stats.size())) {
                bi.stats = bind_data.band_stats[b];
            }
            meta.band_info.push_back(bi);
            meta.bands.push_back({bi.name, bi.type});
        }

        std::string metadata_json = (bind_data.output_format == "v0")
            ? meta.to_json_v0() : meta.to_json();

        // Emit metadata row: block=0, metadata=json, bands=NULL
        idx_t col = 0;
        FlatVector::GetData<uint64_t>(output.data[col])[row_count] = 0;
        col++;

        auto meta_str = StringVector::AddString(output.data[col], metadata_json);
        FlatVector::GetData<string_t>(output.data[col])[row_count] = meta_str;
        col++;

        // Band columns are NULL for metadata row
        int num_band_cols = (bind_data.band_layout == "interleaved") ? 1 : bind_data.raster_band_count;
        for (int b = 0; b < num_band_cols; b++) {
            FlatVector::SetNull(output.data[col], row_count, true);
            col++;
        }

        // Stats columns are NULL for metadata row
        if (bind_data.statistics) {
            for (int b = 0; b < bind_data.raster_band_count * 6; b++) {
                FlatVector::SetNull(output.data[col], row_count, true);
                col++;
            }
        }

        row_count++;
        state.finished = true;
    }

    output.SetCardinality(row_count);
}

// ─────────────────────────────────────────────
// CARDINALITY — tell DuckDB how many rows to expect (enables parallelism)
// ─────────────────────────────────────────────
static unique_ptr<NodeStatistics> ReadRasterCardinality(ClientContext &context,
                                                         const FunctionData *bind_data_p) {
    auto &bind_data = bind_data_p->Cast<ReadRasterBindData>();
    return make_uniq<NodeStatistics>(bind_data.estimated_tiles);
}

// ─────────────────────────────────────────────
// REGISTRATION
// ─────────────────────────────────────────────
void RegisterReadRaster(ExtensionLoader &loader) {
    TableFunction func("read_raster", {LogicalType::VARCHAR},
                       ReadRasterExecute, ReadRasterBind, ReadRasterInitGlobal, ReadRasterInitLocal);

    func.named_parameters["compression"] = LogicalType::VARCHAR;
    func.named_parameters["resampling"] = LogicalType::VARCHAR;
    func.named_parameters["block_size"] = LogicalType::INTEGER;
    func.named_parameters["max_zoom"] = LogicalType::INTEGER;
    func.named_parameters["min_zoom"] = LogicalType::INTEGER;
    func.named_parameters["overviews"] = LogicalType::VARCHAR;
    func.named_parameters["band_layout"] = LogicalType::VARCHAR;
    func.named_parameters["quality"] = LogicalType::INTEGER;
    func.named_parameters["statistics"] = LogicalType::BOOLEAN;
    func.named_parameters["zoom_strategy"] = LogicalType::VARCHAR;
    func.named_parameters["format"] = LogicalType::VARCHAR;
    func.named_parameters["approx"] = LogicalType::BOOLEAN;
    func.cardinality = ReadRasterCardinality;

    loader.RegisterFunction(func);
}

} // namespace duckdb

#endif // RAQUET_HAS_GDAL
