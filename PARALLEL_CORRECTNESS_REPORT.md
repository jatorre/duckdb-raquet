# `read_raster()` Parallel Correctness and Performance Report

**Branch:** [`fix/parallel-correctness`](https://github.com/cayetanobv/duckdb-raquet/tree/fix/parallel-correctness) (5 commits ahead of `jatorre/duckdb-raquet@931b5ff`)
**Tip commit:** `5029627`
**Authored:** cayetano@cartodb.com (paired with Claude Opus 4.7)
**Period:** 2026-04-27 → 2026-04-30
**Test hardware:** 16-core / 31 GB RAM Linux box (Ubuntu 22.04, kernel 5.14)
**Test source:** `classification_germany.tif` (473 MB, EPSG:32632, 64,293 × 86,760 px, 1 band uint8, 5 internal overview levels)

---

## TL;DR

On the source raster above (the production-equivalent failure case), the unmodified `main` branch:

- **Crashes after ~30 minutes** with `double free or corruption (!prev)` (SIGSEGV, exit code 139)
- Produces **0 bytes** of output Parquet

The same query on this branch:

- **Completes in 8 min 44 s**
- Produces a valid 354 MB Parquet with all **53,423** tiles (z=4..13 full pyramid)
- `metadata.num_blocks` matches the actual data row count

Headline numbers (Germany full pyramid, `SET threads=16; SET preserve_insertion_order=false`):

| Variant | Wall | Result |
|---|---|---|
| Upstream `main` (`931b5ff`) | 30:14 | **SIGSEGV**, no output |
| `fix/parallel-correctness` | **8:44** | clean exit, 354 MB Parquet, 53,423 tiles |

The branch fixes three correctness bugs and one perf cliff that, together, made the extension unusable for any GeoTIFF whose reprojected pyramid produced more than `STANDARD_VECTOR_SIZE` (= 2048) overview tiles. That covers virtually every realistic country-scale or higher-resolution raster.

---

## Background

`read_raster()` converts a GDAL-readable raster into RaQuet (Parquet + QUADBIN). It works in three logical phases:

1. **Phase 1 — native zoom.** Every tile at `max_zoom` is warped from the source into a 256×256 (or 512×512) Web-Mercator destination, gzipped, and emitted as one row of the output Parquet. Phase 1 has been parallel since before this branch (each thread pulls tile indices off a shared mutex-protected queue).
2. **Phase 2 — overview pyramid.** Every tile from `min_zoom` up to `max_zoom-1` is warped at the appropriate reduction. Before this branch, Phase 2 ran on a single thread; the rest of the worker pool was kept out by an exclusive `post_native_claimed` gate.
3. **Phase 3 — metadata.** Exactly one row is emitted at `block=0` containing the JSON RaQuet metadata.

Bugs uncovered during this investigation cluster around the Phase 2 / Phase 3 transition.

---

## The 5 commits

Each commit is independently reviewable and addresses one bug. They build on each other but each one is correct standalone — the maintainer can cherry-pick selectively.

### 1. `8a925ee` — `num_blocks` race under parallel execution

**Problem.** The post-native gate winner emits `metadata.num_blocks = state.total_blocks`, but `total_blocks` is incremented inside the Phase 1 inner loop (`read_raster.cpp:1167` originally). When DuckDB schedules multiple workers (`preserve_insertion_order=false`), thread A may drain the shared tile queue and reach the gate while thread B is still processing its last in-flight tile and has not yet executed its `state.total_blocks++`. The metadata row therefore under-reports the actual count.

**Reproducer (pre-fix):** Berlin `classification_germany_berlin.tif` (1.6 MB) at `min_zoom=max_zoom=13`, parallel mode → 160 actual data rows, but `metadata.num_blocks = 146`.

**Fix.** Add `std::atomic<idx_t> phase1_finished{0}` to global state. Each Phase 1 thread increments it after fully processing its tile (emit-or-skip). The gate winner spin-waits on `phase1_finished == native_tiles.size()` before reading `total_blocks`. Acquire/release ordering on the counter matches the implicit happens-before users expect.

Files: `src/raster/read_raster.cpp` (+16 lines)

### 2. `f1ce7d6` — `OVERVIEW_LEVEL` plumbing in `WarpIntoTile`

**Problem.** `WarpIntoTile()` accepts an `overview_level` argument intended to make the COG-overview fast path read directly from a chosen source overview level instead of re-warping from the base. The hint was being written into a local `warp_options_list` (`OVERVIEW_LEVEL`-keyed) that was created, used for nothing, and `CSLDestroy`-ed at the end. Two compounding issues:

1. The list was never assigned to `wo->papszWarpOptions`, so even if `OVERVIEW_LEVEL` had been a valid `GDALWarpOptions` key, it would not have been seen by anything.
2. `OVERVIEW_LEVEL` is not a `GDALWarpOptions` key in any GDAL version; the correct home is `SRC_OVERVIEW_LEVEL` on the *transformer* (`GDALCreateGenImgProjTransformer2`'s options dict), not on the warp itself. The v1 `GDALCreateGenImgProjTransformer` the code was using does not accept options at all.

**Effect of bug.** Both branches of the Phase 2 inner loop ("fast path" and "fallback") were doing the exact same warp from the source's full-resolution band, differing only in resampling algorithm.

**Fix.** Switch to `GDALCreateGenImgProjTransformer2` and pass `SRC_OVERVIEW_LEVEL=N` on its options list. Drop the dead `warp_options_list`.

In practice, GDAL was *also* auto-selecting an overview when none was specified — so this bug was producing correct (but inefficient) output. The fix is mostly cosmetic for perf, but it makes the code actually do what the comments said it did.

Files: `src/raster/read_raster.cpp` (+8 / -10 lines)

### 3. `da8ff95` — Phase 2 silent row-cap drop and 2049 buffer overflow

This is the most consequential bug.

**Problem 1 (silent data loss).** Phase 2 emitted overview tiles directly into the output `DataChunk` in a single `Execute` call, capped at `STANDARD_VECTOR_SIZE` rows:

```cpp
// Old code, Phase 2 inner loop:
if (!empty && row_count < max_rows) {
    // emit
}
// ... continues iterating, doing GDAL warp + IsTileEmpty for tiles past the cap,
// then silently discarding them.
```

After the loop, `state.overviews_built = true` is set unconditionally. The post-native gate (`compare_exchange_strong`) is one-shot, so on subsequent `Execute` calls the gate fails and Phase 2 is never re-entered to recover the dropped tiles. For Germany at default `min_zoom..max_zoom` (~13k overview tiles), about **11k tiles were silently lost** — the warp work was done but the result discarded.

**Problem 2 (buffer overrun → SIGSEGV).** When Phase 2 emitted exactly `max_rows` tiles, Phase 3 still proceeded to write the metadata row at index `row_count` (= `max_rows`), incremented to `max_rows + 1`, and called `output.SetCardinality(max_rows + 1)`. That index is one past the allocated `STANDARD_VECTOR_SIZE` per-vector buffer — undefined behavior. In practice this corrupts allocator metadata; on the Germany full-pyramid run this manifests as `double free or corruption (!prev)` from glibc after roughly 30 minutes (the time it takes to finish all the wasted warps).

**Fix.** Restructure Phase 2 emission as a stage-then-drain pipeline.

- New `OverviewResult { uint64_t block; TileData tile_data; }` struct.
- New `state.overview_results` vector populated by the gate winner during Phase 2 staging (no emission inline).
- New `phase2_staged` atomic bool, published with release semantics after the staging loop.
- Drain loop runs in subsequent `Execute` calls (and in the gate winner's same call after staging). Drain is lock-free via `std::atomic<idx_t> overview_drain_idx`; threads that over-increment past `.size()` simply break out, which is fine — the only invariant the metadata gate needs is "drain index has reached size()".
- Drain caps at `row_count + 1 < max_rows` so there is always room for the metadata row in the same chunk. This eliminates the 2049 overflow.
- Phase 3 metadata only fires once the queue is drained; still gated by the existing `metadata_emitted` CAS so it emits exactly once across the worker pool.

Validated on Berlin (160 native + 72 overview = 232 tiles, both default and parallel; `num_blocks` matches in all cases) and on Germany Phase-1-only (39,676 tiles), Germany z=12..13 (49,805 tiles), and the full pyramid (53,423 tiles).

Files: `src/raster/read_raster.cpp` (+127 / -76 lines)

### 4. `badcc5d` — Lift the WebMercator gate on the COG fast path

**Problem.** The Phase 2 inner loop's COG-overview fast path was gated by `bind_data.src_is_web_mercator`. For UTM (and any other non-WM) source CRS, this was always false, so the loop fell through to the `WarpIntoTile(GRA_Average)` fallback every time — even when the source had a perfectly usable internal overview pyramid. The reduction-factor tolerance check immediately below the gate (`std::abs(ovr_reduction - reduction_factor) / reduction_factor < 0.1`) is what actually validates suitability of an overview level for the requested zoom; the source CRS is irrelevant, because the destination tile is in Web Mercator either way and the warper reprojects from the chosen overview just as it would from the base raster.

**Fix.** Drop the `src_is_web_mercator` precondition. With commit `f1ce7d6` having actually wired `SRC_OVERVIEW_LEVEL` through, the fast path now works for all source CRSes that have internal overviews.

In practice this had only a small measured perf impact on Germany because GDAL was already auto-selecting an appropriate overview internally. It is still a real correctness fix in that the code now does what its comments and structure say it does — and it stops calling `GRA_Average` (a resampling alg meant for downsample-from-base) on a tile that wasn't being sampled from base.

Files: `src/raster/read_raster.cpp` (+13 / -2 lines)

### 5. `5029627` — Parallelize Phase 2 across worker threads

**Problem.** Even with the staging refactor in commit `da8ff95`, the Phase 2 staging itself was still single-threaded — the post-native gate let exactly one thread do all overview warping. With 13k+ overview tiles for the Germany full pyramid, this was the dominant cost (roughly 10:47 of the 14:20 pre-Patch-C parallel run on z=12..13 alone).

**Fix.** Replace the exclusive gate with a queue-based parallel work pull.

- `state.next_overview_idx` (atomic): work pull pointer.
- `state.overview_frames_processed` (atomic): completion counter.
- One-shot `phase2_init_*` pair handles the "wait for Phase 1 stragglers" step exactly once across all workers; it also short-circuits `phase2_staged` when there are no overview frames.
- Each worker pulls indices via `fetch_add`, warps using its own per-thread `local.src_ds` GDAL handle (already opened lazily for Phase 1), and pushes non-empty results into `overview_results` under a brief mutex.
- The thread that increments `overview_frames_processed` to total publishes `phase2_staged`.
- The state-level GDAL handles for Phase 2 (`overview_src_ds`, `overview_driver`, `overview_wkt`, `overview_srs`) are removed; each thread reuses its existing per-thread Phase 1 handle for overview warps. The destructor and `InitGlobal` are simplified accordingly.

Files: `src/raster/read_raster.cpp` (+134 / -124 lines)

---

## Measured impact

All measurements on this branch's container-built binary (Docker image based on `ghcr.io/osgeo/gdal:ubuntu-small-3.10.3` to match `vcpkg.json`'s baseline GDAL version) on the 16-core box.

### End-to-end (Germany full pyramid, the production-equivalent query)

| Variant | Wall | Output | Status |
|---|---|---|---|
| Upstream main (`931b5ff`, community extension v0.2.3) | **30:14** | 0 bytes | **SIGSEGV** (`double free or corruption`) |
| `fix/parallel-correctness` | **8:44** | 354 MB / 53,423 tiles | clean |

### Smaller / bounded queries (for control)

| Source | Query | Mode | Wall | Tiles |
|---|---|---|---|---|
| Berlin (1.6 MB) | Phase 1 only (z=13) | default | 2.46 s | 160 |
| Berlin | Phase 1 only | parallel | 2.95 s | 160 |
| Berlin | Full pyramid (z=9..13) | default | 15.80 s | 232 |
| Berlin | Full pyramid | parallel | **4.47 s** | 232 |
| Germany | Phase 1 only (z=13) | default | 15:07 | 39,676 |
| Germany | Phase 1 only | parallel | **3:33** | 39,676 |
| Germany | z=12..13 | parallel (pre-Patch-C) | 14:20 | 49,805 |
| Germany | z=12..13 | parallel (with Patch C) | **10:34** | 49,805 |
| Germany | full pyramid (z=4..13) | parallel (with all 5 patches) | **8:44** | 53,423 |

Correctness check in every variant: `metadata.num_blocks` equals the actual count of `block != 0` data rows.

### Phase decomposition (Germany, parallel mode, all patches)

```
Total wall:                      8:44
├─ Phase 1 (39,676 tiles):       ~3:33   ~10-12 cores busy
├─ Phase 2 (~13,747 tiles):      ~5:11   ~12 cores busy
└─ Drain + metadata + Parquet:   <30s
```

CPU utilization across the full run averaged ~11.4 cores busy out of 16 (1139 % `time -P`).

---

## Why we can't go significantly faster from here

The headline 8:44 is dominated by GDAL-internal serialization that the extension cannot directly affect.

### Per-tile cost in Phase 2 (single-tile timeline, on-CPU portions)

```
~5-15 ms   GDALCreateGenImgProjTransformer2  (PROJ context init, geotransform calc)
~150 ms    ChunkAndWarpImage                  (the actual reproject + sample)
~2 ms      ReadAndCompressBands               (gzip 65 KB → ~13 KB compressed)
~0.5 ms    IsTileEmpty                        (scan dest)
~0.5 ms    CreateTileDataset                  (VSIMEM alloc)
~0.5 ms    GDALClose + VSIUnlink              (cleanup)
+ scheduling / atomics / mutex push  ~negligible
```

About **85 % of per-tile time is inside GDAL** — specifically inside `ChunkAndWarpImage` (the reprojection + pixel sampling) and the per-tile transformer init.

### Why we top out at ~12 cores busy and not 16

Three serializing points, all upstream:

1. **GDAL process-global block cache mutex.** GDAL maintains a single block cache for the entire process; every block fault during a warp acquires this mutex. With 12 threads concurrently reading from the same overview level, contention rises. There is no per-thread block cache in GDAL — the mutex is the design. Disabling the cache (`GDAL_CACHEMAX=0`) re-reads from disk and is strictly worse.
2. **PROJ database access.** Each transformer initializes a PROJ context which reads from the embedded `proj.db`. PROJ has internal locks around its database and grid access. With 12 threads creating transformers, this contributes to the per-tile transformer-init time variance.
3. **`GDALAllRegister` / driver registry locks.** Less frequent (per-thread, per-`OpenGDALDataset`), but still global.

### What is **not** a bottleneck

- Disk / memory bandwidth — the 473 MB source fits in OS page cache after the first read.
- Our `overview_results` push mutex — nanoseconds per tile, ~13k tiles, single-digit ms total.
- Atomic counters (`next_overview_idx`, `overview_frames_processed`, `phase1_finished`) — uncontended `fetch_add` is sub-µs.
- DuckDB pipeline / scheduling overhead — minimal at this scale.

---

## Next steps to improve performance

Ranked by impact-per-effort, with the cheap wins first.

### Tier 1 — small, safe, well-defined (minutes to hours each)

1. **Replace the worker yield-loop with a condition variable.** Idle threads currently spin on `std::this_thread::yield()` while waiting for Phase-1 stragglers and during the `phase2_init_claimed` CAS contention window. On a 16-core run, ~3 spinning threads × ~12 % CPU each ≈ ~40 % wasted CPU. Wall time changes little; CPU/energy use drops materially and a sharp hot spot in CPU profiles disappears. Trivial change (~30-50 lines): replace the two `while(yield)` loops with `std::condition_variable::wait` and notify when the predicate flips.

2. **Reuse the warp transformer across tiles when source is constant.** Each tile currently builds a fresh `GDALCreateGenImgProjTransformer2` (5-15 ms each). The source dataset and source/dest CRS are constant for the whole query; only the destination geotransform varies. With a per-thread cached transformer reset per-tile via `GDALSetGenImgProjTransformerDstGeoTransform`, the per-tile init cost can drop to ~1 ms. Estimated saving: **10-20 %** on Phase 2 wall time. ~80-120 lines, careful around per-thread lifecycle and cleanup.

### Tier 2 — substantial change, real win for sparse shapes

3. **Pre-filter empty bbox tiles before warping.** Phase 1 emits 39,676 tiles for Germany, but the bbox enumeration produces ~53k bbox tiles before empty-check. Every bbox tile is currently warped, then `IsTileEmpty` decides whether to emit. The fix is to pre-compute a coarse Web-Mercator "data presence" mask at init time (one low-res warp from the lowest available source overview) and use it to skip the per-tile warp + IsTileEmpty entirely for tiles whose bbox doesn't intersect any source-pixel-with-data. Estimated saving: **15-25 %** on Phase 1 for sparse shapes; near-zero for full-rectangle sources. Real implementation cost: ~150-300 lines including new global-state fields, a one-time low-res warp pipeline, per-tile mask lookup integrated into both Phase 1 and Phase 2 loops, plus design choices around mask resolution and source-CRS handling. **Defer to a dedicated branch** — too big to bundle with the cheap wins above, and impact is shape-dependent rather than universal.

### Tier 3 — large-scale changes, larger payoff

4. **Pre-warp the source ONCE to a Web-Mercator intermediate, then sample tiles from it.** This is the architectural win that unlocks 3-5× more speedup, by eliminating per-tile reprojection entirely. The cost is a one-time intermediate that must be sized to fit the destination zoom. For Germany at z=13 native, that's roughly 5 GB uncompressed in memory — too much for many production environments. Could be made workable by:
   - Tiling the intermediate on disk and memory-mapping it
   - Doing the pre-warp in chunks at lower zooms first (overview pyramid gets cheap)
   - Falling back to per-tile warp when the intermediate would exceed a memory budget
   This is the right shape for a future v0.3 of the extension but it's a big patch (probably ~500 lines and needs careful memory handling).

5. **Per-thread GDAL block cache (upstream GDAL change).** The single biggest non-extension win available. Would require a proposal and PR against GDAL itself. If accepted, it would lift the 12-cores-busy ceiling on this hardware to closer to 16. Long timeline, low probability of acceptance without a strong external sponsor.

### Tier 4 — considered and rejected

- **`compression='zstd'` as the default.** zstd is faster than gzip on large blobs, but raquet tiles are ~13 KB compressed (gzip's strong zone). Compression is already only ~5-10 % of per-tile cost, so the absolute wall-time saving is small (<2 % on Germany). More importantly, the deck.gl carto raster-tile decoder (`modules/carto/src/layers/schema/carto-raster-tile.ts`) types `compression` as `null | 'gzip'` — switching the server default would silently break every browser-side tile reader. zstd remains an opt-in `compression='zstd'` parameter for server-side / DuckDB-only pipelines.
- **In-place gzip compression in `ReadAndCompressBands`.** The pipeline reads each band once via `GDALRasterIO` into `raw_bands[b]`, then gzips into a separate output buffer. There is no double-copy to eliminate, and gzip cannot compress in-place (output buffer ≠ input buffer by zlib's design). The "5-8 % saving" originally claimed isn't recoverable here.
- **Inner-warp threading at low zoom (`GDAL_NUM_THREADS=ALL_CPUS` for tiny pyramid bottoms).** Niche; only helps at z≤6 where there are fewer tiles than cores. Implementation has to detect that regime and gate it. Marginal value on Germany; not worth the complexity until something else makes it the bottleneck.
- **`GDAL_NUM_THREADS=ALL_CPUS` with current outer parallelism.** Would over-subscribe CPU at 12 outer threads × N inner. Likely negative.
- **Custom warp implementation.** Reproducing GDAL's reprojection pipeline ourselves is many engineer-years and won't beat GDAL on accuracy.

---

## Realistic perf ceiling on this hardware/source

Tier 1 (#1 + #2) lands in this branch / a follow-up; Tier 2 (#3) lands in a dedicated branch. Together they could plausibly get Germany full pyramid from 8:44 down to roughly **6:00-6:30** (transformer reuse is the biggest of the three, pre-filter is shape-dependent, condvar is energy-only). Below that requires either Tier 3 (pre-warp architecture rewrite) or upstream GDAL changes. The current 8:44 is not a wall we hit because of bad code; it is the floor where GDAL's design forces single-threaded sections to dominate once outer parallelism saturates the block-cache mutex.

---

## Build environment

The whole build is containerized to avoid touching the host system, while still producing a binary that closely matches the production-deployed extension. The setup lives in a sibling directory at `~/dev_projs/cartolibs/duckdb-raquet-build/`, which contains:

```
duckdb-raquet-build/
├── Dockerfile        # GDAL 3.10.3 base + build deps + DuckDB CLI 1.5.2
└── build.sh          # wrapper: builds image once, mounts repo, builds extension
```

This directory is **outside** the source repo on purpose so the build artefacts and helper scripts don't pollute the upstream tree.

### Why a custom Docker image (and why GDAL 3.10.3)

The duckdb-raquet build has a heavy dependency surface. Its `vcpkg.json` lists GDAL (with `geos` + `network` features), PROJ, OpenSSL, curl, zlib, libjpeg-turbo, libwebp, and SQLite3. The community-extension build that ships to production resolves these via vcpkg's pinned baseline `ce613c41…`, which currently locks GDAL to **3.10.3**.

A direct `vcpkg install` for that dependency set on a fresh machine takes 2-3 hours of pure compile time (most of it spent on GDAL+PROJ from source). To skip that:

- The base image `ghcr.io/osgeo/gdal:ubuntu-small-3.10.3` from the OSGeo project ships a known-good GDAL 3.10.3 with PROJ 9.x, both built from source, with development headers and CMake config files installed in `/usr` (`gdal-config` at `/usr/bin/gdal-config`, CMake config at `/usr/lib/x86_64-linux-gnu/cmake/gdal/`). That's exactly what `find_package(GDAL CONFIG)` in the extension's `CMakeLists.txt` needs.
- We then just `apt install` the smaller leaves of the dependency graph (zlib, libjpeg, libwebp, libsqlite3) for the final link.

The result: the first image build takes ~3-5 minutes (mostly the apt install on top of the cached GDAL base layer), and a cold extension build takes ~20-30 min on a 16-core box. Subsequent extension builds against the same image are incremental and finish in 1-3 minutes for small source changes.

### `Dockerfile` (annotated)

```dockerfile
# duckdb-raquet-build/Dockerfile
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.10.3

ENV DEBIAN_FRONTEND=noninteractive

# Smaller deps not in the GDAL base image.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        curl \
        git \
        ninja-build \
        pkg-config \
        python3 \
        libsqlite3-dev \
        libwebp-dev \
        libjpeg-dev \
        zlib1g-dev \
        unzip \
        ca-certificates \
        time \
    && rm -rf /var/lib/apt/lists/*

# DuckDB 1.5.2 CLI (matches the bundled DuckDB submodule), so we can LOAD
# the freshly-built .duckdb_extension and run benchmarks inside this same
# image — important because the extension is dynamically linked against
# libgdal.so.36 from the GDAL 3.10.3 base layer, which only exists here.
RUN curl -fsSL -o /tmp/duckdb.zip \
        https://github.com/duckdb/duckdb/releases/download/v1.5.2/duckdb_cli-linux-amd64.zip \
    && unzip -q /tmp/duckdb.zip -d /usr/local/bin/ \
    && rm /tmp/duckdb.zip \
    && /usr/local/bin/duckdb --version

WORKDIR /workspace
# Source tree is mounted at /workspace at run time.
CMD ["bash", "-c", "make release"]
```

### `build.sh` (the wrapper)

```bash
#!/usr/bin/env bash
# Build the duckdb-raquet extension inside the docker image, then leave the
# .duckdb_extension on the host under build/release/extension/raquet/.
set -euo pipefail

REPO="${RAQUET_SRC:-/home/cayetano/dev_projs/cartolibs/duckdb-raquet}"
IMAGE="raquet-build:gdal-3.10.3"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# First-call build of the image; reused after.
if ! docker image inspect "${IMAGE}" >/dev/null 2>&1; then
  docker build -t "${IMAGE}" "${HERE}"
fi

# Run as the host user so the host build/ directory ends up host-owned.
docker run --rm \
  --user "$(id -u):$(id -g)" \
  -v "${REPO}:/workspace" \
  "${IMAGE}" \
  bash -c "make release 2>&1 | tail -200"
```

The script supports a few modes:

| Invocation | Effect |
|---|---|
| `./build.sh` | incremental release build (default) |
| `./build.sh clean` | wipe `build/` on host, then build |
| `./build.sh shell` | drop into a bash shell inside the container with the source mounted |
| `./build.sh -- make test` | run an arbitrary make target inside the container |

The host's `build/` directory persists across container runs, so CMake's incremental builds work normally.

### One-shot setup on a fresh machine

```bash
git clone https://github.com/cayetanobv/duckdb-raquet.git \
  ~/dev_projs/cartolibs/duckdb-raquet
cd ~/dev_projs/cartolibs/duckdb-raquet
git fetch origin fix/parallel-correctness && git checkout fix/parallel-correctness
git submodule update --init --recursive   # ~500 MB DuckDB submodule + extension-ci-tools

mkdir -p ~/dev_projs/cartolibs/duckdb-raquet-build
# (drop in the Dockerfile and build.sh from above)
chmod +x ~/dev_projs/cartolibs/duckdb-raquet-build/build.sh

~/dev_projs/cartolibs/duckdb-raquet-build/build.sh
# First call: image build (3-5 min) + cold extension build (~25 min on 16 cores)
# Output: build/release/extension/raquet/raquet.duckdb_extension (~20 MB)
```

### Iteration loop while developing

```bash
# Edit a file under src/raster/...
# Then rebuild incrementally:
~/dev_projs/cartolibs/duckdb-raquet-build/build.sh
# Typical incremental build for one source file: 1-3 min
```

### Known build wart

The `make release` target builds a downstream DuckDB tool, `plan_serializer`, that has a stale link line missing `-lproj` once our extension (which embeds PROJ via `proj_embed.cpp`) is in the link graph. The error looks like:

```
/usr/bin/ld: ../extension/raquet/libraquet_extension.a(proj_embed.cpp.o):
  undefined reference to symbol 'proj_context_set_database_path'
```

This **does not affect the extension build** — the `.duckdb_extension` artefact is produced before `plan_serializer` is linked. The `make` exit code is non-zero, but `build/release/extension/raquet/raquet.duckdb_extension` exists and is correct. The wrapper script `tail`s the last 200 lines of output so this is visible. A fix for that link line probably belongs upstream in `extension-ci-tools`.

---

## How to reproduce the benchmarks

### Run the headline benchmark inside the build container

```bash
docker run --rm \
  -v /home/cayetano/dev_projs/cartolibs/duckdb-raquet:/repo:ro \
  -v /home/cayetano/Downloads/raster:/data:ro \
  -v /tmp/raquet_perf:/out \
  raquet-build:gdal-3.10.3 \
  duckdb -unsigned -c "
    SET threads=16; SET preserve_insertion_order=false;
    LOAD '/repo/build/release/extension/raquet/raquet.duckdb_extension';
    COPY (SELECT * FROM read_raster(
            '/data/classification_germany.tif', format='v0'))
      TO '/out/germany_full.parquet' (FORMAT parquet);
  "
```

We must run inside the same image because the extension is dynamically linked against the container's `libgdal.so.36`. The `-unsigned` flag is required because the extension is locally-built (no DuckDB community-extension signature). `SET preserve_insertion_order=false` is what unlocks DuckDB's parallel scheduling for the `read_raster` source pipeline; without it, DuckDB collapses the source to a single thread.

### Compare against upstream main in the same container

```bash
docker run --rm \
  -v /home/cayetano/Downloads/raster:/data:ro \
  -v /tmp/raquet_perf:/out \
  raquet-build:gdal-3.10.3 \
  duckdb -unsigned -c "
    SET threads=16; SET preserve_insertion_order=false;
    INSTALL raquet FROM community;  -- v0.2.3 from the duckdb community repo @ 931b5ff
    LOAD raquet;
    COPY (SELECT * FROM read_raster(
            '/data/classification_germany.tif', format='v0'))
      TO '/out/germany_full_upstream.parquet' (FORMAT parquet);
  "
# Expect SIGSEGV after ~30 min, /out/germany_full_upstream.parquet remains 0 bytes
```

### Verify correctness on the patched output

```sql
-- Total tile count and metadata.num_blocks should match
SELECT
  (SELECT COUNT(*) FROM read_parquet('/tmp/raquet_perf/germany_full.parquet')
     WHERE block != 0) AS data_rows,
  (SELECT metadata FROM read_parquet('/tmp/raquet_perf/germany_full.parquet')
     WHERE block  = 0) AS metadata;
-- Expected: data_rows = 53423, metadata.num_blocks = 53423
```

---

## File map (this branch)

| Commit | File | Net diff |
|---|---|---|
| `8a925ee` | `src/raster/read_raster.cpp` | +16 |
| `f1ce7d6` | `src/raster/read_raster.cpp` | +8 / -10 |
| `da8ff95` | `src/raster/read_raster.cpp` | +127 / -76 |
| `badcc5d` | `src/raster/read_raster.cpp` | +13 / -2 |
| `5029627` | `src/raster/read_raster.cpp` | +134 / -124 |
| **Total** | `src/raster/read_raster.cpp` | **+298 / -212** |

All changes are confined to one file. No public API changes (function signatures, named parameters, output schema, metadata fields are all unchanged).

---

## Provenance

The original investigation that surfaced these bugs lives at
`~/dev_projs/cartolibs/cloud-native/carto-cli/raster-import-investigation.md`
(internal cloud-native repo). That document was the starting point — it
described the symptom (slow imports of large rasters) and the production
fix for the unrelated `/vsicurl/` opening bug (PR #24329). The five
commits in this branch address the *separate* perf and correctness issues
that document flagged as open follow-ups.
