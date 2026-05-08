#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Registers the `raquet_merge_bands(LIST<VARCHAR>)` table function.
//
// Takes a list of single-band raquet parquet paths and returns the
// merged multi-band raquet content (one metadata row at block=0 with
// composed multi-band metadata, plus joined data rows where each input's
// band_1 column maps to the corresponding output column band_i).
//
// Validates spatial-frame compatibility across inputs at bind time
// (version, block geometry, pyramid range, bounds, dimensions, dtype,
// compression). Mismatches raise InvalidInputException.
//
// The data join is delegated to DuckDB's parallel hash join via an
// internal Connection — this function is mostly orchestration and
// metadata composition.
void RegisterMergeBandsFunction(ExtensionLoader &loader);

}  // namespace duckdb
