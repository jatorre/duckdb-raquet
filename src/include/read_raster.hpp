#pragma once

#ifdef RAQUET_HAS_GDAL

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

void RegisterReadRaster(ExtensionLoader &loader);

} // namespace duckdb

#endif // RAQUET_HAS_GDAL
