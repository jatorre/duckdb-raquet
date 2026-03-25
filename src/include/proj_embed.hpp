#pragma once

#ifdef RAQUET_HAS_GDAL

namespace duckdb {
namespace raquet {

// Initialize the embedded PROJ database.
// Must be called before any GDAL/PROJ coordinate transformations.
// Safe to call multiple times (uses std::once_flag internally).
void InitEmbeddedProj();

} // namespace raquet
} // namespace duckdb

#endif // RAQUET_HAS_GDAL
