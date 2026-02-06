#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// All metadata helper functions (raquet_is_metadata_row, raquet_is_data_row)
// were removed during API consolidation. Users should use `block = 0` and
// `block != 0` directly instead.

void RegisterMetadataFunctions(ExtensionLoader &loader) {
    // No functions to register - metadata helpers were consolidated away
}

} // namespace duckdb
