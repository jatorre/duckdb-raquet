# This file is included by DuckDB's build system

duckdb_extension_load(raquet
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/src/include
    LOAD_TESTS
)

# Include parquet (required for read_parquet)
duckdb_extension_load(parquet)

# Include httpfs for remote file access
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG add35a03c1adfe530bb4ef69133b94fe8ec8ea35
)

# Note: iceberg extension requires avro with a custom-patched libavro-c
# For now, use two-step workflow: standard DuckDB for iceberg_scan, dev DuckDB for raquet functions
