#define DUCKDB_EXTENSION_MAIN

#include "raquet_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"

namespace duckdb {

// Forward declarations for function registration
void RegisterQuadbinFunctions(ExtensionLoader &loader);
void RegisterPolyfillFunctions(ExtensionLoader &loader);
void RegisterRasterValueFunctions(ExtensionLoader &loader);
void RegisterRasterStatsFunctions(ExtensionLoader &loader);
void RegisterRegionStatsFunctions(ExtensionLoader &loader);
void RegisterClipFunctions(ExtensionLoader &loader);
void RegisterBandMathFunctions(ExtensionLoader &loader);
void RegisterMetadataFunctions(ExtensionLoader &loader);
void RegisterRaquetTableFunctions(ExtensionLoader &loader);

// Table macro definitions for read_raquet with spatial filtering overloads
// v0.3.0 format: metadata is in a row where block=0, data rows have block!=0
// These macros propagate the metadata to all data rows using REPLACE
static const DefaultTableMacro RAQUET_TABLE_MACROS[] = {
    // 1-arg: Basic read - propagates metadata from metadata row to all data rows
    {DEFAULT_SCHEMA, "read_raquet", {"file", nullptr}, {{nullptr, nullptr}},
     R"(
        SELECT * REPLACE (
            (SELECT metadata FROM read_parquet(file) WHERE block = 0) AS metadata
        )
        FROM read_parquet(file)
        WHERE block != 0
     )"},

    // 2-arg: Spatial filter with auto-detected resolution
    {DEFAULT_SCHEMA, "read_raquet", {"file", "geometry", nullptr}, {{nullptr, nullptr}},
     R"(
        WITH file_resolution AS (
            SELECT quadbin_resolution(block) AS res
            FROM read_parquet(file)
            WHERE block != 0
            LIMIT 1
        )
        SELECT * REPLACE (
            (SELECT metadata FROM read_parquet(file) WHERE block = 0) AS metadata
        )
        FROM read_parquet(file)
        WHERE block IN (
            SELECT UNNEST(QUADBIN_POLYFILL(geometry, (SELECT res FROM file_resolution)))
        )
        AND block != 0
     )"},

    // 3-arg: Spatial filter with explicit resolution
    {DEFAULT_SCHEMA, "read_raquet", {"file", "geometry", "resolution", nullptr}, {{nullptr, nullptr}},
     R"(
        SELECT * REPLACE (
            (SELECT metadata FROM read_parquet(file) WHERE block = 0) AS metadata
        )
        FROM read_parquet(file)
        WHERE block IN (SELECT UNNEST(QUADBIN_POLYFILL(geometry, resolution)))
          AND block != 0
     )"},

    // Sentinel
    {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
};

// Table macro definition for read_raquet_metadata
// Returns the metadata row only (block = 0)
static const DefaultTableMacro RAQUET_METADATA_TABLE_MACRO = {
    DEFAULT_SCHEMA,
    "read_raquet_metadata",
    {"file", nullptr},
    {{nullptr, nullptr}},
    R"(
        SELECT metadata
        FROM read_parquet(file)
        WHERE block = 0
     )"
};

// Table macro definitions for ST_Raster - works with iceberg tables or any table
// that follows the raquet convention (block=0 for metadata, block!=0 for data)
// Usage: SELECT * FROM ST_Raster('demo_catalog.portolan.modis_lst_spain')
// Note: Table name must be passed as a string (uses query_table internally)
static const DefaultTableMacro RAQUET_FROM_TABLE_MACROS[] = {
    // 1-arg: Basic read from table - propagates metadata from block=0 row to all data rows
    {DEFAULT_SCHEMA, "ST_Raster", {"tbl", nullptr}, {{nullptr, nullptr}},
     R"(
        SELECT * REPLACE (
            (SELECT metadata FROM query_table(tbl) WHERE block = 0) AS metadata
        )
        FROM query_table(tbl)
        WHERE block != 0
     )"},

    // 2-arg: Spatial filter with auto-detected resolution
    {DEFAULT_SCHEMA, "ST_Raster", {"tbl", "geometry", nullptr}, {{nullptr, nullptr}},
     R"(
        WITH table_resolution AS (
            SELECT quadbin_resolution(block::UBIGINT) AS res
            FROM query_table(tbl)
            WHERE block != 0
            LIMIT 1
        )
        SELECT * REPLACE (
            (SELECT metadata FROM query_table(tbl) WHERE block = 0) AS metadata
        )
        FROM query_table(tbl)
        WHERE block::UBIGINT IN (
            SELECT UNNEST(QUADBIN_POLYFILL(geometry, (SELECT res FROM table_resolution)))
        )
        AND block != 0
     )"},

    // 3-arg: Spatial filter with explicit resolution
    {DEFAULT_SCHEMA, "ST_Raster", {"tbl", "geometry", "resolution", nullptr}, {{nullptr, nullptr}},
     R"(
        SELECT * REPLACE (
            (SELECT metadata FROM query_table(tbl) WHERE block = 0) AS metadata
        )
        FROM query_table(tbl)
        WHERE block::UBIGINT IN (SELECT UNNEST(QUADBIN_POLYFILL(geometry, resolution)))
          AND block != 0
     )"},

    // Sentinel
    {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
};

// Table macro definitions for ST_RasterAt - point query from iceberg tables
// Usage: SELECT * FROM ST_RasterAt('demo_catalog.portolan.modis_lst_spain', lon, lat)
// Note: Table name must be passed as a string (uses query_table internally)
static const DefaultTableMacro RAQUET_TABLE_AT_MACROS[] = {
    // 2-arg: Point query with geometry and auto-detected resolution
    {DEFAULT_SCHEMA, "ST_RasterAt", {"tbl", "point", nullptr}, {{nullptr, nullptr}},
     R"(
        WITH table_resolution AS (
            SELECT quadbin_resolution(block::UBIGINT) AS res
            FROM query_table(tbl)
            WHERE block != 0
            LIMIT 1
        )
        SELECT * REPLACE (
            (SELECT metadata FROM query_table(tbl) WHERE block = 0) AS metadata
        )
        FROM query_table(tbl)
        WHERE block::UBIGINT = quadbin_from_lonlat(ST_X(point), ST_Y(point), (SELECT res FROM table_resolution))
     )"},

    // 3-arg: Point query with lon/lat and auto-detected resolution
    {DEFAULT_SCHEMA, "ST_RasterAt", {"tbl", "lon", "lat", nullptr}, {{nullptr, nullptr}},
     R"(
        WITH table_resolution AS (
            SELECT quadbin_resolution(block::UBIGINT) AS res
            FROM query_table(tbl)
            WHERE block != 0
            LIMIT 1
        )
        SELECT * REPLACE (
            (SELECT metadata FROM query_table(tbl) WHERE block = 0) AS metadata
        )
        FROM query_table(tbl)
        WHERE block::UBIGINT = quadbin_from_lonlat(lon, lat, (SELECT res FROM table_resolution))
     )"},

    // 4-arg: Point query with lon/lat and explicit resolution
    {DEFAULT_SCHEMA, "ST_RasterAt", {"tbl", "lon", "lat", "resolution", nullptr}, {{nullptr, nullptr}},
     R"(
        SELECT * REPLACE (
            (SELECT metadata FROM query_table(tbl) WHERE block = 0) AS metadata
        )
        FROM query_table(tbl)
        WHERE block::UBIGINT = quadbin_from_lonlat(lon, lat, resolution)
     )"},

    // Sentinel
    {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
};

// Table macro definitions for read_raquet_at - point query convenience function
// Returns raster data at a specific point location, hiding the block filter implementation detail
// This is optimized for single-point queries and returns exactly one row
// Supports both lon/lat and geometry parameters
static const DefaultTableMacro RAQUET_AT_TABLE_MACROS[] = {
    // 2-arg: Point query with geometry and auto-detected resolution
    // Usage: SELECT ... FROM read_raquet_at('file.parquet', ST_Point(lon, lat))
    {DEFAULT_SCHEMA, "read_raquet_at", {"file", "point", nullptr}, {{nullptr, nullptr}},
     R"(
        WITH file_resolution AS (
            SELECT quadbin_resolution(block) AS res
            FROM read_parquet(file)
            WHERE block != 0
            LIMIT 1
        )
        SELECT * REPLACE (
            (SELECT metadata FROM read_parquet(file) WHERE block = 0) AS metadata
        )
        FROM read_parquet(file)
        WHERE block = quadbin_from_lonlat(ST_X(point), ST_Y(point), (SELECT res FROM file_resolution))
     )"},

    // 3-arg: Point query with lon/lat and auto-detected resolution
    // Usage: SELECT ... FROM read_raquet_at('file.parquet', lon, lat)
    {DEFAULT_SCHEMA, "read_raquet_at", {"file", "lon", "lat", nullptr}, {{nullptr, nullptr}},
     R"(
        WITH file_resolution AS (
            SELECT quadbin_resolution(block) AS res
            FROM read_parquet(file)
            WHERE block != 0
            LIMIT 1
        )
        SELECT * REPLACE (
            (SELECT metadata FROM read_parquet(file) WHERE block = 0) AS metadata
        )
        FROM read_parquet(file)
        WHERE block = quadbin_from_lonlat(lon, lat, (SELECT res FROM file_resolution))
     )"},

    // 4-arg: Point query with lon/lat and explicit resolution
    // Usage: SELECT ... FROM read_raquet_at('file.parquet', lon, lat, 13)
    {DEFAULT_SCHEMA, "read_raquet_at", {"file", "lon", "lat", "resolution", nullptr}, {{nullptr, nullptr}},
     R"(
        SELECT * REPLACE (
            (SELECT metadata FROM read_parquet(file) WHERE block = 0) AS metadata
        )
        FROM read_parquet(file)
        WHERE block = quadbin_from_lonlat(lon, lat, resolution)
     )"},

    // Sentinel
    {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
};

// Create a table macro function from a DefaultTableMacro definition
static unique_ptr<MacroFunction> CreateTableMacroFunction(const DefaultTableMacro &default_macro) {
    Parser parser;
    parser.ParseQuery(default_macro.macro);
    if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
        throw InternalException("Expected a single select statement in CreateTableMacroFunction");
    }
    auto node = std::move(parser.statements[0]->Cast<SelectStatement>().node);
    auto function = make_uniq<TableMacroFunction>(std::move(node));

    // Add parameters
    for (idx_t param_idx = 0; default_macro.parameters[param_idx] != nullptr; param_idx++) {
        function->parameters.push_back(make_uniq<ColumnRefExpression>(default_macro.parameters[param_idx]));
    }

    return function;
}

// Create a CreateMacroInfo with multiple overloaded table macros
static unique_ptr<CreateMacroInfo> CreateReadRaquetMacroInfo() {
    auto bind_info = make_uniq<CreateMacroInfo>(CatalogType::TABLE_MACRO_ENTRY);
    bind_info->schema = DEFAULT_SCHEMA;
    bind_info->name = "read_raquet";
    bind_info->temporary = true;
    bind_info->internal = true;

    // Add all overloads to the macros vector
    for (idx_t i = 0; RAQUET_TABLE_MACROS[i].name != nullptr; i++) {
        bind_info->macros.push_back(CreateTableMacroFunction(RAQUET_TABLE_MACROS[i]));
    }

    return bind_info;
}

static unique_ptr<CreateMacroInfo> CreateReadRaquetMetadataMacroInfo() {
    auto bind_info = make_uniq<CreateMacroInfo>(CatalogType::TABLE_MACRO_ENTRY);
    bind_info->schema = DEFAULT_SCHEMA;
    bind_info->name = "read_raquet_metadata";
    bind_info->temporary = true;
    bind_info->internal = true;

    bind_info->macros.push_back(CreateTableMacroFunction(RAQUET_METADATA_TABLE_MACRO));

    return bind_info;
}

// Create a CreateMacroInfo for read_raquet_at table macro
static unique_ptr<CreateMacroInfo> CreateReadRaquetAtMacroInfo() {
    auto bind_info = make_uniq<CreateMacroInfo>(CatalogType::TABLE_MACRO_ENTRY);
    bind_info->schema = DEFAULT_SCHEMA;
    bind_info->name = "read_raquet_at";
    bind_info->temporary = true;
    bind_info->internal = true;

    // Add all overloads to the macros vector
    for (idx_t i = 0; RAQUET_AT_TABLE_MACROS[i].name != nullptr; i++) {
        bind_info->macros.push_back(CreateTableMacroFunction(RAQUET_AT_TABLE_MACROS[i]));
    }

    return bind_info;
}

// Create a CreateMacroInfo for ST_Raster table macro
static unique_ptr<CreateMacroInfo> CreateReadRaquetTableMacroInfo() {
    auto bind_info = make_uniq<CreateMacroInfo>(CatalogType::TABLE_MACRO_ENTRY);
    bind_info->schema = DEFAULT_SCHEMA;
    bind_info->name = "ST_Raster";
    bind_info->temporary = true;
    bind_info->internal = true;

    for (idx_t i = 0; RAQUET_FROM_TABLE_MACROS[i].name != nullptr; i++) {
        bind_info->macros.push_back(CreateTableMacroFunction(RAQUET_FROM_TABLE_MACROS[i]));
    }

    return bind_info;
}

// Create a CreateMacroInfo for ST_RasterAt table macro
static unique_ptr<CreateMacroInfo> CreateReadRaquetTableAtMacroInfo() {
    auto bind_info = make_uniq<CreateMacroInfo>(CatalogType::TABLE_MACRO_ENTRY);
    bind_info->schema = DEFAULT_SCHEMA;
    bind_info->name = "ST_RasterAt";
    bind_info->temporary = true;
    bind_info->internal = true;

    for (idx_t i = 0; RAQUET_TABLE_AT_MACROS[i].name != nullptr; i++) {
        bind_info->macros.push_back(CreateTableMacroFunction(RAQUET_TABLE_AT_MACROS[i]));
    }

    return bind_info;
}

static void LoadInternal(ExtensionLoader &loader) {
    // Register all functions
    RegisterQuadbinFunctions(loader);
    RegisterPolyfillFunctions(loader);
    RegisterRasterValueFunctions(loader);
    RegisterRasterStatsFunctions(loader);
    RegisterRegionStatsFunctions(loader);
    RegisterClipFunctions(loader);
    RegisterBandMathFunctions(loader);
    RegisterMetadataFunctions(loader);
    RegisterRaquetTableFunctions(loader);

    // Register read_raquet table macro with all overloads
    auto macro_info = CreateReadRaquetMacroInfo();
    loader.RegisterFunction(*macro_info);

    // Register read_raquet_metadata table macro
    auto metadata_macro_info = CreateReadRaquetMetadataMacroInfo();
    loader.RegisterFunction(*metadata_macro_info);

    // Register read_raquet_at table macro for point queries
    auto at_macro_info = CreateReadRaquetAtMacroInfo();
    loader.RegisterFunction(*at_macro_info);

    // Register ST_Raster table macro for iceberg/table queries
    auto table_macro_info = CreateReadRaquetTableMacroInfo();
    loader.RegisterFunction(*table_macro_info);

    // Register ST_RasterAt table macro for point queries on tables
    auto table_at_macro_info = CreateReadRaquetTableAtMacroInfo();
    loader.RegisterFunction(*table_at_macro_info);
}

void RaquetExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}

std::string RaquetExtension::Name() {
    return "raquet";
}

std::string RaquetExtension::Version() const {
#ifdef EXT_VERSION_RAQUET
    return EXT_VERSION_RAQUET;
#else
    return "0.1.0";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(raquet, loader) {
    duckdb::LoadInternal(loader);
}

}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
