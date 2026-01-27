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
void RegisterMetadataFunctions(ExtensionLoader &loader);
void RegisterRaquetTableFunctions(ExtensionLoader &loader);

// Table macro definitions for read_raquet with spatial filtering overloads
static const DefaultTableMacro RAQUET_TABLE_MACROS[] = {
    // 1-arg: Basic read (unchanged behavior)
    {DEFAULT_SCHEMA, "read_raquet", {"file", nullptr}, {{nullptr, nullptr}},
     "SELECT * FROM read_parquet(file)"},

    // 2-arg: Spatial filter with auto-detected resolution
    {DEFAULT_SCHEMA, "read_raquet", {"file", "geometry", nullptr}, {{nullptr, nullptr}},
     R"(
        WITH file_resolution AS (
            SELECT quadbin_resolution(block) AS res
            FROM read_parquet(file)
            WHERE block != 0
            LIMIT 1
        )
        SELECT * FROM read_parquet(file)
        WHERE block IN (
            SELECT UNNEST(QUADBIN_POLYFILL(geometry, (SELECT res FROM file_resolution)))
        )
        AND block != 0
     )"},

    // 3-arg: Spatial filter with explicit resolution
    {DEFAULT_SCHEMA, "read_raquet", {"file", "geometry", "resolution", nullptr}, {{nullptr, nullptr}},
     R"(
        SELECT * FROM read_parquet(file)
        WHERE block IN (SELECT UNNEST(QUADBIN_POLYFILL(geometry, resolution)))
          AND block != 0
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

static void LoadInternal(ExtensionLoader &loader) {
    // Register all functions
    RegisterQuadbinFunctions(loader);
    RegisterPolyfillFunctions(loader);
    RegisterRasterValueFunctions(loader);
    RegisterRasterStatsFunctions(loader);
    RegisterRegionStatsFunctions(loader);
    RegisterMetadataFunctions(loader);
    RegisterRaquetTableFunctions(loader);

    // Register read_raquet table macro with all overloads
    auto macro_info = CreateReadRaquetMacroInfo();
    loader.RegisterFunction(*macro_info);
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
