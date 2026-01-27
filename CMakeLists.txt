cmake_minimum_required(VERSION 3.12)

# Set extension name here
set(TARGET_NAME raquet)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# DuckDB's extension distribution supports
set(EXTENSION_STATIC_BUILD 1)

include_directories(src/include)


set(EXTENSION_SOURCES
    src/raquet_extension.cpp
    src/quadbin/quadbin_functions.cpp
    src/quadbin/quadbin_polyfill.cpp
    src/raster/band_decoder.cpp
    src/raster/st_raster_value.cpp
    src/raster/st_raster_stats.cpp
    src/raster/st_region_stats.cpp
    src/raster/st_clip.cpp
    src/raster/band_math.cpp
    src/metadata/raquet_metadata.cpp
    src/table_functions/raquet_table_functions.cpp
)

# Find zlib for gzip decompression
find_package(ZLIB REQUIRED)

if(NOT "${DUCKDB_EXTENSION_CONFIGS}" STREQUAL "")
    # Using DuckDB extension build system
    build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
    build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})
    target_link_libraries(${EXTENSION_NAME} ZLIB::ZLIB)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} ZLIB::ZLIB)
else()
    # Standalone build for development
    add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})
    target_link_libraries(${EXTENSION_NAME} ZLIB::ZLIB)
endif()
