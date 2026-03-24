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
    src/raster/band_encoder.cpp
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

# Optional: Find libjpeg for JPEG decompression (v0.4.0)
find_package(JPEG QUIET)
if(JPEG_FOUND)
    message(STATUS "JPEG support enabled")
else()
    message(STATUS "JPEG support disabled (libjpeg not found)")
endif()

# Optional: Find libwebp for WebP decompression (v0.4.0)
find_package(WebP QUIET)
if(WebP_FOUND)
    message(STATUS "WebP support enabled")
else()
    message(STATUS "WebP support disabled (libwebp not found)")
endif()

# Optional: Find GDAL for raster ingestion (read_raster)
find_package(GDAL QUIET)
if(GDAL_FOUND)
    message(STATUS "GDAL support enabled (read_raster available)")
    list(APPEND EXTENSION_SOURCES src/raster/read_raster.cpp)
else()
    message(STATUS "GDAL support disabled (libgdal not found)")
endif()

if(NOT "${DUCKDB_EXTENSION_CONFIGS}" STREQUAL "")
    # Using DuckDB extension build system
    build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
    build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})
    target_link_libraries(${EXTENSION_NAME} ZLIB::ZLIB)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} ZLIB::ZLIB)

    if(JPEG_FOUND)
        target_compile_definitions(${EXTENSION_NAME} PRIVATE RAQUET_HAS_JPEG)
        target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE RAQUET_HAS_JPEG)
        target_link_libraries(${EXTENSION_NAME} JPEG::JPEG)
        target_link_libraries(${LOADABLE_EXTENSION_NAME} JPEG::JPEG)
    endif()
    if(WebP_FOUND)
        target_compile_definitions(${EXTENSION_NAME} PRIVATE RAQUET_HAS_WEBP)
        target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE RAQUET_HAS_WEBP)
        target_link_libraries(${EXTENSION_NAME} WebP::webp)
        target_link_libraries(${LOADABLE_EXTENSION_NAME} WebP::webp)
    endif()
    if(GDAL_FOUND)
        target_compile_definitions(${EXTENSION_NAME} PRIVATE RAQUET_HAS_GDAL)
        target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE RAQUET_HAS_GDAL)
        target_link_libraries(${EXTENSION_NAME} GDAL::GDAL)
        target_link_libraries(${LOADABLE_EXTENSION_NAME} GDAL::GDAL)
    endif()
else()
    # Standalone build for development
    add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})
    target_link_libraries(${EXTENSION_NAME} ZLIB::ZLIB)

    if(JPEG_FOUND)
        target_compile_definitions(${EXTENSION_NAME} PRIVATE RAQUET_HAS_JPEG)
        target_link_libraries(${EXTENSION_NAME} JPEG::JPEG)
    endif()
    if(WebP_FOUND)
        target_compile_definitions(${EXTENSION_NAME} PRIVATE RAQUET_HAS_WEBP)
        target_link_libraries(${EXTENSION_NAME} WebP::webp)
    endif()
    if(GDAL_FOUND)
        target_compile_definitions(${EXTENSION_NAME} PRIVATE RAQUET_HAS_GDAL)
        target_link_libraries(${EXTENSION_NAME} GDAL::GDAL)
    endif()
endif()
