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

# Find GDAL for raster ingestion (read_raster)
find_package(GDAL QUIET CONFIG)
if(NOT GDAL_FOUND)
    find_package(GDAL QUIET)
endif()
if(GDAL_FOUND)
    message(STATUS "GDAL support enabled (read_raster available)")
    list(APPEND EXTENSION_SOURCES src/raster/read_raster.cpp)

    # Find PROJ and SQLite3 for embedded proj.db support
    find_package(PROJ QUIET CONFIG)
    find_package(unofficial-sqlite3 QUIET CONFIG)
    if(NOT unofficial-sqlite3_FOUND)
        find_package(SQLite3 QUIET)
    endif()

    # Generate proj_db.c from proj.db at build time
    # Find proj.db from PROJ's share directory
    set(PROJ_DB_FOUND FALSE)
    if(PROJ_FOUND OR PROJ4_FOUND)
        # Try vcpkg installed location
        foreach(PREFIX ${CMAKE_PREFIX_PATH} ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET})
            if(EXISTS "${PREFIX}/share/proj/proj.db")
                set(PROJ_DB_PATH "${PREFIX}/share/proj/proj.db")
                set(PROJ_DB_FOUND TRUE)
                break()
            endif()
        endforeach()
    endif()
    # Fallback: system paths
    if(NOT PROJ_DB_FOUND)
        find_file(PROJ_DB_PATH proj.db
            PATHS /opt/homebrew/share/proj /usr/share/proj /usr/local/share/proj
            NO_DEFAULT_PATH)
        if(PROJ_DB_PATH)
            set(PROJ_DB_FOUND TRUE)
        endif()
    endif()

    if(PROJ_DB_FOUND)
        message(STATUS "Found proj.db: ${PROJ_DB_PATH}")
        set(PROJ_DB_C "${CMAKE_CURRENT_BINARY_DIR}/proj_db.c")
        add_custom_command(
            OUTPUT "${PROJ_DB_C}"
            COMMAND ${CMAKE_COMMAND} -E echo "Generating proj_db.c from ${PROJ_DB_PATH}..."
            COMMAND xxd -i proj.db "${PROJ_DB_C}"
            WORKING_DIRECTORY "${PROJ_DB_PATH}/.."
            DEPENDS "${PROJ_DB_PATH}"
            COMMENT "Embedding proj.db into C source"
        )
        list(APPEND EXTENSION_SOURCES
            "${PROJ_DB_C}"
            src/raster/memvfs.c
            src/raster/proj_embed.cpp
        )
        message(STATUS "PROJ database embedding enabled")
    else()
        message(STATUS "proj.db not found - PROJ_DATA env var required at runtime")
    endif()
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
        if(PROJ_DB_FOUND)
            if(TARGET PROJ::proj)
                target_link_libraries(${EXTENSION_NAME} PROJ::proj)
                target_link_libraries(${LOADABLE_EXTENSION_NAME} PROJ::proj)
            endif()
            if(TARGET unofficial::sqlite3::sqlite3)
                target_link_libraries(${EXTENSION_NAME} unofficial::sqlite3::sqlite3)
                target_link_libraries(${LOADABLE_EXTENSION_NAME} unofficial::sqlite3::sqlite3)
            elseif(TARGET SQLite::SQLite3)
                target_link_libraries(${EXTENSION_NAME} SQLite::SQLite3)
                target_link_libraries(${LOADABLE_EXTENSION_NAME} SQLite::SQLite3)
            endif()
        endif()
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
        if(PROJ_DB_FOUND)
            if(TARGET PROJ::proj)
                target_link_libraries(${EXTENSION_NAME} PROJ::proj)
            endif()
            if(TARGET unofficial::sqlite3::sqlite3)
                target_link_libraries(${EXTENSION_NAME} unofficial::sqlite3::sqlite3)
            elseif(TARGET SQLite::SQLite3)
                target_link_libraries(${EXTENSION_NAME} SQLite::SQLite3)
            endif()
        endif()
    endif()
endif()
