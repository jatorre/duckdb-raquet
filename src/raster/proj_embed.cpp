#ifdef RAQUET_HAS_GDAL

#include <proj.h>
#include <sqlite3.h>
#include <mutex>
#include <stdexcept>
#include <cstdio>

// Embedded proj.db data (generated at build time by xxd)
extern "C" unsigned char proj_db[];
extern "C" unsigned int proj_db_len;

// memvfs SQLite extension
extern "C" int sqlite3_memvfs_init(sqlite3 *, char **, const sqlite3_api_routines *);

namespace duckdb {
namespace raquet {

void InitEmbeddedProj() {
    static std::once_flag flag;
    std::call_once(flag, []() {
        // Initialize SQLite and register the memvfs VFS
        sqlite3_initialize();
        sqlite3_memvfs_init(nullptr, nullptr, nullptr);

        auto vfs = sqlite3_vfs_find("memvfs");
        if (!vfs) {
            throw std::runtime_error("Could not find sqlite memvfs extension for PROJ");
        }
        sqlite3_vfs_register(vfs, 0);

        // Build a URI pointing to the in-memory proj.db
        char path[256];
        snprintf(path, sizeof(path),
                 "file:/proj.db?immutable=1&ptr=%llu&sz=%u&max=%u",
                 reinterpret_cast<unsigned long long>(proj_db), proj_db_len, proj_db_len);

        // Configure the default PROJ context to use memvfs
        proj_context_set_sqlite3_vfs_name(nullptr, "memvfs");

        // Verify we can open the database
        sqlite3 *db = nullptr;
        int rc = sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, "memvfs");
        if (rc != SQLITE_OK) {
            throw std::runtime_error("Could not open embedded proj.db via memvfs");
        }
        sqlite3_close(db);

        // Set the database path for PROJ
        int ok = proj_context_set_database_path(nullptr, path, nullptr, nullptr);
        if (!ok) {
            throw std::runtime_error("Could not set PROJ database path");
        }
    });
}

} // namespace raquet
} // namespace duckdb

#endif // RAQUET_HAS_GDAL
