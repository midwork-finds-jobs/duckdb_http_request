# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(httputil_request
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Required extension for HTTP support - load from GitHub since it's out-of-tree now
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG v1.4-andium
)
