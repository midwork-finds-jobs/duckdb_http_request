# FIXME: Version Compatibility

## httpfs + DuckDB version mismatch

**Issue:** community-extensions CI uses DuckDB v1.4.3 which has `DUCKDB_LOG_WARN` macro, but newer httpfs uses `DUCKDB_LOG_WARNING` (renamed in DuckDB commit `283a63de5` on 2025-12-11).

**Symptom:**

```text
error: 'DUCKDB_LOG_WARNING' was not declared in this scope; did you mean 'DUCKDB_LOG_WARN'?
```

**Fix:** Pin httpfs to commit before the macro rename:

- DuckDB: `v1.4.3` (2025-12-08)
- httpfs: `7bd8c9c` (2025-12-10)

**When to update:** Once community-extensions updates to DuckDB >= v1.5 (or a version including commit `283a63de5`), update `extension_config.cmake` to use `GIT_TAG v1.4-andium` or later.
