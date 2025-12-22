# duckdb extension to make request with the httputil

You can look examples of how the httputil can be used from
`../duckdb-common-crawl/`.

Your mission is to create new functions which can be used for duckdb.

If you get stuck ask examples from githits mcp.

## Http get request

```sql
SELECT
    http_request(url, method := 'GET', byte_range := ...)
```

It should be able to create requests like this:

```sh
export OFFSET=93300763 LENGTH=2453 FILENAME="parse-output/segment/1346876860454/1346971482168_168.arc.gz"
curl -s -r"$OFFSET-$((OFFSET + LENGTH - 1))"     "https://data.commoncrawl.org/$FILENAME" | gzip -dc
```

Do not add new http libraries! Use the httputil from duckdb. Autoload `httpfs` with this extension.

## Releasing new versions

After significant changes, suggest releasing a new version:

1. Commit changes
2. Create tag: `git tag -a vX.Y.Z -m "vX.Y.Z: Description"`
3. Push: `git push origin HEAD && git push origin vX.Y.Z`
4. Update community-extensions:
   - Clone/update `/tmp/community-extensions` from duckdb/community-extensions
   - Create branch: `git checkout -b update-http-request-vX.Y.Z`
   - Update `extensions/http_request/description.yml` (version + ref + features)
   - Push and create PR to duckdb/community-extensions

The extension version comes from git tags (vX.Y.Z format).
