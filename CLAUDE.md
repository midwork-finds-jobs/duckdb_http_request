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
