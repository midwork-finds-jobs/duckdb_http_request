# http_request

DuckDB extension for making HTTP requests with full method support (GET, POST, PUT, PATCH, DELETE, HEAD).

## Installation

```sql
INSTALL http_request FROM community;
LOAD http_request;
```

## Functions

### HTTP Methods

All HTTP methods return a struct with `status`, `headers`, and `body`:

```sql
-- Returns: STRUCT(status INTEGER, headers MAP(VARCHAR, VARCHAR), body BLOB)

-- GET request
SELECT http_get('https://api.example.com/data');

-- POST request
SELECT http_post('https://api.example.com/data');

-- PUT request
SELECT http_put('https://api.example.com/data');

-- PATCH request
SELECT http_patch('https://api.example.com/data');

-- DELETE request
SELECT http_delete('https://api.example.com/data');

-- HEAD request (no body returned)
SELECT http_head('https://api.example.com/data');
```

### With Headers and Query Parameters

```sql
-- Add custom headers and query parameters
SELECT http_get(
    'https://api.example.com/data',
    headers := {'Authorization': 'Bearer token123', 'Accept': 'application/json'},
    params := {'page': '1', 'limit': '10'}
);

-- Decode response body as text
SELECT decode(http_get('https://api.example.com/data').body);
```

### Table Function Syntax

For named parameters, use table function syntax with `FROM`:

```sql
SELECT * FROM http_get(
    'https://api.example.com/data',
    headers := {'Accept': 'application/json'},
    params := {'id': '123'}
);

-- POST with body
SELECT * FROM http_post(
    'https://api.example.com/data',
    body := '{"name": "test"}',
    headers := {'Content-Type': 'application/json'}
);
```

### Helper Functions

```sql
-- Generate Range header struct for partial content requests
SELECT http_range_header(727608633, 1278);
-- Returns: {'Range': 'bytes=727608633-727609910'}

-- Use with http_get for byte range requests
SELECT http_get(
    'https://example.com/largefile.bin',
    headers := http_range_header(0, 1024)
);

-- Generate range string (for manual header construction)
SELECT byte_range(727608633, 1278);
-- Returns: 'bytes=727608633-727609910'
```

## Configuration

### Proxy Settings

```sql
-- Set HTTP proxy
SET http_proxy = 'localhost:8888';
SET http_proxy_username = 'user';
SET http_proxy_password = 'pass';

-- Or use secrets
CREATE SECRET http_proxy_secret (
    TYPE http,
    http_proxy 'localhost:8888',
    http_proxy_username 'user',
    http_proxy_password 'pass'
);
```

### Timeout and Retry Settings

These settings are shared with the httpfs extension:

```sql
SET http_timeout = 30;           -- Timeout in seconds (default: 30)
SET http_retries = 3;            -- Number of retries (default: 3)
SET http_keep_alive = true;      -- Keep connections alive (default: true)
```

### Redirect Handling

By default, HTTP redirects are followed automatically. Disable to track redirect chains:

```sql
-- Disable auto-follow to inspect redirects
SET http_follow_redirects = false;

-- Check redirect status and Location header
SELECT
    status,
    headers['Location'] as redirect_url
FROM http_get('https://example.com/short-link');
-- Returns 301/302 with Location header

-- Re-enable auto-follow (default)
SET http_follow_redirects = true;
```

## Response Handling

```sql
-- Access response parts
SELECT
    resp.status,
    resp.headers,
    decode(resp.body) as body_text
FROM (
    SELECT http_get('https://api.example.com/data') as resp
);

-- Check status code
SELECT http_get('https://api.example.com/data').status;

-- Get specific header
SELECT http_get('https://api.example.com/data').headers['Content-Type'];
```

## Compression

Response bodies are automatically decompressed if gzip or zstd compressed (detected via magic bytes).

## Example: Common Crawl Byte Range Request

```sql
-- Fetch a specific byte range from Common Crawl
SELECT decode(http_get(
    'https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-01/segments/file.warc.gz',
    headers := http_range_header(93300763, 2453)
).body);
```

## Building from Source

```bash
git clone --recurse-submodules https://github.com/user/duckdb_http_request.git
cd duckdb_http_request
make release
```

## License

MIT
