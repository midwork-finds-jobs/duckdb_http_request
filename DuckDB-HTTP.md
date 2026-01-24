# Using DuckDB's HTTP Libraries in Extensions

This guide explains how to make HTTP requests directly in DuckDB extensions using the bundled `cpp-httplib` library.

## Overview

DuckDB bundles `cpp-httplib` (v0.14.3) with OpenSSL support. The library is exposed via the `duckdb_httplib_openssl` namespace.

**Capabilities:**

- HTTP/1.1 (no HTTP/2 or HTTP/3)
- HTTPS with OpenSSL
- Keep-alive connections
- Proxy support
- Timeouts
- Custom headers
- All HTTP methods (GET, POST, PUT, PATCH, DELETE, HEAD)
- Multipart form data
- Gzip decompression (built-in)

## Required Includes

```cpp
#include "duckdb.hpp"
#include "duckdb/common/http_util.hpp"           // HTTPUtil for proxy parsing
#include "duckdb/common/file_opener.hpp"         // For reading DuckDB settings
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_file_opener.hpp"

// Enable OpenSSL support before including httplib
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"
```

## Basic GET Request

```cpp
#include "duckdb.hpp"
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

namespace duckdb {

string SimpleGet(const string &url) {
    // Parse URL into host and path
    // Example: "https://api.example.com/data" -> "https://api.example.com", "/data"

    duckdb_httplib_openssl::Client client("https://api.example.com");

    // Configure client
    client.set_follow_location(true);           // Follow redirects
    client.set_read_timeout(30, 0);             // 30 second timeout
    client.set_connection_timeout(30, 0);
    client.enable_server_certificate_verification(false);  // Skip cert verification

    // Make request
    auto res = client.Get("/data");

    if (res && res->status == 200) {
        return res->body;
    }

    throw IOException("HTTP request failed");
}

} // namespace duckdb
```

## Reading DuckDB HTTP Settings

DuckDB has built-in HTTP settings that users can configure. Respect these in your extension:

```cpp
#include "duckdb/common/file_opener.hpp"
#include "duckdb/main/client_context_file_opener.hpp"

struct HttpSettings {
    uint64_t timeout = 30;
    bool keep_alive = true;
    string proxy;
    string proxy_username;
    string proxy_password;
    string user_agent;
};

HttpSettings GetHttpSettings(ClientContext &context, const string &url) {
    HttpSettings settings;

    auto &db = DatabaseInstance::GetDatabase(context);
    auto &config = db.config;

    // Create file opener to read settings
    ClientContextFileOpener opener(context);
    FileOpenerInfo info;
    info.file_path = url;

    // Read DuckDB settings
    FileOpener::TryGetCurrentSetting(&opener, "http_timeout", settings.timeout, &info);
    FileOpener::TryGetCurrentSetting(&opener, "http_keep_alive", settings.keep_alive, &info);

    // Get proxy from config
    settings.proxy = config.options.http_proxy;
    settings.proxy_username = config.options.http_proxy_username;
    settings.proxy_password = config.options.http_proxy_password;

    // Build user agent
    settings.user_agent = StringUtil::Format("%s %s", config.UserAgent(), DuckDB::SourceID());

    return settings;
}
```

## Complete HTTP Request with Settings

```cpp
#include "duckdb/common/http_util.hpp"

string MakeHttpRequest(ClientContext &context, const string &url) {
    auto settings = GetHttpSettings(context, url);

    // Parse URL
    string proto_host_port, path;
    // Simple parsing (use a proper URL parser in production)
    auto path_start = url.find('/', 8);  // Skip "https://"
    proto_host_port = url.substr(0, path_start);
    path = (path_start != string::npos) ? url.substr(path_start) : "/";

    // Create client
    duckdb_httplib_openssl::Client client(proto_host_port);

    // Apply settings
    client.set_read_timeout(static_cast<time_t>(settings.timeout), 0);
    client.set_write_timeout(static_cast<time_t>(settings.timeout), 0);
    client.set_connection_timeout(static_cast<time_t>(settings.timeout), 0);
    client.set_keep_alive(settings.keep_alive);
    client.set_follow_location(true);
    client.enable_server_certificate_verification(false);

    // Configure proxy if set
    if (!settings.proxy.empty()) {
        string proxy_host;
        idx_t proxy_port = 80;
        string proxy_copy = settings.proxy;
        HTTPUtil::ParseHTTPProxyHost(proxy_copy, proxy_host, proxy_port);
        client.set_proxy(proxy_host, static_cast<int>(proxy_port));

        if (!settings.proxy_username.empty()) {
            client.set_proxy_basic_auth(settings.proxy_username, settings.proxy_password);
        }
    }

    // Set headers
    duckdb_httplib_openssl::Headers headers = {
        {"User-Agent", settings.user_agent},
        {"Accept", "application/json"}
    };

    // Make request
    auto res = client.Get(path, headers);

    if (res.error() != duckdb_httplib_openssl::Error::Success) {
        throw IOException("HTTP request failed: connection error");
    }

    if (res->status != 200) {
        throw IOException("HTTP request failed: status %d", res->status);
    }

    return res->body;
}
```

## Table Function Example: ECB Exchange Rates

Here's a complete example for fetching ECB exchange rates:

```cpp
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

namespace duckdb {

struct EcbRatesBindData : public TableFunctionData {
    bool done = false;
    string xml_data;
};

static unique_ptr<FunctionData> EcbRatesBind(ClientContext &context,
                                              TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types,
                                              vector<string> &names) {
    auto bind_data = make_uniq<EcbRatesBindData>();

    // Fetch XML from ECB
    duckdb_httplib_openssl::Client client("https://www.ecb.europa.eu");
    client.set_follow_location(true);
    client.set_read_timeout(30, 0);
    client.enable_server_certificate_verification(false);

    auto res = client.Get("/stats/eurofxref/eurofxref-daily.xml");

    if (!res || res->status != 200) {
        throw IOException("Failed to fetch ECB rates");
    }

    bind_data->xml_data = res->body;

    // Define output columns
    return_types.push_back(LogicalType::VARCHAR);  // currency
    return_types.push_back(LogicalType::DOUBLE);   // rate
    return_types.push_back(LogicalType::DATE);     // date

    names.push_back("currency");
    names.push_back("rate");
    names.push_back("date");

    return std::move(bind_data);
}

static void EcbRatesFunction(ClientContext &context, TableFunctionInput &data,
                             DataChunk &output) {
    auto &bind_data = data.bind_data->CastNoConst<EcbRatesBindData>();

    if (bind_data.done) {
        output.SetCardinality(0);
        return;
    }

    // Parse XML and populate output...
    // (XML parsing code here)

    bind_data.done = true;
}

void RegisterEcbRatesFunction(DatabaseInstance &db) {
    TableFunction func("ecb_rates", {}, EcbRatesFunction, EcbRatesBind);
    ExtensionUtil::RegisterFunction(db, func);
}

} // namespace duckdb
```

## POST Request with JSON Body

```cpp
string PostJson(const string &url, const string &json_body) {
    duckdb_httplib_openssl::Client client("https://api.example.com");

    duckdb_httplib_openssl::Headers headers = {
        {"Content-Type", "application/json"},
        {"Accept", "application/json"}
    };

    auto res = client.Post("/endpoint", headers, json_body, "application/json");

    if (res && res->status == 200) {
        return res->body;
    }

    throw IOException("POST request failed");
}
```

## Multipart Form Upload

```cpp
string UploadFile(const string &url, const string &filename, const string &content) {
    duckdb_httplib_openssl::Client client("https://api.example.com");

    duckdb_httplib_openssl::MultipartFormDataItems items = {
        {"file", content, filename, "application/octet-stream"}
    };

    auto res = client.Post("/upload", items);

    if (res && res->status == 200) {
        return res->body;
    }

    throw IOException("Upload failed");
}
```

## Error Handling

```cpp
auto res = client.Get(path, headers);

// Check connection errors
if (res.error() != duckdb_httplib_openssl::Error::Success) {
    switch (res.error()) {
        case duckdb_httplib_openssl::Error::Connection:
            throw IOException("Connection failed");
        case duckdb_httplib_openssl::Error::Read:
            throw IOException("Read timeout");
        case duckdb_httplib_openssl::Error::Write:
            throw IOException("Write timeout");
        case duckdb_httplib_openssl::Error::SSLConnection:
            throw IOException("SSL connection failed");
        default:
            throw IOException("HTTP error: %d", static_cast<int>(res.error()));
    }
}

// Check HTTP status
if (res->status >= 400) {
    throw IOException("HTTP %d: %s", res->status, res->body.c_str());
}
```

## Response Headers

```cpp
auto res = client.Get(path);

for (auto &header : res->headers) {
    // header.first = header name
    // header.second = header value

    if (StringUtil::CIEquals(header.first, "Content-Type")) {
        string content_type = header.second;
    }
}
```

## CMake Configuration

Add httpfs dependency in `extension_config.cmake`:

```cmake
duckdb_extension_load(your_extension
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Required for HTTP support
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG v1.4-andium
)
```

## Best Practices

1. **Respect DuckDB settings**: Read `http_timeout`, `http_keep_alive`, `http_proxy` from context
2. **Handle errors gracefully**: Check both connection errors and HTTP status codes
3. **Set timeouts**: Always configure read/write/connection timeouts
4. **Use User-Agent**: Include DuckDB version info in User-Agent header
5. **Decompress responses**: Handle gzip/zstd compressed responses
6. **Thread safety**: `Client` instances are not thread-safe; create per-thread or use mutex

## Limitations

- HTTP/1.1 only (no HTTP/2 or HTTP/3)
- No connection pooling across requests (each `Client` instance manages its own connection)
- No async/await pattern (blocking calls only)
- Certificate verification often disabled for compatibility

## See Also

- [cpp-httplib documentation](https://github.com/yhirose/cpp-httplib)
- [http_request extension source](https://github.com/midwork-finds-jobs/duckdb_http_request)
- [DuckDB extension development](https://duckdb.org/docs/extensions/overview)
