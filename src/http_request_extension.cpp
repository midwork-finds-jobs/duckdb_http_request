#define DUCKDB_EXTENSION_MAIN

#include "http_request_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/gzip_file_system.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

// Use httplib directly for full HTTP method support
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

#include "zstd.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace duckdb {

// Default max concurrent HTTP requests per scalar function call
static constexpr idx_t DEFAULT_HTTP_MAX_CONCURRENT = 32;

// Zstd magic number: 0xFD2FB528 (little-endian: 28 B5 2F FD)
static constexpr uint8_t ZSTD_MAGIC_1 = 0x28;
static constexpr uint8_t ZSTD_MAGIC_2 = 0xB5;
static constexpr uint8_t ZSTD_MAGIC_3 = 0x2F;
static constexpr uint8_t ZSTD_MAGIC_4 = 0xFD;

static bool CheckIsZstd(const char *data, idx_t size) {
	if (size < 4) {
		return false;
	}
	return static_cast<uint8_t>(data[0]) == ZSTD_MAGIC_1 && static_cast<uint8_t>(data[1]) == ZSTD_MAGIC_2 &&
	       static_cast<uint8_t>(data[2]) == ZSTD_MAGIC_3 && static_cast<uint8_t>(data[3]) == ZSTD_MAGIC_4;
}

static string DecompressZstd(const string &compressed) {
	unsigned long long decompressed_size = duckdb_zstd::ZSTD_getFrameContentSize(compressed.data(), compressed.size());

	if (decompressed_size == ZSTD_CONTENTSIZE_ERROR) {
		throw IOException("Invalid zstd compressed data");
	}

	if (decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
		decompressed_size = compressed.size() * 10;
	}

	vector<char> buffer(decompressed_size);

	size_t actual_size =
	    duckdb_zstd::ZSTD_decompress(buffer.data(), buffer.size(), compressed.data(), compressed.size());

	if (duckdb_zstd::ZSTD_isError(actual_size)) {
		throw IOException("Zstd decompression failed: %s", duckdb_zstd::ZSTD_getErrorName(actual_size));
	}

	return string(buffer.data(), actual_size);
}

// Forward declarations
static void ParseUrl(const string &url, string &proto_host_port, string &path);
static Value ParseSetCookieHeader(const string &cookie_str);
static Value BuildHttpResponseValue(int32_t status_code, const string &content_type, int64_t content_length,
                                    vector<Value> &header_keys, vector<Value> &header_values, vector<Value> &cookies,
                                    const string &body);

// Struct to hold HTTP settings extracted from context (thread-safe to pass to workers)
struct HttpSettings {
	uint64_t timeout;
	bool keep_alive;
	string proxy;
	string proxy_username;
	string proxy_password;
	string user_agent;
	uint64_t max_concurrency;
	bool use_cache;
	bool follow_redirects;
};

// Struct to hold HTTP response (for parallel collection)
struct HttpResponseData {
	int32_t status_code;
	string content_type;
	int64_t content_length;
	vector<Value> header_keys;
	vector<Value> header_values;
	vector<Value> cookies;
	string body;
	string error; // Non-empty if request failed
};

// Normalize HTTP header name to Title-Case (e.g., "content-type" -> "Content-Type")
// Per HTTP convention, header names are case-insensitive but commonly written in Title-Case
static string NormalizeHeaderName(const string &name) {
	string result;
	result.reserve(name.size());
	bool capitalize_next = true;
	for (char c : name) {
		if (c == '-') {
			result += c;
			capitalize_next = true;
		} else if (capitalize_next) {
			result += static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
			capitalize_next = false;
		} else {
			result += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
		}
	}
	return result;
}

// Cache entry with timestamp
struct HttpCacheEntry {
	HttpResponseData response;
	std::chrono::steady_clock::time_point timestamp;
};

// Global HTTP request cache to prevent duplicate requests within same query
// Uses short TTL (1 second) to cache responses during query execution
class HttpRequestCache {
public:
	static HttpRequestCache &Instance() {
		static HttpRequestCache instance;
		return instance;
	}

	// Generate cache key from request parameters (uses hashing for memory efficiency)
	static string MakeKey(const string &url, const string &method, const duckdb_httplib_openssl::Headers &headers,
	                      const string &body = "") {
		// Hash headers to save memory (headers can be large)
		std::hash<string> hasher;
		size_t headers_hash = 0;
		for (const auto &h : headers) {
			headers_hash ^= hasher(h.first) + 0x9e3779b9 + (headers_hash << 6) + (headers_hash >> 2);
			headers_hash ^= hasher(h.second) + 0x9e3779b9 + (headers_hash << 6) + (headers_hash >> 2);
		}
		// Hash body if present (body can be large for POST/PUT)
		size_t body_hash = body.empty() ? 0 : hasher(body);
		// Combine: method|url|headers_hash|body_hash
		return method + "|" + url + "|" + std::to_string(headers_hash) + "|" + std::to_string(body_hash);
	}

	// Try to get cached response (returns true if found and not expired)
	bool TryGet(const string &key, HttpResponseData &out) {
		std::lock_guard<std::mutex> lock(mutex_);
		auto it = cache_.find(key);
		if (it == cache_.end()) {
			return false;
		}
		auto now = std::chrono::steady_clock::now();
		auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - it->second.timestamp).count();
		if (age_ms > TTL_MS) {
			cache_.erase(it);
			return false;
		}
		out = it->second.response;
		return true;
	}

	// Store response in cache
	void Put(const string &key, const HttpResponseData &response) {
		std::lock_guard<std::mutex> lock(mutex_);
		// Cleanup old entries periodically
		if (cache_.size() > 1000) {
			CleanupExpired();
		}
		cache_[key] = {response, std::chrono::steady_clock::now()};
	}

private:
	HttpRequestCache() = default;

	void CleanupExpired() {
		auto now = std::chrono::steady_clock::now();
		for (auto it = cache_.begin(); it != cache_.end();) {
			auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - it->second.timestamp).count();
			if (age_ms > TTL_MS) {
				it = cache_.erase(it);
			} else {
				++it;
			}
		}
	}

	static constexpr int64_t TTL_MS = 1000; // 1 second TTL
	std::mutex mutex_;
	std::unordered_map<string, HttpCacheEntry> cache_;
};

// Extract HTTP settings from context (call from main thread)
static HttpSettings ExtractHttpSettings(ClientContext &context, const string &url) {
	HttpSettings settings;
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &config = db.config;

	settings.timeout = 30;
	settings.keep_alive = true;
	settings.max_concurrency = DEFAULT_HTTP_MAX_CONCURRENT;
	settings.use_cache = true;
	settings.follow_redirects = true;

	ClientContextFileOpener opener(context);
	FileOpenerInfo info;
	info.file_path = url;

	FileOpener::TryGetCurrentSetting(&opener, "http_timeout", settings.timeout, &info);
	FileOpener::TryGetCurrentSetting(&opener, "http_keep_alive", settings.keep_alive, &info);
	FileOpener::TryGetCurrentSetting(&opener, "http_max_concurrency", settings.max_concurrency, &info);
	FileOpener::TryGetCurrentSetting(&opener, "http_request_cache", settings.use_cache, &info);
	FileOpener::TryGetCurrentSetting(&opener, "http_follow_redirects", settings.follow_redirects, &info);

	FileOpener::TryGetCurrentSetting(&opener, "http_proxy", settings.proxy, &info);
	FileOpener::TryGetCurrentSetting(&opener, "http_proxy_username", settings.proxy_username, &info);
	FileOpener::TryGetCurrentSetting(&opener, "http_proxy_password", settings.proxy_password, &info);

	KeyValueSecretReader secret_reader(opener, &info, "http");
	string proxy_from_secret;
	if (secret_reader.TryGetSecretKey<string>("http_proxy", proxy_from_secret) && !proxy_from_secret.empty()) {
		settings.proxy = proxy_from_secret;
	}
	secret_reader.TryGetSecretKey<string>("http_proxy_username", settings.proxy_username);
	secret_reader.TryGetSecretKey<string>("http_proxy_password", settings.proxy_password);

	// Check for custom user agent setting, otherwise use default
	string custom_user_agent;
	if (FileOpener::TryGetCurrentSetting(&opener, "http_user_agent", custom_user_agent, &info) &&
	    !custom_user_agent.empty()) {
		settings.user_agent = custom_user_agent;
	} else {
		settings.user_agent = StringUtil::Format("%s %s", config.UserAgent(), DuckDB::SourceID());
	}

	return settings;
}

// Thread-safe HTTP request execution (no context dependency)
static HttpResponseData ExecuteHttpRequestThreadSafe(const HttpSettings &settings, const string &url,
                                                     const string &method,
                                                     const duckdb_httplib_openssl::Headers &headers,
                                                     const string &request_body, const string &content_type) {
	HttpResponseData result;
	result.status_code = 0;
	result.content_length = -1;

	try {
		string proto_host_port, path;
		ParseUrl(url, proto_host_port, path);

		duckdb_httplib_openssl::Client client(proto_host_port);
		client.set_follow_location(settings.follow_redirects);
		client.set_decompress(false);
		client.enable_server_certificate_verification(false);

		auto timeout_sec = static_cast<time_t>(settings.timeout);
		client.set_read_timeout(timeout_sec, 0);
		client.set_write_timeout(timeout_sec, 0);
		client.set_connection_timeout(timeout_sec, 0);
		client.set_keep_alive(settings.keep_alive);

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

		duckdb_httplib_openssl::Headers req_headers = headers;
		if (req_headers.find("User-Agent") == req_headers.end()) {
			req_headers.insert({"User-Agent", settings.user_agent});
		}

		duckdb_httplib_openssl::Result res(nullptr, duckdb_httplib_openssl::Error::Unknown);

		if (StringUtil::CIEquals(method, "HEAD")) {
			res = client.Head(path, req_headers);
		} else if (StringUtil::CIEquals(method, "DELETE")) {
			res = client.Delete(path, req_headers);
		} else if (StringUtil::CIEquals(method, "POST")) {
			string ct = content_type.empty() ? "application/octet-stream" : content_type;
			res = client.Post(path, req_headers, request_body, ct);
		} else if (StringUtil::CIEquals(method, "PUT")) {
			string ct = content_type.empty() ? "application/octet-stream" : content_type;
			res = client.Put(path, req_headers, request_body, ct);
		} else if (StringUtil::CIEquals(method, "PATCH")) {
			string ct = content_type.empty() ? "application/octet-stream" : content_type;
			res = client.Patch(path, req_headers, request_body, ct);
		} else {
			res = client.Get(path, req_headers);
		}

		if (res.error() != duckdb_httplib_openssl::Error::Success) {
			result.error = "HTTP request failed: " + to_string(res.error());
			return result;
		}

		result.status_code = res->status;
		string response_body = res->body;

		for (auto &header : res->headers) {
			if (StringUtil::CIEquals(header.first, "Set-Cookie")) {
				result.cookies.push_back(ParseSetCookieHeader(header.second));
			} else {
				string normalized_key = NormalizeHeaderName(header.first);
				if (StringUtil::CIEquals(header.first, "Content-Type")) {
					result.content_type = header.second;
				} else if (StringUtil::CIEquals(header.first, "Content-Length")) {
					try {
						result.content_length = std::stoll(header.second);
					} catch (...) {
					}
				}
				bool found = false;
				for (idx_t i = 0; i < result.header_keys.size(); i++) {
					if (StringUtil::CIEquals(result.header_keys[i].GetValue<string>(), normalized_key)) {
						result.header_values[i] = Value(header.second);
						found = true;
						break;
					}
				}
				if (!found) {
					result.header_keys.push_back(Value(normalized_key));
					result.header_values.push_back(Value(header.second));
				}
			}
		}

		// Auto-decompress
		result.body = response_body;
		try {
			if (GZipFileSystem::CheckIsZip(response_body.data(), response_body.size())) {
				result.body = GZipFileSystem::UncompressGZIPString(response_body);
			} else if (CheckIsZstd(response_body.data(), response_body.size())) {
				result.body = DecompressZstd(response_body);
			}
		} catch (...) {
		}

	} catch (std::exception &e) {
		result.error = e.what();
	}

	return result;
}

// Convert Value headers to httplib Headers (supports both STRUCT and MAP)
static duckdb_httplib_openssl::Headers ValueToHttplibHeaders(const Value &headers_val) {
	duckdb_httplib_openssl::Headers headers;
	if (headers_val.IsNull()) {
		return headers;
	}

	auto &type = headers_val.type();

	// Handle MAP type: MAP {'key': 'value', ...}
	if (type.id() == LogicalTypeId::MAP) {
		auto &map_children = MapValue::GetChildren(headers_val);
		for (auto &entry : map_children) {
			auto &struct_children = StructValue::GetChildren(entry);
			if (struct_children.size() >= 2 && !struct_children[0].IsNull() && !struct_children[1].IsNull()) {
				headers.insert({struct_children[0].ToString(), struct_children[1].ToString()});
			}
		}
		return headers;
	}

	// Handle STRUCT type: {key: 'value', ...}
	if (type.id() == LogicalTypeId::STRUCT) {
		auto &child_types = StructType::GetChildTypes(type);
		auto &children = StructValue::GetChildren(headers_val);
		for (idx_t i = 0; i < child_types.size(); i++) {
			if (!children[i].IsNull()) {
				headers.insert({child_types[i].first, children[i].ToString()});
			}
		}
	}

	return headers;
}

// Build response Value from HttpResponseData
static Value BuildHttpResponseFromData(const HttpResponseData &data) {
	vector<Value> header_keys = data.header_keys;
	vector<Value> header_values = data.header_values;
	vector<Value> cookies = data.cookies;
	return BuildHttpResponseValue(data.status_code, data.content_type, data.content_length, header_keys, header_values,
	                              cookies, data.body);
}

// Parallel HTTP execution for scalar functions (with optional caching to prevent duplicate requests)
static void ExecuteHttpRequestsParallel(ClientContext &context, const vector<string> &urls, const string &method,
                                        const vector<duckdb_httplib_openssl::Headers> &headers_list,
                                        const vector<string> &bodies, const vector<string> &content_types,
                                        vector<HttpResponseData> &results) {
	idx_t count = urls.size();
	results.resize(count);

	if (count == 0) {
		return;
	}

	// Extract settings from context (main thread only)
	HttpSettings settings = ExtractHttpSettings(context, urls[0]);
	idx_t max_concurrent = settings.max_concurrency;
	bool use_cache = settings.use_cache;
	auto &cache = HttpRequestCache::Instance();

	// Check cache first and collect indices that need fetching
	vector<idx_t> to_fetch;
	vector<string> cache_keys(count);

	for (idx_t i = 0; i < count; i++) {
		if (use_cache) {
			cache_keys[i] = HttpRequestCache::MakeKey(urls[i], method, headers_list[i], bodies[i]);
			if (cache.TryGet(cache_keys[i], results[i])) {
				// Cache hit - result already populated
				continue;
			}
		}
		to_fetch.push_back(i);
	}

	if (to_fetch.empty()) {
		// All results from cache
		return;
	}

	// For single request or max_concurrent=1, execute sequentially
	if (to_fetch.size() == 1 || max_concurrent <= 1) {
		for (idx_t i : to_fetch) {
			results[i] =
			    ExecuteHttpRequestThreadSafe(settings, urls[i], method, headers_list[i], bodies[i], content_types[i]);
			if (use_cache) {
				cache.Put(cache_keys[i], results[i]);
			}
		}
		return;
	}

	// Parallel execution
	std::atomic<idx_t> next_idx {0};
	idx_t num_threads = std::min(static_cast<idx_t>(to_fetch.size()), max_concurrent);
	vector<std::thread> workers;

	for (idx_t t = 0; t < num_threads; t++) {
		workers.emplace_back([&, use_cache]() {
			while (true) {
				idx_t fetch_idx = next_idx.fetch_add(1);
				if (fetch_idx >= to_fetch.size()) {
					break;
				}
				idx_t i = to_fetch[fetch_idx];
				results[i] = ExecuteHttpRequestThreadSafe(settings, urls[i], method, headers_list[i], bodies[i],
				                                          content_types[i]);
				if (use_cache) {
					cache.Put(cache_keys[i], results[i]);
				}
			}
		});
	}

	for (auto &w : workers) {
		w.join();
	}
}

// Extract headers from STRUCT value (field names are header names, values are header values)
static void ExtractHeaders(const Value &headers_val, HTTPHeaders &req_headers) {
	if (headers_val.IsNull()) {
		return;
	}
	auto &struct_type = headers_val.type();
	auto &child_types = StructType::GetChildTypes(struct_type);
	auto &children = StructValue::GetChildren(headers_val);

	for (idx_t i = 0; i < child_types.size(); i++) {
		if (!children[i].IsNull()) {
			string key = child_types[i].first;
			string value = children[i].ToString();
			req_headers.Insert(key, value);
		}
	}
}

// Build URL with query parameters from STRUCT value
static string BuildUrlWithParams(const string &base_url, const Value &params_val) {
	if (params_val.IsNull()) {
		return base_url;
	}
	auto &struct_type = params_val.type();
	auto &child_types = StructType::GetChildTypes(struct_type);
	auto &children = StructValue::GetChildren(params_val);

	string url = base_url;
	bool has_query = base_url.find('?') != string::npos;

	for (idx_t i = 0; i < child_types.size(); i++) {
		if (!children[i].IsNull()) {
			string key = child_types[i].first;
			string value = children[i].ToString();
			url += has_query ? "&" : "?";
			url += StringUtil::URLEncode(key) + "=" + StringUtil::URLEncode(value);
			has_query = true;
		}
	}
	return url;
}

// Build application/x-www-form-urlencoded body from STRUCT value
static string BuildFormEncodedBody(const Value &params_val) {
	if (params_val.IsNull()) {
		return "";
	}
	auto &struct_type = params_val.type();
	auto &child_types = StructType::GetChildTypes(struct_type);
	auto &children = StructValue::GetChildren(params_val);

	string body;
	bool first = true;

	for (idx_t i = 0; i < child_types.size(); i++) {
		if (!children[i].IsNull()) {
			string key = child_types[i].first;
			string value = children[i].ToString();
			if (!first) {
				body += "&";
			}
			body += StringUtil::URLEncode(key) + "=" + StringUtil::URLEncode(value);
			first = false;
		}
	}
	return body;
}

// Parse URL into host and path components
static void ParseUrl(const string &url, string &proto_host_port, string &path) {
	// Find scheme
	auto scheme_end = url.find("://");
	if (scheme_end == string::npos) {
		throw IOException("Invalid URL: missing scheme");
	}

	// Find path start (first / after scheme://)
	auto path_start = url.find('/', scheme_end + 3);
	if (path_start == string::npos) {
		proto_host_port = url;
		path = "/";
	} else {
		proto_host_port = url.substr(0, path_start);
		path = url.substr(path_start);
	}
}

// Cookie struct type for parsed Set-Cookie headers
static LogicalType CreateCookieStructType() {
	child_list_t<LogicalType> cookie_children;
	cookie_children.push_back(make_pair("name", LogicalType::VARCHAR));
	cookie_children.push_back(make_pair("value", LogicalType::VARCHAR));
	cookie_children.push_back(make_pair("expires", LogicalType::VARCHAR));
	cookie_children.push_back(make_pair("max_age", LogicalType::INTEGER));
	cookie_children.push_back(make_pair("path", LogicalType::VARCHAR));
	cookie_children.push_back(make_pair("domain", LogicalType::VARCHAR));
	cookie_children.push_back(make_pair("secure", LogicalType::BOOLEAN));
	cookie_children.push_back(make_pair("httponly", LogicalType::BOOLEAN));
	cookie_children.push_back(make_pair("samesite", LogicalType::VARCHAR));
	return LogicalType::STRUCT(std::move(cookie_children));
}

// Parse a single Set-Cookie header value into a struct
static Value ParseSetCookieHeader(const string &cookie_str) {
	child_list_t<Value> cookie_values;
	string name, value, expires, path, domain, samesite;
	Value max_age = Value(LogicalType::INTEGER); // NULL by default
	bool secure = false, httponly = false;

	// Split by ';'
	vector<string> parts;
	idx_t start = 0;
	for (idx_t i = 0; i <= cookie_str.size(); i++) {
		if (i == cookie_str.size() || cookie_str[i] == ';') {
			if (i > start) {
				string part = cookie_str.substr(start, i - start);
				StringUtil::Trim(part);
				if (!part.empty()) {
					parts.push_back(part);
				}
			}
			start = i + 1;
		}
	}

	// First part is name=value
	if (!parts.empty()) {
		auto eq_pos = parts[0].find('=');
		if (eq_pos != string::npos) {
			name = parts[0].substr(0, eq_pos);
			value = parts[0].substr(eq_pos + 1);
			StringUtil::Trim(name);
			StringUtil::Trim(value);
		} else {
			name = parts[0];
		}
	}

	// Parse remaining attributes
	for (idx_t i = 1; i < parts.size(); i++) {
		string &part = parts[i];
		auto eq_pos = part.find('=');
		string attr_name, attr_value;
		if (eq_pos != string::npos) {
			attr_name = part.substr(0, eq_pos);
			attr_value = part.substr(eq_pos + 1);
			StringUtil::Trim(attr_name);
			StringUtil::Trim(attr_value);
		} else {
			attr_name = part;
			StringUtil::Trim(attr_name);
		}

		// Case-insensitive attribute matching
		if (StringUtil::CIEquals(attr_name, "expires")) {
			expires = attr_value;
		} else if (StringUtil::CIEquals(attr_name, "max-age")) {
			try {
				max_age = Value::INTEGER(std::stoi(attr_value));
			} catch (...) {
				// Invalid max-age, keep NULL
			}
		} else if (StringUtil::CIEquals(attr_name, "path")) {
			path = attr_value;
		} else if (StringUtil::CIEquals(attr_name, "domain")) {
			domain = attr_value;
		} else if (StringUtil::CIEquals(attr_name, "secure")) {
			secure = true;
		} else if (StringUtil::CIEquals(attr_name, "httponly")) {
			httponly = true;
		} else if (StringUtil::CIEquals(attr_name, "samesite")) {
			samesite = attr_value;
		}
	}

	cookie_values.push_back(make_pair("name", Value(name)));
	cookie_values.push_back(make_pair("value", Value(value)));
	cookie_values.push_back(make_pair("expires", expires.empty() ? Value(LogicalType::VARCHAR) : Value(expires)));
	cookie_values.push_back(make_pair("max_age", max_age));
	cookie_values.push_back(make_pair("path", path.empty() ? Value(LogicalType::VARCHAR) : Value(path)));
	cookie_values.push_back(make_pair("domain", domain.empty() ? Value(LogicalType::VARCHAR) : Value(domain)));
	cookie_values.push_back(make_pair("secure", Value::BOOLEAN(secure)));
	cookie_values.push_back(make_pair("httponly", Value::BOOLEAN(httponly)));
	cookie_values.push_back(make_pair("samesite", samesite.empty() ? Value(LogicalType::VARCHAR) : Value(samesite)));

	return Value::STRUCT(std::move(cookie_values));
}

// Core HTTP request logic - used by both scalar and table functions
// Supports GET, HEAD, POST, PUT, PATCH, DELETE methods
// For POST/PUT/PATCH, request_body contains the body to send
static void PerformHttpRequestCore(ClientContext &context, const string &url, const string &method,
                                   const Value &headers_val, const string &request_body, const string &content_type,
                                   int32_t &status_code, string &resp_content_type, int64_t &resp_content_length,
                                   vector<Value> &header_keys, vector<Value> &header_values, vector<Value> &cookies,
                                   string &final_body) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &config = db.config;

	// Read HTTP settings from extension options (httpfs settings)
	uint64_t http_timeout = 30;
	uint64_t http_retries = 3;
	bool http_keep_alive = true;
	bool http_follow_redirects = true;

	// Create file opener to read settings
	ClientContextFileOpener opener(context);
	FileOpenerInfo info;
	info.file_path = url;

	// Try to read httpfs extension settings
	FileOpener::TryGetCurrentSetting(&opener, "http_timeout", http_timeout, &info);
	FileOpener::TryGetCurrentSetting(&opener, "http_retries", http_retries, &info);
	FileOpener::TryGetCurrentSetting(&opener, "http_keep_alive", http_keep_alive, &info);
	FileOpener::TryGetCurrentSetting(&opener, "http_follow_redirects", http_follow_redirects, &info);

	// Read proxy settings - first from generic settings, then fall back to secrets
	string http_proxy;
	string http_proxy_username;
	string http_proxy_password;
	FileOpener::TryGetCurrentSetting(&opener, "http_proxy", http_proxy, &info);
	FileOpener::TryGetCurrentSetting(&opener, "http_proxy_username", http_proxy_username, &info);
	FileOpener::TryGetCurrentSetting(&opener, "http_proxy_password", http_proxy_password, &info);

	// Try reading proxy from secrets (httpfs style)
	KeyValueSecretReader secret_reader(opener, &info, "http");
	string proxy_from_secret;
	if (secret_reader.TryGetSecretKey<string>("http_proxy", proxy_from_secret) && !proxy_from_secret.empty()) {
		http_proxy = proxy_from_secret;
	}
	secret_reader.TryGetSecretKey<string>("http_proxy_username", http_proxy_username);
	secret_reader.TryGetSecretKey<string>("http_proxy_password", http_proxy_password);

	// Parse URL
	string proto_host_port, path;
	ParseUrl(url, proto_host_port, path);

	// Create httplib client with SSL support
	duckdb_httplib_openssl::Client client(proto_host_port);
	client.set_follow_location(http_follow_redirects);
	client.set_decompress(false); // We handle decompression ourselves
	client.enable_server_certificate_verification(false);

	// Apply timeout settings
	auto timeout_sec = static_cast<time_t>(http_timeout);
	client.set_read_timeout(timeout_sec, 0);
	client.set_write_timeout(timeout_sec, 0);
	client.set_connection_timeout(timeout_sec, 0);
	client.set_keep_alive(http_keep_alive);

	// Configure proxy
	if (!http_proxy.empty()) {
		string proxy_host;
		idx_t proxy_port = 80;
		HTTPUtil::ParseHTTPProxyHost(http_proxy, proxy_host, proxy_port);
		client.set_proxy(proxy_host, static_cast<int>(proxy_port));

		if (!http_proxy_username.empty()) {
			client.set_proxy_basic_auth(http_proxy_username, http_proxy_password);
		}
	}

	// Build headers - add user headers first so they can override defaults
	duckdb_httplib_openssl::Headers headers;

	if (!headers_val.IsNull()) {
		auto &struct_type = headers_val.type();
		auto &child_types = StructType::GetChildTypes(struct_type);
		auto &children = StructValue::GetChildren(headers_val);
		for (idx_t i = 0; i < child_types.size(); i++) {
			if (!children[i].IsNull()) {
				headers.insert({child_types[i].first, children[i].ToString()});
			}
		}
	}

	// Set default User-Agent only if not provided by user
	if (headers.find("User-Agent") == headers.end()) {
		headers.insert({"User-Agent", StringUtil::Format("%s %s", db.config.UserAgent(), DuckDB::SourceID())});
	}

	string response_body;
	status_code = 0;

	duckdb_httplib_openssl::Result res =
	    duckdb_httplib_openssl::Result(nullptr, duckdb_httplib_openssl::Error::Unknown);

	if (StringUtil::CIEquals(method, "HEAD")) {
		res = client.Head(path, headers);
	} else if (StringUtil::CIEquals(method, "DELETE")) {
		res = client.Delete(path, headers);
	} else if (StringUtil::CIEquals(method, "POST")) {
		// Check if Content-Type was provided in headers
		string ct = content_type;
		if (ct.empty()) {
			auto ct_it = headers.find("Content-Type");
			if (ct_it != headers.end()) {
				ct = ct_it->second;
				headers.erase(ct_it); // Remove to avoid duplicate
			} else {
				ct = "application/octet-stream";
			}
		}
		res = client.Post(path, headers, request_body, ct);
	} else if (StringUtil::CIEquals(method, "PUT")) {
		string ct = content_type;
		if (ct.empty()) {
			auto ct_it = headers.find("Content-Type");
			if (ct_it != headers.end()) {
				ct = ct_it->second;
				headers.erase(ct_it); // Remove to avoid duplicate
			} else {
				ct = "application/octet-stream";
			}
		}
		res = client.Put(path, headers, request_body, ct);
	} else if (StringUtil::CIEquals(method, "PATCH")) {
		string ct = content_type;
		if (ct.empty()) {
			auto ct_it = headers.find("Content-Type");
			if (ct_it != headers.end()) {
				ct = ct_it->second;
				headers.erase(ct_it); // Remove to avoid duplicate
			} else {
				ct = "application/octet-stream";
			}
		}
		res = client.Patch(path, headers, request_body, ct);
	} else {
		// GET (default)
		res = client.Get(path, headers);
	}

	if (res.error() != duckdb_httplib_openssl::Error::Success) {
		throw IOException("HTTP request failed: %s", to_string(res.error()));
	}

	status_code = res->status;
	response_body = res->body;

	// Initialize extracted fields
	resp_content_type = "";
	resp_content_length = -1;

	// Collect headers - Set-Cookie goes to cookies array, others to headers map
	for (auto &header : res->headers) {
		if (StringUtil::CIEquals(header.first, "Set-Cookie")) {
			cookies.push_back(ParseSetCookieHeader(header.second));
		} else {
			string normalized_key = NormalizeHeaderName(header.first);
			// Extract convenience fields
			if (StringUtil::CIEquals(header.first, "Content-Type")) {
				resp_content_type = header.second;
			} else if (StringUtil::CIEquals(header.first, "Content-Length")) {
				try {
					resp_content_length = std::stoll(header.second);
				} catch (...) {
					// Invalid content-length, keep -1
				}
			}

			// For duplicate non-cookie headers, last value wins
			bool found = false;
			for (idx_t i = 0; i < header_keys.size(); i++) {
				if (StringUtil::CIEquals(header_keys[i].GetValue<string>(), normalized_key)) {
					header_values[i] = Value(header.second);
					found = true;
					break;
				}
			}
			if (!found) {
				header_keys.push_back(Value(normalized_key));
				header_values.push_back(Value(header.second));
			}
		}
	}

	// Auto-decompress based on magic bytes (using core GZipFileSystem::CheckIsZip for gzip)
	final_body = response_body;

	try {
		if (GZipFileSystem::CheckIsZip(response_body.data(), response_body.size())) {
			final_body = GZipFileSystem::UncompressGZIPString(response_body);
		} else if (CheckIsZstd(response_body.data(), response_body.size())) {
			final_body = DecompressZstd(response_body);
		}
	} catch (...) {
		final_body = response_body;
	}
}

// Convenience overload without body (for GET, HEAD, DELETE)
static void PerformHttpRequestCore(ClientContext &context, const string &url, const string &method,
                                   const Value &headers_val, int32_t &status_code, string &resp_content_type,
                                   int64_t &resp_content_length, vector<Value> &header_keys,
                                   vector<Value> &header_values, vector<Value> &cookies, string &final_body) {
	PerformHttpRequestCore(context, url, method, headers_val, "", "", status_code, resp_content_type,
	                       resp_content_length, header_keys, header_values, cookies, final_body);
}

///===--------------------------------------------------------------------===//
// HTTP_HEADERS Extension Type
//===--------------------------------------------------------------------===//
// HTTP_HEADERS is an extension type wrapping MAP(VARCHAR, VARCHAR)
// Keys are normalized to Title-Case for consistent access
// The [] operator uses case-insensitive lookup via custom map_extract_value

struct HttpHeadersType {
	static LogicalType GetDefault() {
		auto type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
		type.SetAlias("HTTP_HEADERS");
		return type;
	}
};

// Headers type: HTTP_HEADERS (MAP with Title-Case normalized keys)
static LogicalType CreateHeadersMapType() {
	return HttpHeadersType::GetDefault();
}

// Case-insensitive map_extract_value for HTTP_HEADERS type
// This enables headers['content-type'] to work regardless of case
static void HttpHeadersExtractValue(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &headers_vec = args.data[0];
	auto &key_vec = args.data[1];
	auto count = args.size();

	for (idx_t i = 0; i < count; i++) {
		auto headers_val = headers_vec.GetValue(i);
		auto key_val = key_vec.GetValue(i);

		if (headers_val.IsNull() || key_val.IsNull()) {
			result.SetValue(i, Value(LogicalType::VARCHAR));
			continue;
		}

		string search_key = key_val.GetValue<string>();

		// Iterate through MAP entries for case-insensitive lookup
		auto &map_children = MapValue::GetChildren(headers_val);
		bool found = false;
		for (auto &entry : map_children) {
			auto &kv = StructValue::GetChildren(entry);
			if (kv.size() >= 2 && !kv[0].IsNull()) {
				string key = kv[0].GetValue<string>();
				if (StringUtil::CIEquals(key, search_key)) {
					result.SetValue(i, kv[1]);
					found = true;
					break;
				}
			}
		}

		if (!found) {
			result.SetValue(i, Value(LogicalType::VARCHAR));
		}
	}
}

// Build response STRUCT value
static Value BuildHttpResponseValue(int32_t status_code, const string &content_type, int64_t content_length,
                                    vector<Value> &header_keys, vector<Value> &header_values, vector<Value> &cookies,
                                    const string &body) {
	child_list_t<Value> struct_values;
	struct_values.push_back(make_pair("status", Value::INTEGER(status_code)));
	struct_values.push_back(
	    make_pair("content_type", content_type.empty() ? Value(LogicalType::VARCHAR) : Value(content_type)));
	struct_values.push_back(
	    make_pair("content_length", content_length < 0 ? Value(LogicalType::BIGINT) : Value::BIGINT(content_length)));
	struct_values.push_back(
	    make_pair("headers", Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, header_keys, header_values)));
	struct_values.push_back(make_pair("cookies", Value::LIST(CreateCookieStructType(), std::move(cookies))));
	struct_values.push_back(make_pair("body", Value::BLOB_RAW(body)));
	return Value::STRUCT(std::move(struct_values));
}

// http_response STRUCT type
static LogicalType CreateHttpResponseType() {
	child_list_t<LogicalType> struct_children;
	struct_children.push_back(make_pair("status", LogicalType::INTEGER));
	struct_children.push_back(make_pair("content_type", LogicalType::VARCHAR));
	struct_children.push_back(make_pair("content_length", LogicalType::BIGINT));
	struct_children.push_back(make_pair("headers", CreateHeadersMapType()));
	struct_children.push_back(make_pair("cookies", LogicalType::LIST(CreateCookieStructType())));
	struct_children.push_back(make_pair("body", LogicalType::BLOB));
	return LogicalType::STRUCT(std::move(struct_children));
}

// byte_range(offset, length) -> 'bytes=offset-end' string for Range header
static void ByteRangeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &offset_vec = args.data[0];
	auto &length_vec = args.data[1];
	auto count = args.size();

	BinaryExecutor::Execute<int64_t, int64_t, string_t>(
	    offset_vec, length_vec, result, count, [&](int64_t offset, int64_t length) {
		    int64_t end = offset + length - 1;
		    string range_str = "bytes=" + std::to_string(offset) + "-" + std::to_string(end);
		    return StringVector::AddString(result, range_str);
	    });
}

// http_range_header(offset, length) -> STRUCT with Range header for use as headers parameter
static void HttpRangeHeaderFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &offset_vec = args.data[0];
	auto &length_vec = args.data[1];
	auto count = args.size();

	for (idx_t i = 0; i < count; i++) {
		auto offset = offset_vec.GetValue(i).GetValue<int64_t>();
		auto length = length_vec.GetValue(i).GetValue<int64_t>();
		int64_t end = offset + length - 1;
		string range_str = "bytes=" + std::to_string(offset) + "-" + std::to_string(end);

		child_list_t<Value> struct_values;
		struct_values.push_back(make_pair("Range", Value(range_str)));
		result.SetValue(i, Value::STRUCT(std::move(struct_values)));
	}
}

// Bind function for http_range_header - handles named parameter reordering
static unique_ptr<FunctionData> HttpRangeHeaderBind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() == 2) {
		idx_t offset_idx = 0;
		idx_t length_idx = 1;

		for (idx_t i = 0; i < arguments.size(); i++) {
			if (StringUtil::CIEquals(arguments[i]->alias, "offset")) {
				offset_idx = i;
			} else if (StringUtil::CIEquals(arguments[i]->alias, "length")) {
				length_idx = i;
			}
		}

		if (offset_idx == 1 && length_idx == 0) {
			std::swap(arguments[0], arguments[1]);
		}
	}
	return nullptr;
}

// Return type for http_range_header function
static LogicalType CreateHttpRangeHeaderType() {
	child_list_t<LogicalType> struct_children;
	struct_children.push_back(make_pair("Range", LogicalType::VARCHAR));
	return LogicalType::STRUCT(std::move(struct_children));
}

//------------------------------------------------------------------------------
// SCALAR FUNCTIONS
//------------------------------------------------------------------------------

// http_head(url)
static void HttpHeadScalar1(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count);

	for (idx_t i = 0; i < count; i++) {
		urls[i] = url_vec.GetValue(i).GetValue<string>();
	}

	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "HEAD", headers_list, bodies, content_types, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// http_head(url, headers)
static void HttpHeadScalar2(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count);

	for (idx_t i = 0; i < count; i++) {
		urls[i] = url_vec.GetValue(i).GetValue<string>();
		headers_list[i] = ValueToHttplibHeaders(headers_vec.GetValue(i));
	}

	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "HEAD", headers_list, bodies, content_types, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// http_head(url, headers, params)
static void HttpHeadScalar3(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto &params_vec = args.data[2];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count);

	for (idx_t i = 0; i < count; i++) {
		auto base_url = url_vec.GetValue(i).GetValue<string>();
		auto params = params_vec.GetValue(i);
		urls[i] = BuildUrlWithParams(base_url, params);
		headers_list[i] = ValueToHttplibHeaders(headers_vec.GetValue(i));
	}

	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "HEAD", headers_list, bodies, content_types, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// Bind function for http_head(url, headers, params) - reorders arguments based on aliases
static unique_ptr<FunctionData> HttpHeadScalar3Bind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() == 3) {
		idx_t headers_idx = 1;
		idx_t params_idx = 2;

		for (idx_t i = 1; i < arguments.size(); i++) {
			if (StringUtil::CIEquals(arguments[i]->alias, "headers")) {
				headers_idx = i;
			} else if (StringUtil::CIEquals(arguments[i]->alias, "params")) {
				params_idx = i;
			}
		}

		if (headers_idx == 2 && params_idx == 1) {
			std::swap(arguments[1], arguments[2]);
		}
	}
	return nullptr;
}

// http_get(url)
static void HttpGetScalar1(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto count = args.size();
	auto &context = state.GetContext();

	// Collect all inputs
	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count);

	for (idx_t i = 0; i < count; i++) {
		urls[i] = url_vec.GetValue(i).GetValue<string>();
	}

	// Execute in parallel
	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "GET", headers_list, bodies, content_types, results);

	// Set results
	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// http_get(url, headers)
static void HttpGetScalar2(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count);

	for (idx_t i = 0; i < count; i++) {
		urls[i] = url_vec.GetValue(i).GetValue<string>();
		headers_list[i] = ValueToHttplibHeaders(headers_vec.GetValue(i));
	}

	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "GET", headers_list, bodies, content_types, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// http_get(url, headers, params)
static void HttpGetScalar3(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto &params_vec = args.data[2];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count);

	for (idx_t i = 0; i < count; i++) {
		auto base_url = url_vec.GetValue(i).GetValue<string>();
		auto params = params_vec.GetValue(i);
		urls[i] = BuildUrlWithParams(base_url, params);
		headers_list[i] = ValueToHttplibHeaders(headers_vec.GetValue(i));
	}

	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "GET", headers_list, bodies, content_types, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// Bind function for http_get(url, headers, params) - reorders arguments based on aliases
static unique_ptr<FunctionData> HttpGetScalar3Bind(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	// Check if we have named arguments (aliases) and need to reorder
	if (arguments.size() == 3) {
		// Find indices for headers and params based on alias names
		idx_t headers_idx = 1; // default position
		idx_t params_idx = 2;  // default position

		for (idx_t i = 1; i < arguments.size(); i++) {
			if (StringUtil::CIEquals(arguments[i]->alias, "headers")) {
				headers_idx = i;
			} else if (StringUtil::CIEquals(arguments[i]->alias, "params")) {
				params_idx = i;
			}
		}

		// Swap if needed to ensure headers is at index 1 and params at index 2
		if (headers_idx == 2 && params_idx == 1) {
			std::swap(arguments[1], arguments[2]);
		}
	}
	return nullptr;
}

//------------------------------------------------------------------------------
// http_delete scalar functions
//------------------------------------------------------------------------------

// http_delete(url)
static void HttpDeleteScalar1(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count);

	for (idx_t i = 0; i < count; i++) {
		urls[i] = url_vec.GetValue(i).GetValue<string>();
	}

	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "DELETE", headers_list, bodies, content_types, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// http_delete(url, headers)
static void HttpDeleteScalar2(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count);

	for (idx_t i = 0; i < count; i++) {
		urls[i] = url_vec.GetValue(i).GetValue<string>();
		headers_list[i] = ValueToHttplibHeaders(headers_vec.GetValue(i));
	}

	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "DELETE", headers_list, bodies, content_types, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// http_delete(url, headers, params)
static void HttpDeleteScalar3(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto &params_vec = args.data[2];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count);

	for (idx_t i = 0; i < count; i++) {
		auto base_url = url_vec.GetValue(i).GetValue<string>();
		auto params = params_vec.GetValue(i);
		urls[i] = BuildUrlWithParams(base_url, params);
		headers_list[i] = ValueToHttplibHeaders(headers_vec.GetValue(i));
	}

	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "DELETE", headers_list, bodies, content_types, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// Bind function for http_delete(url, headers, params) - reorders arguments based on aliases
static unique_ptr<FunctionData> HttpDeleteScalar3Bind(ClientContext &context, ScalarFunction &bound_function,
                                                      vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() == 3) {
		idx_t headers_idx = 1;
		idx_t params_idx = 2;

		for (idx_t i = 1; i < arguments.size(); i++) {
			if (StringUtil::CIEquals(arguments[i]->alias, "headers")) {
				headers_idx = i;
			} else if (StringUtil::CIEquals(arguments[i]->alias, "params")) {
				params_idx = i;
			}
		}

		if (headers_idx == 2 && params_idx == 1) {
			std::swap(arguments[1], arguments[2]);
		}
	}
	return nullptr;
}

//------------------------------------------------------------------------------
// http_post scalar functions
//------------------------------------------------------------------------------

// http_post(url)
static void HttpPostScalar1(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count, "application/octet-stream");

	for (idx_t i = 0; i < count; i++) {
		urls[i] = url_vec.GetValue(i).GetValue<string>();
	}

	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "POST", headers_list, bodies, content_types, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// http_post(url, headers)
static void HttpPostScalar2(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count);

	for (idx_t i = 0; i < count; i++) {
		urls[i] = url_vec.GetValue(i).GetValue<string>();
		headers_list[i] = ValueToHttplibHeaders(headers_vec.GetValue(i));
	}

	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "POST", headers_list, bodies, content_types, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// http_post(url, headers, params)
static void HttpPostScalar3(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto &params_vec = args.data[2];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count);

	for (idx_t i = 0; i < count; i++) {
		auto base_url = url_vec.GetValue(i).GetValue<string>();
		auto params = params_vec.GetValue(i);
		urls[i] = BuildUrlWithParams(base_url, params);
		headers_list[i] = ValueToHttplibHeaders(headers_vec.GetValue(i));
	}

	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "POST", headers_list, bodies, content_types, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// Bind function for http_post(url, headers, params) - reorders arguments based on aliases
static unique_ptr<FunctionData> HttpPostScalar3Bind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() == 3) {
		idx_t headers_idx = 1;
		idx_t params_idx = 2;

		for (idx_t i = 1; i < arguments.size(); i++) {
			if (StringUtil::CIEquals(arguments[i]->alias, "headers")) {
				headers_idx = i;
			} else if (StringUtil::CIEquals(arguments[i]->alias, "params")) {
				params_idx = i;
			}
		}

		if (headers_idx == 2 && params_idx == 1) {
			std::swap(arguments[1], arguments[2]);
		}
	}
	return nullptr;
}

//------------------------------------------------------------------------------
// http_put scalar functions
//------------------------------------------------------------------------------

// http_put(url)
static void HttpPutScalar1(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count, "application/octet-stream");

	for (idx_t i = 0; i < count; i++) {
		urls[i] = url_vec.GetValue(i).GetValue<string>();
	}

	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "PUT", headers_list, bodies, content_types, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// http_put(url, headers)
static void HttpPutScalar2(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count);

	for (idx_t i = 0; i < count; i++) {
		urls[i] = url_vec.GetValue(i).GetValue<string>();
		headers_list[i] = ValueToHttplibHeaders(headers_vec.GetValue(i));
	}

	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "PUT", headers_list, bodies, content_types, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// http_put(url, headers, params)
static void HttpPutScalar3(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto &params_vec = args.data[2];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count);

	for (idx_t i = 0; i < count; i++) {
		auto base_url = url_vec.GetValue(i).GetValue<string>();
		auto params = params_vec.GetValue(i);
		urls[i] = BuildUrlWithParams(base_url, params);
		headers_list[i] = ValueToHttplibHeaders(headers_vec.GetValue(i));
	}

	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "PUT", headers_list, bodies, content_types, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// Bind function for http_put(url, headers, params) - reorders arguments based on aliases
static unique_ptr<FunctionData> HttpPutScalar3Bind(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() == 3) {
		idx_t headers_idx = 1;
		idx_t params_idx = 2;

		for (idx_t i = 1; i < arguments.size(); i++) {
			if (StringUtil::CIEquals(arguments[i]->alias, "headers")) {
				headers_idx = i;
			} else if (StringUtil::CIEquals(arguments[i]->alias, "params")) {
				params_idx = i;
			}
		}

		if (headers_idx == 2 && params_idx == 1) {
			std::swap(arguments[1], arguments[2]);
		}
	}
	return nullptr;
}

//------------------------------------------------------------------------------
// http_patch scalar functions
//------------------------------------------------------------------------------

// http_patch(url)
static void HttpPatchScalar1(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count, "application/octet-stream");

	for (idx_t i = 0; i < count; i++) {
		urls[i] = url_vec.GetValue(i).GetValue<string>();
	}

	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "PATCH", headers_list, bodies, content_types, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// http_patch(url, headers)
static void HttpPatchScalar2(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count);

	for (idx_t i = 0; i < count; i++) {
		urls[i] = url_vec.GetValue(i).GetValue<string>();
		headers_list[i] = ValueToHttplibHeaders(headers_vec.GetValue(i));
	}

	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "PATCH", headers_list, bodies, content_types, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// http_patch(url, headers, params)
static void HttpPatchScalar3(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto &params_vec = args.data[2];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count);

	for (idx_t i = 0; i < count; i++) {
		auto base_url = url_vec.GetValue(i).GetValue<string>();
		auto params = params_vec.GetValue(i);
		urls[i] = BuildUrlWithParams(base_url, params);
		headers_list[i] = ValueToHttplibHeaders(headers_vec.GetValue(i));
	}

	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "PATCH", headers_list, bodies, content_types, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// Bind function for http_patch(url, headers, params) - reorders arguments based on aliases
static unique_ptr<FunctionData> HttpPatchScalar3Bind(ClientContext &context, ScalarFunction &bound_function,
                                                     vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() == 3) {
		idx_t headers_idx = 1;
		idx_t params_idx = 2;

		for (idx_t i = 1; i < arguments.size(); i++) {
			if (StringUtil::CIEquals(arguments[i]->alias, "headers")) {
				headers_idx = i;
			} else if (StringUtil::CIEquals(arguments[i]->alias, "params")) {
				params_idx = i;
			}
		}

		if (headers_idx == 2 && params_idx == 1) {
			std::swap(arguments[1], arguments[2]);
		}
	}
	return nullptr;
}

//------------------------------------------------------------------------------
// http_post_form scalar functions (application/x-www-form-urlencoded)
//------------------------------------------------------------------------------

// http_post_form(url, params)
static void HttpPostFormScalar2(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &params_vec = args.data[1];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count, "application/x-www-form-urlencoded");

	for (idx_t i = 0; i < count; i++) {
		urls[i] = url_vec.GetValue(i).GetValue<string>();
		bodies[i] = BuildFormEncodedBody(params_vec.GetValue(i));
	}

	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "POST", headers_list, bodies, content_types, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// http_post_form(url, params, headers)
static void HttpPostFormScalar3(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &params_vec = args.data[1];
	auto &headers_vec = args.data[2];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<string> bodies(count);
	vector<string> content_types(count, "application/x-www-form-urlencoded");

	for (idx_t i = 0; i < count; i++) {
		urls[i] = url_vec.GetValue(i).GetValue<string>();
		bodies[i] = BuildFormEncodedBody(params_vec.GetValue(i));
		headers_list[i] = ValueToHttplibHeaders(headers_vec.GetValue(i));
	}

	vector<HttpResponseData> results;
	ExecuteHttpRequestsParallel(context, urls, "POST", headers_list, bodies, content_types, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// Bind function for http_post_form(url, params, headers) - allows params/headers reordering
static unique_ptr<FunctionData> HttpPostFormScalar3Bind(ClientContext &context, ScalarFunction &bound_function,
                                                        vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() == 3) {
		idx_t params_idx = 1;
		idx_t headers_idx = 2;

		for (idx_t i = 1; i < arguments.size(); i++) {
			if (StringUtil::CIEquals(arguments[i]->alias, "params")) {
				params_idx = i;
			} else if (StringUtil::CIEquals(arguments[i]->alias, "headers")) {
				headers_idx = i;
			}
		}

		if (params_idx == 2 && headers_idx == 1) {
			std::swap(arguments[1], arguments[2]);
		}
	}
	return nullptr;
}

//------------------------------------------------------------------------------
// MULTIPART FORM DATA FUNCTIONS
//------------------------------------------------------------------------------

// Convert DuckDB LIST of STRUCTs to UploadFormDataItems
// Expected struct: {name: VARCHAR, content: BLOB/VARCHAR, filename: VARCHAR (optional), content_type: VARCHAR
// (optional)}
static duckdb_httplib_openssl::UploadFormDataItems ValueToMultipartItems(const Value &files_val,
                                                                         const Value &fields_val) {
	duckdb_httplib_openssl::UploadFormDataItems items;

	// Process files (LIST of STRUCTs)
	if (!files_val.IsNull() && files_val.type().id() == LogicalTypeId::LIST) {
		auto &file_list = ListValue::GetChildren(files_val);
		for (auto &file_val : file_list) {
			if (file_val.IsNull() || file_val.type().id() != LogicalTypeId::STRUCT) {
				continue;
			}

			duckdb_httplib_openssl::UploadFormData item;
			auto &struct_type = file_val.type();
			auto &child_types = StructType::GetChildTypes(struct_type);
			auto &children = StructValue::GetChildren(file_val);

			for (idx_t i = 0; i < child_types.size(); i++) {
				if (children[i].IsNull()) {
					continue;
				}
				string field_name = StringUtil::Lower(child_types[i].first);
				if (field_name == "name") {
					item.name = children[i].ToString();
				} else if (field_name == "content") {
					// Handle both BLOB and VARCHAR content
					if (children[i].type().id() == LogicalTypeId::BLOB) {
						auto blob = children[i].GetValueUnsafe<string_t>();
						item.content = string(blob.GetData(), blob.GetSize());
					} else {
						item.content = children[i].ToString();
					}
				} else if (field_name == "filename") {
					item.filename = children[i].ToString();
				} else if (field_name == "content_type") {
					item.content_type = children[i].ToString();
				}
			}

			// Only add if name and content are provided
			if (!item.name.empty() && !item.content.empty()) {
				items.push_back(item);
			}
		}
	}

	// Process form fields (STRUCT)
	if (!fields_val.IsNull() && fields_val.type().id() == LogicalTypeId::STRUCT) {
		auto &struct_type = fields_val.type();
		auto &child_types = StructType::GetChildTypes(struct_type);
		auto &children = StructValue::GetChildren(fields_val);

		for (idx_t i = 0; i < child_types.size(); i++) {
			if (children[i].IsNull()) {
				continue;
			}
			duckdb_httplib_openssl::UploadFormData item;
			item.name = child_types[i].first;
			item.content = children[i].ToString();
			// No filename = regular form field
			items.push_back(item);
		}
	}

	return items;
}

// Thread-safe multipart POST execution
static HttpResponseData ExecuteMultipartPostThreadSafe(const HttpSettings &settings, const string &url,
                                                       const duckdb_httplib_openssl::Headers &headers,
                                                       const duckdb_httplib_openssl::UploadFormDataItems &items) {
	HttpResponseData result;
	result.status_code = 0;
	result.content_length = -1;

	try {
		string proto_host_port, path;
		ParseUrl(url, proto_host_port, path);

		duckdb_httplib_openssl::Client client(proto_host_port);
		client.set_follow_location(settings.follow_redirects);
		client.set_decompress(false);
		client.enable_server_certificate_verification(false);

		auto timeout_sec = static_cast<time_t>(settings.timeout);
		client.set_read_timeout(timeout_sec, 0);
		client.set_write_timeout(timeout_sec, 0);
		client.set_connection_timeout(timeout_sec, 0);
		client.set_keep_alive(settings.keep_alive);

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

		duckdb_httplib_openssl::Headers req_headers = headers;
		if (req_headers.find("User-Agent") == req_headers.end()) {
			req_headers.insert({"User-Agent", settings.user_agent});
		}

		// Execute multipart POST
		auto res = client.Post(path, req_headers, items);

		if (res.error() != duckdb_httplib_openssl::Error::Success) {
			result.error = "HTTP multipart request failed: " + to_string(res.error());
			return result;
		}

		result.status_code = res->status;
		string response_body = res->body;

		for (auto &header : res->headers) {
			if (StringUtil::CIEquals(header.first, "Set-Cookie")) {
				result.cookies.push_back(ParseSetCookieHeader(header.second));
			} else {
				string normalized_key = NormalizeHeaderName(header.first);
				if (StringUtil::CIEquals(header.first, "Content-Type")) {
					result.content_type = header.second;
				} else if (StringUtil::CIEquals(header.first, "Content-Length")) {
					try {
						result.content_length = std::stoll(header.second);
					} catch (...) {
					}
				}
				bool found = false;
				for (idx_t i = 0; i < result.header_keys.size(); i++) {
					if (StringUtil::CIEquals(result.header_keys[i].GetValue<string>(), normalized_key)) {
						auto &values_list = result.header_values[i];
						auto existing = ListValue::GetChildren(values_list);
						existing.push_back(Value(header.second));
						result.header_values[i] = Value::LIST(LogicalType::VARCHAR, existing);
						found = true;
						break;
					}
				}
				if (!found) {
					result.header_keys.push_back(Value(normalized_key));
					result.header_values.push_back(
					    Value::LIST(LogicalType(LogicalTypeId::VARCHAR), vector<Value> {Value(header.second)}));
				}
			}
		}

		result.body = response_body;

		// Handle compression
		try {
			if (GZipFileSystem::CheckIsZip(response_body.data(), response_body.size())) {
				result.body = GZipFileSystem::UncompressGZIPString(response_body);
			} else if (CheckIsZstd(response_body.data(), response_body.size())) {
				result.body = DecompressZstd(response_body);
			}
		} catch (...) {
		}

	} catch (std::exception &e) {
		result.error = e.what();
	}

	return result;
}

// Parallel execution for multipart POST
static void ExecuteMultipartPostParallel(ClientContext &context, const vector<string> &urls,
                                         const vector<duckdb_httplib_openssl::Headers> &headers_list,
                                         const vector<duckdb_httplib_openssl::UploadFormDataItems> &items_list,
                                         vector<HttpResponseData> &results) {
	idx_t count = urls.size();
	results.resize(count);

	if (count == 0) {
		return;
	}

	HttpSettings settings = ExtractHttpSettings(context, urls[0]);
	idx_t max_concurrent = settings.max_concurrency;

	if (count == 1 || max_concurrent <= 1) {
		for (idx_t i = 0; i < count; i++) {
			results[i] = ExecuteMultipartPostThreadSafe(settings, urls[i], headers_list[i], items_list[i]);
		}
		return;
	}

	std::atomic<idx_t> next_idx {0};
	idx_t num_threads = std::min(count, max_concurrent);
	vector<std::thread> workers;

	for (idx_t t = 0; t < num_threads; t++) {
		workers.emplace_back([&]() {
			while (true) {
				idx_t i = next_idx.fetch_add(1);
				if (i >= count) {
					break;
				}
				results[i] = ExecuteMultipartPostThreadSafe(settings, urls[i], headers_list[i], items_list[i]);
			}
		});
	}

	for (auto &w : workers) {
		w.join();
	}
}

// http_post_multipart(url, files)
static void HttpPostMultipartScalar2(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &files_vec = args.data[1];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<duckdb_httplib_openssl::UploadFormDataItems> items_list(count);

	for (idx_t i = 0; i < count; i++) {
		urls[i] = url_vec.GetValue(i).GetValue<string>();
		items_list[i] = ValueToMultipartItems(files_vec.GetValue(i), Value());
	}

	vector<HttpResponseData> results;
	ExecuteMultipartPostParallel(context, urls, headers_list, items_list, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// http_post_multipart(url, files, fields)
static void HttpPostMultipartScalar3(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &files_vec = args.data[1];
	auto &fields_vec = args.data[2];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<duckdb_httplib_openssl::UploadFormDataItems> items_list(count);

	for (idx_t i = 0; i < count; i++) {
		urls[i] = url_vec.GetValue(i).GetValue<string>();
		items_list[i] = ValueToMultipartItems(files_vec.GetValue(i), fields_vec.GetValue(i));
	}

	vector<HttpResponseData> results;
	ExecuteMultipartPostParallel(context, urls, headers_list, items_list, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// http_post_multipart(url, files, fields, headers)
static void HttpPostMultipartScalar4(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &files_vec = args.data[1];
	auto &fields_vec = args.data[2];
	auto &headers_vec = args.data[3];
	auto count = args.size();
	auto &context = state.GetContext();

	vector<string> urls(count);
	vector<duckdb_httplib_openssl::Headers> headers_list(count);
	vector<duckdb_httplib_openssl::UploadFormDataItems> items_list(count);

	for (idx_t i = 0; i < count; i++) {
		urls[i] = url_vec.GetValue(i).GetValue<string>();
		items_list[i] = ValueToMultipartItems(files_vec.GetValue(i), fields_vec.GetValue(i));
		headers_list[i] = ValueToHttplibHeaders(headers_vec.GetValue(i));
	}

	vector<HttpResponseData> results;
	ExecuteMultipartPostParallel(context, urls, headers_list, items_list, results);

	for (idx_t i = 0; i < count; i++) {
		if (!results[i].error.empty()) {
			throw IOException(results[i].error);
		}
		result.SetValue(i, BuildHttpResponseFromData(results[i]));
	}
}

// Bind function for http_post_multipart - allows named parameter reordering
static unique_ptr<FunctionData> HttpPostMultipartScalarBind(ClientContext &context, ScalarFunction &bound_function,
                                                            vector<unique_ptr<Expression>> &arguments) {
	// Handle named parameter reordering for 3 and 4 arg versions
	if (arguments.size() >= 3) {
		// Map aliases to expected positions
		idx_t files_idx = 1;
		idx_t fields_idx = 2;
		idx_t headers_idx = (arguments.size() == 4) ? 3 : -1;

		for (idx_t i = 1; i < arguments.size(); i++) {
			if (StringUtil::CIEquals(arguments[i]->alias, "files")) {
				files_idx = i;
			} else if (StringUtil::CIEquals(arguments[i]->alias, "fields")) {
				fields_idx = i;
			} else if (StringUtil::CIEquals(arguments[i]->alias, "headers")) {
				headers_idx = i;
			}
		}

		// Reorder if needed
		if (arguments.size() == 3 && files_idx != 1) {
			std::swap(arguments[1], arguments[files_idx == 2 ? 2 : 1]);
		}
		if (arguments.size() == 4) {
			// Ensure order: url, files, fields, headers
			vector<unique_ptr<Expression>> reordered;
			reordered.push_back(std::move(arguments[0]));
			reordered.push_back(std::move(arguments[files_idx]));
			reordered.push_back(std::move(arguments[fields_idx]));
			reordered.push_back(std::move(arguments[headers_idx]));
			arguments = std::move(reordered);
		}
	}
	return nullptr;
}

//------------------------------------------------------------------------------
// TABLE FUNCTIONS (with named parameters)
//------------------------------------------------------------------------------

struct HttpTableBindData : public TableFunctionData {
	string url;
	string method;
	Value headers;
	Value params;
	string body;
	string content_type;
};

struct HttpTableState : public GlobalTableFunctionState {
	bool done = false;
};

// Helper to set common return types for all HTTP functions
static void SetHttpReturnTypes(vector<LogicalType> &return_types, vector<string> &names) {
	return_types.push_back(LogicalType::INTEGER);
	names.push_back("status");

	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("content_type");

	return_types.push_back(LogicalType::BIGINT);
	names.push_back("content_length");

	return_types.push_back(CreateHeadersMapType());
	names.push_back("headers");

	return_types.push_back(LogicalType::LIST(CreateCookieStructType()));
	names.push_back("cookies");

	return_types.push_back(LogicalType::BLOB);
	names.push_back("body");
}

static unique_ptr<FunctionData> HttpGetTableBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<HttpTableBindData>();
	result->url = input.inputs[0].GetValue<string>();
	result->method = "GET";
	result->headers = Value();
	result->params = Value();

	auto headers_entry = input.named_parameters.find("headers");
	if (headers_entry != input.named_parameters.end()) {
		result->headers = headers_entry->second;
	}

	auto params_entry = input.named_parameters.find("params");
	if (params_entry != input.named_parameters.end()) {
		result->params = params_entry->second;
	}

	// Build URL with query parameters
	result->url = BuildUrlWithParams(result->url, result->params);

	SetHttpReturnTypes(return_types, names);
	return result;
}

static unique_ptr<FunctionData> HttpHeadTableBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<HttpTableBindData>();
	result->url = input.inputs[0].GetValue<string>();
	result->method = "HEAD";
	result->headers = Value();
	result->params = Value();

	auto headers_entry = input.named_parameters.find("headers");
	if (headers_entry != input.named_parameters.end()) {
		result->headers = headers_entry->second;
	}

	auto params_entry = input.named_parameters.find("params");
	if (params_entry != input.named_parameters.end()) {
		result->params = params_entry->second;
	}

	// Build URL with query parameters
	result->url = BuildUrlWithParams(result->url, result->params);

	SetHttpReturnTypes(return_types, names);
	return result;
}

static unique_ptr<FunctionData> HttpDeleteTableBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<HttpTableBindData>();
	result->url = input.inputs[0].GetValue<string>();
	result->method = "DELETE";
	result->headers = Value();
	result->params = Value();

	auto headers_entry = input.named_parameters.find("headers");
	if (headers_entry != input.named_parameters.end()) {
		result->headers = headers_entry->second;
	}

	auto params_entry = input.named_parameters.find("params");
	if (params_entry != input.named_parameters.end()) {
		result->params = params_entry->second;
	}

	// Build URL with query parameters
	result->url = BuildUrlWithParams(result->url, result->params);

	SetHttpReturnTypes(return_types, names);
	return result;
}

static unique_ptr<FunctionData> HttpPostTableBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<HttpTableBindData>();
	result->url = input.inputs[0].GetValue<string>();
	result->method = "POST";
	result->headers = Value();
	result->params = Value();
	result->content_type = "application/octet-stream";

	auto headers_entry = input.named_parameters.find("headers");
	if (headers_entry != input.named_parameters.end()) {
		result->headers = headers_entry->second;
	}

	auto params_entry = input.named_parameters.find("params");
	if (params_entry != input.named_parameters.end()) {
		result->params = params_entry->second;
	}

	auto body_entry = input.named_parameters.find("body");
	if (body_entry != input.named_parameters.end()) {
		result->body = body_entry->second.GetValueUnsafe<string>();
	}

	auto content_type_entry = input.named_parameters.find("content_type");
	if (content_type_entry != input.named_parameters.end()) {
		result->content_type = content_type_entry->second.GetValue<string>();
	}

	// Build URL with query parameters
	result->url = BuildUrlWithParams(result->url, result->params);

	SetHttpReturnTypes(return_types, names);
	return result;
}

static unique_ptr<FunctionData> HttpPutTableBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<HttpTableBindData>();
	result->url = input.inputs[0].GetValue<string>();
	result->method = "PUT";
	result->headers = Value();
	result->params = Value();
	result->content_type = "application/octet-stream";

	auto headers_entry = input.named_parameters.find("headers");
	if (headers_entry != input.named_parameters.end()) {
		result->headers = headers_entry->second;
	}

	auto params_entry = input.named_parameters.find("params");
	if (params_entry != input.named_parameters.end()) {
		result->params = params_entry->second;
	}

	auto body_entry = input.named_parameters.find("body");
	if (body_entry != input.named_parameters.end()) {
		result->body = body_entry->second.GetValueUnsafe<string>();
	}

	auto content_type_entry = input.named_parameters.find("content_type");
	if (content_type_entry != input.named_parameters.end()) {
		result->content_type = content_type_entry->second.GetValue<string>();
	}

	// Build URL with query parameters
	result->url = BuildUrlWithParams(result->url, result->params);

	SetHttpReturnTypes(return_types, names);
	return result;
}

static unique_ptr<FunctionData> HttpPatchTableBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<HttpTableBindData>();
	result->url = input.inputs[0].GetValue<string>();
	result->method = "PATCH";
	result->headers = Value();
	result->params = Value();
	result->content_type = "application/octet-stream";

	auto headers_entry = input.named_parameters.find("headers");
	if (headers_entry != input.named_parameters.end()) {
		result->headers = headers_entry->second;
	}

	auto params_entry = input.named_parameters.find("params");
	if (params_entry != input.named_parameters.end()) {
		result->params = params_entry->second;
	}

	auto body_entry = input.named_parameters.find("body");
	if (body_entry != input.named_parameters.end()) {
		result->body = body_entry->second.GetValueUnsafe<string>();
	}

	auto content_type_entry = input.named_parameters.find("content_type");
	if (content_type_entry != input.named_parameters.end()) {
		result->content_type = content_type_entry->second.GetValue<string>();
	}

	// Build URL with query parameters
	result->url = BuildUrlWithParams(result->url, result->params);

	SetHttpReturnTypes(return_types, names);
	return result;
}

static unique_ptr<FunctionData> HttpPostFormTableBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<HttpTableBindData>();
	result->url = input.inputs[0].GetValue<string>();
	result->method = "POST";
	result->headers = Value();
	result->params = Value();
	result->content_type = "application/x-www-form-urlencoded";

	auto headers_entry = input.named_parameters.find("headers");
	if (headers_entry != input.named_parameters.end()) {
		result->headers = headers_entry->second;
	}

	auto params_entry = input.named_parameters.find("params");
	if (params_entry != input.named_parameters.end()) {
		// Convert params STRUCT to form-encoded body
		result->body = BuildFormEncodedBody(params_entry->second);
	}

	SetHttpReturnTypes(return_types, names);
	return result;
}

static unique_ptr<GlobalTableFunctionState> HttpTableInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<HttpTableState>();
}

static void HttpTableFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<HttpTableBindData>();
	auto &state = data_p.global_state->Cast<HttpTableState>();

	if (state.done) {
		return;
	}
	state.done = true;

	int32_t status_code;
	string resp_content_type;
	int64_t resp_content_length;
	vector<Value> header_keys, header_values, cookies;
	string response_body;

	PerformHttpRequestCore(context, bind_data.url, bind_data.method, bind_data.headers, bind_data.body,
	                       bind_data.content_type, status_code, resp_content_type, resp_content_length, header_keys,
	                       header_values, cookies, response_body);

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::INTEGER(status_code));
	output.SetValue(1, 0, resp_content_type.empty() ? Value(LogicalType::VARCHAR) : Value(resp_content_type));
	output.SetValue(2, 0, resp_content_length < 0 ? Value(LogicalType::BIGINT) : Value::BIGINT(resp_content_length));
	output.SetValue(3, 0, Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, header_keys, header_values));
	output.SetValue(4, 0, Value::LIST(CreateCookieStructType(), std::move(cookies)));
	output.SetValue(5, 0, Value::BLOB_RAW(response_body));
}

//------------------------------------------------------------------------------
// MULTIPART TABLE FUNCTION
//------------------------------------------------------------------------------

struct HttpMultipartTableBindData : public TableFunctionData {
	string url;
	Value files;
	Value fields;
	Value headers;
};

struct HttpMultipartTableState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> HttpPostMultipartTableBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<HttpMultipartTableBindData>();
	result->url = input.inputs[0].GetValue<string>();
	result->files = Value();
	result->fields = Value();
	result->headers = Value();

	auto files_entry = input.named_parameters.find("files");
	if (files_entry != input.named_parameters.end()) {
		result->files = files_entry->second;
	}

	auto fields_entry = input.named_parameters.find("fields");
	if (fields_entry != input.named_parameters.end()) {
		result->fields = fields_entry->second;
	}

	auto headers_entry = input.named_parameters.find("headers");
	if (headers_entry != input.named_parameters.end()) {
		result->headers = headers_entry->second;
	}

	SetHttpReturnTypes(return_types, names);
	return result;
}

static unique_ptr<GlobalTableFunctionState> HttpMultipartTableInit(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	return make_uniq<HttpMultipartTableState>();
}

static void HttpMultipartTableFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<HttpMultipartTableBindData>();
	auto &state = data_p.global_state->Cast<HttpMultipartTableState>();

	if (state.done) {
		return;
	}
	state.done = true;

	HttpSettings settings = ExtractHttpSettings(context, bind_data.url);
	duckdb_httplib_openssl::Headers req_headers = ValueToHttplibHeaders(bind_data.headers);
	auto items = ValueToMultipartItems(bind_data.files, bind_data.fields);

	auto result = ExecuteMultipartPostThreadSafe(settings, bind_data.url, req_headers, items);

	if (!result.error.empty()) {
		throw IOException(result.error);
	}

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::INTEGER(result.status_code));
	output.SetValue(1, 0, result.content_type.empty() ? Value(LogicalType::VARCHAR) : Value(result.content_type));
	output.SetValue(2, 0,
	                result.content_length < 0 ? Value(LogicalType::BIGINT) : Value::BIGINT(result.content_length));
	output.SetValue(3, 0,
	                Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, result.header_keys, result.header_values));
	output.SetValue(4, 0, Value::LIST(CreateCookieStructType(), std::move(result.cookies)));
	output.SetValue(5, 0, Value::BLOB_RAW(result.body));
}

//------------------------------------------------------------------------------
// REGISTRATION
//------------------------------------------------------------------------------

static void LoadInternal(ExtensionLoader &loader) {
	// Register extension settings
	auto &instance = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(instance);
	config.AddExtensionOption("http_max_concurrency",
	                          "Maximum number of concurrent HTTP requests per scalar function call (default: 32)",
	                          LogicalType::UBIGINT, Value::UBIGINT(DEFAULT_HTTP_MAX_CONCURRENT));
	config.AddExtensionOption("http_user_agent", "Custom User-Agent header for all HTTP requests (default: DuckDB)",
	                          LogicalType::VARCHAR, Value(""));
	config.AddExtensionOption("http_request_cache",
	                          "Cache HTTP responses within query to prevent duplicate requests (default: true)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("http_follow_redirects", "Automatically follow HTTP redirects (default: true)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));

	auto http_response_type = CreateHttpResponseType();

	// Register byte_range(offset, length) helper function
	ScalarFunction byte_range_func("byte_range", {LogicalType::BIGINT, LogicalType::BIGINT}, LogicalType::VARCHAR,
	                               ByteRangeFunction);
	loader.RegisterFunction(byte_range_func);

	// Register http_range_header(offset, length) helper function - returns STRUCT for headers parameter
	auto http_range_header_type = CreateHttpRangeHeaderType();
	ScalarFunction http_range_header_func("http_range_header", {LogicalType::BIGINT, LogicalType::BIGINT},
	                                      http_range_header_type, HttpRangeHeaderFunction, HttpRangeHeaderBind);
	loader.RegisterFunction(http_range_header_func);

	// Register HTTP_HEADERS extension type
	auto http_headers_type = HttpHeadersType::GetDefault();
	loader.RegisterType("HTTP_HEADERS", http_headers_type);

	// Register case-insensitive map_extract_value for HTTP_HEADERS type
	// This enables headers['content-type'] to work regardless of case
	ScalarFunction http_headers_extract("map_extract_value", {http_headers_type, LogicalType::VARCHAR},
	                                    LogicalType::VARCHAR, HttpHeadersExtractValue);
	loader.RegisterFunction(http_headers_extract);

	// Register http_head scalar functions (VOLATILE to prevent constant folding)
	ScalarFunctionSet http_head_scalar("http_head");
	ScalarFunction http_head_1({LogicalType::VARCHAR}, http_response_type, HttpHeadScalar1);
	http_head_1.stability = FunctionStability::VOLATILE;
	http_head_scalar.AddFunction(http_head_1);
	ScalarFunction http_head_2({LogicalType::VARCHAR, LogicalType::ANY}, http_response_type, HttpHeadScalar2);
	http_head_2.stability = FunctionStability::VOLATILE;
	http_head_scalar.AddFunction(http_head_2);
	ScalarFunction http_head_3({LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY}, http_response_type,
	                           HttpHeadScalar3, HttpHeadScalar3Bind);
	http_head_3.stability = FunctionStability::VOLATILE;
	http_head_scalar.AddFunction(http_head_3);
	loader.RegisterFunction(http_head_scalar);

	// Register http_get scalar functions (VOLATILE to prevent constant folding)
	ScalarFunctionSet http_get_scalar("http_get");
	ScalarFunction http_get_1({LogicalType::VARCHAR}, http_response_type, HttpGetScalar1);
	http_get_1.stability = FunctionStability::VOLATILE;
	http_get_scalar.AddFunction(http_get_1);
	ScalarFunction http_get_2({LogicalType::VARCHAR, LogicalType::ANY}, http_response_type, HttpGetScalar2);
	http_get_2.stability = FunctionStability::VOLATILE;
	http_get_scalar.AddFunction(http_get_2);
	ScalarFunction http_get_3({LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY}, http_response_type,
	                          HttpGetScalar3, HttpGetScalar3Bind);
	http_get_3.stability = FunctionStability::VOLATILE;
	http_get_scalar.AddFunction(http_get_3);
	loader.RegisterFunction(http_get_scalar);

	// Register http_head table function (with named parameters)
	TableFunction http_head_table("http_head", {LogicalType::VARCHAR}, HttpTableFunction, HttpHeadTableBind,
	                              HttpTableInit);
	http_head_table.named_parameters["headers"] = LogicalType::ANY;
	http_head_table.named_parameters["params"] = LogicalType::ANY;
	loader.RegisterFunction(http_head_table);

	// Register http_get table function (with named parameters)
	TableFunction http_get_table("http_get", {LogicalType::VARCHAR}, HttpTableFunction, HttpGetTableBind,
	                             HttpTableInit);
	http_get_table.named_parameters["headers"] = LogicalType::ANY;
	http_get_table.named_parameters["params"] = LogicalType::ANY;
	loader.RegisterFunction(http_get_table);

	// Register http_delete scalar functions (VOLATILE to prevent constant folding)
	ScalarFunctionSet http_delete_scalar("http_delete");
	ScalarFunction http_delete_1({LogicalType::VARCHAR}, http_response_type, HttpDeleteScalar1);
	http_delete_1.stability = FunctionStability::VOLATILE;
	http_delete_scalar.AddFunction(http_delete_1);
	ScalarFunction http_delete_2({LogicalType::VARCHAR, LogicalType::ANY}, http_response_type, HttpDeleteScalar2);
	http_delete_2.stability = FunctionStability::VOLATILE;
	http_delete_scalar.AddFunction(http_delete_2);
	ScalarFunction http_delete_3({LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY}, http_response_type,
	                             HttpDeleteScalar3, HttpDeleteScalar3Bind);
	http_delete_3.stability = FunctionStability::VOLATILE;
	http_delete_scalar.AddFunction(http_delete_3);
	loader.RegisterFunction(http_delete_scalar);

	// Register http_delete table function (with named parameters)
	TableFunction http_delete_table("http_delete", {LogicalType::VARCHAR}, HttpTableFunction, HttpDeleteTableBind,
	                                HttpTableInit);
	http_delete_table.named_parameters["headers"] = LogicalType::ANY;
	http_delete_table.named_parameters["params"] = LogicalType::ANY;
	loader.RegisterFunction(http_delete_table);

	// Register http_post scalar functions (VOLATILE to prevent constant folding)
	ScalarFunctionSet http_post_scalar("http_post");
	ScalarFunction http_post_1({LogicalType::VARCHAR}, http_response_type, HttpPostScalar1);
	http_post_1.stability = FunctionStability::VOLATILE;
	http_post_scalar.AddFunction(http_post_1);
	ScalarFunction http_post_2({LogicalType::VARCHAR, LogicalType::ANY}, http_response_type, HttpPostScalar2);
	http_post_2.stability = FunctionStability::VOLATILE;
	http_post_scalar.AddFunction(http_post_2);
	ScalarFunction http_post_3({LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY}, http_response_type,
	                           HttpPostScalar3, HttpPostScalar3Bind);
	http_post_3.stability = FunctionStability::VOLATILE;
	http_post_scalar.AddFunction(http_post_3);
	loader.RegisterFunction(http_post_scalar);

	// Register http_post table function (with named parameters)
	TableFunction http_post_table("http_post", {LogicalType::VARCHAR}, HttpTableFunction, HttpPostTableBind,
	                              HttpTableInit);
	http_post_table.named_parameters["headers"] = LogicalType::ANY;
	http_post_table.named_parameters["params"] = LogicalType::ANY;
	http_post_table.named_parameters["body"] = LogicalType::BLOB;
	http_post_table.named_parameters["content_type"] = LogicalType::VARCHAR;
	loader.RegisterFunction(http_post_table);

	// Register http_put scalar functions (VOLATILE to prevent constant folding)
	ScalarFunctionSet http_put_scalar("http_put");
	ScalarFunction http_put_1({LogicalType::VARCHAR}, http_response_type, HttpPutScalar1);
	http_put_1.stability = FunctionStability::VOLATILE;
	http_put_scalar.AddFunction(http_put_1);
	ScalarFunction http_put_2({LogicalType::VARCHAR, LogicalType::ANY}, http_response_type, HttpPutScalar2);
	http_put_2.stability = FunctionStability::VOLATILE;
	http_put_scalar.AddFunction(http_put_2);
	ScalarFunction http_put_3({LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY}, http_response_type,
	                          HttpPutScalar3, HttpPutScalar3Bind);
	http_put_3.stability = FunctionStability::VOLATILE;
	http_put_scalar.AddFunction(http_put_3);
	loader.RegisterFunction(http_put_scalar);

	// Register http_put table function (with named parameters)
	TableFunction http_put_table("http_put", {LogicalType::VARCHAR}, HttpTableFunction, HttpPutTableBind,
	                             HttpTableInit);
	http_put_table.named_parameters["headers"] = LogicalType::ANY;
	http_put_table.named_parameters["params"] = LogicalType::ANY;
	http_put_table.named_parameters["body"] = LogicalType::BLOB;
	http_put_table.named_parameters["content_type"] = LogicalType::VARCHAR;
	loader.RegisterFunction(http_put_table);

	// Register http_patch scalar functions (VOLATILE to prevent constant folding)
	ScalarFunctionSet http_patch_scalar("http_patch");
	ScalarFunction http_patch_1({LogicalType::VARCHAR}, http_response_type, HttpPatchScalar1);
	http_patch_1.stability = FunctionStability::VOLATILE;
	http_patch_scalar.AddFunction(http_patch_1);
	ScalarFunction http_patch_2({LogicalType::VARCHAR, LogicalType::ANY}, http_response_type, HttpPatchScalar2);
	http_patch_2.stability = FunctionStability::VOLATILE;
	http_patch_scalar.AddFunction(http_patch_2);
	ScalarFunction http_patch_3({LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY}, http_response_type,
	                            HttpPatchScalar3, HttpPatchScalar3Bind);
	http_patch_3.stability = FunctionStability::VOLATILE;
	http_patch_scalar.AddFunction(http_patch_3);
	loader.RegisterFunction(http_patch_scalar);

	// Register http_patch table function (with named parameters)
	TableFunction http_patch_table("http_patch", {LogicalType::VARCHAR}, HttpTableFunction, HttpPatchTableBind,
	                               HttpTableInit);
	http_patch_table.named_parameters["headers"] = LogicalType::ANY;
	http_patch_table.named_parameters["params"] = LogicalType::ANY;
	http_patch_table.named_parameters["body"] = LogicalType::BLOB;
	http_patch_table.named_parameters["content_type"] = LogicalType::VARCHAR;
	loader.RegisterFunction(http_patch_table);

	// Register http_post_form scalar functions (VOLATILE to prevent constant folding)
	ScalarFunctionSet http_post_form_scalar("http_post_form");
	ScalarFunction http_post_form_2({LogicalType::VARCHAR, LogicalType::ANY}, http_response_type, HttpPostFormScalar2);
	http_post_form_2.stability = FunctionStability::VOLATILE;
	http_post_form_scalar.AddFunction(http_post_form_2);
	ScalarFunction http_post_form_3({LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY}, http_response_type,
	                                HttpPostFormScalar3, HttpPostFormScalar3Bind);
	http_post_form_3.stability = FunctionStability::VOLATILE;
	http_post_form_scalar.AddFunction(http_post_form_3);
	loader.RegisterFunction(http_post_form_scalar);

	// Register http_post_form table function (with named parameters)
	TableFunction http_post_form_table("http_post_form", {LogicalType::VARCHAR}, HttpTableFunction,
	                                   HttpPostFormTableBind, HttpTableInit);
	http_post_form_table.named_parameters["headers"] = LogicalType::ANY;
	http_post_form_table.named_parameters["params"] = LogicalType::ANY;
	loader.RegisterFunction(http_post_form_table);

	// Register http_post_multipart scalar functions (VOLATILE to prevent constant folding)
	ScalarFunctionSet http_post_multipart_scalar("http_post_multipart");
	// http_post_multipart(url, files)
	ScalarFunction http_post_multipart_2({LogicalType::VARCHAR, LogicalType::ANY}, http_response_type,
	                                     HttpPostMultipartScalar2);
	http_post_multipart_2.stability = FunctionStability::VOLATILE;
	http_post_multipart_scalar.AddFunction(http_post_multipart_2);
	// http_post_multipart(url, files, fields)
	ScalarFunction http_post_multipart_3({LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY}, http_response_type,
	                                     HttpPostMultipartScalar3, HttpPostMultipartScalarBind);
	http_post_multipart_3.stability = FunctionStability::VOLATILE;
	http_post_multipart_scalar.AddFunction(http_post_multipart_3);
	// http_post_multipart(url, files, fields, headers)
	ScalarFunction http_post_multipart_4({LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY, LogicalType::ANY},
	                                     http_response_type, HttpPostMultipartScalar4, HttpPostMultipartScalarBind);
	http_post_multipart_4.stability = FunctionStability::VOLATILE;
	http_post_multipart_scalar.AddFunction(http_post_multipart_4);
	loader.RegisterFunction(http_post_multipart_scalar);

	// Register http_post_multipart table function (with named parameters)
	TableFunction http_post_multipart_table("http_post_multipart", {LogicalType::VARCHAR}, HttpMultipartTableFunction,
	                                        HttpPostMultipartTableBind, HttpMultipartTableInit);
	http_post_multipart_table.named_parameters["files"] = LogicalType::ANY;
	http_post_multipart_table.named_parameters["fields"] = LogicalType::ANY;
	http_post_multipart_table.named_parameters["headers"] = LogicalType::ANY;
	loader.RegisterFunction(http_post_multipart_table);
}

void HttpRequestExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string HttpRequestExtension::Name() {
	return "http_request";
}

std::string HttpRequestExtension::Version() const {
#ifdef EXT_VERSION_HTTP_REQUEST
	return EXT_VERSION_HTTP_REQUEST;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(http_request, loader) {
	duckdb::LoadInternal(loader);
}
}
