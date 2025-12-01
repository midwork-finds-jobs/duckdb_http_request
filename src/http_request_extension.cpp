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

namespace duckdb {

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

// Core HTTP request logic - used by both scalar and table functions
// Supports GET, HEAD, POST, PUT, PATCH, DELETE methods
// For POST/PUT/PATCH, request_body contains the body to send
static void PerformHttpRequestCore(ClientContext &context, const string &url, const string &method,
                                   const Value &headers_val, const string &request_body, const string &content_type,
                                   int32_t &status_code, vector<Value> &header_keys, vector<Value> &header_values,
                                   string &final_body) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &config = db.config;

	// Read HTTP settings from extension options (httpfs settings)
	uint64_t http_timeout = 30;
	uint64_t http_retries = 3;
	bool http_keep_alive = true;

	// Create file opener to read settings
	ClientContextFileOpener opener(context);
	FileOpenerInfo info;
	info.file_path = url;

	// Try to read httpfs extension settings
	FileOpener::TryGetCurrentSetting(&opener, "http_timeout", http_timeout, &info);
	FileOpener::TryGetCurrentSetting(&opener, "http_retries", http_retries, &info);
	FileOpener::TryGetCurrentSetting(&opener, "http_keep_alive", http_keep_alive, &info);

	// Read proxy settings - first from secrets, then fall back to core settings
	string http_proxy = config.options.http_proxy;
	string http_proxy_username = config.options.http_proxy_username;
	string http_proxy_password = config.options.http_proxy_password;

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
	client.set_follow_location(true);
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

	for (auto &header : res->headers) {
		header_keys.push_back(Value(header.first));
		header_values.push_back(Value(header.second));
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
                                   const Value &headers_val, int32_t &status_code, vector<Value> &header_keys,
                                   vector<Value> &header_values, string &final_body) {
	PerformHttpRequestCore(context, url, method, headers_val, "", "", status_code, header_keys, header_values,
	                       final_body);
}

// Build response STRUCT value
static Value BuildHttpResponseValue(int32_t status_code, vector<Value> &header_keys, vector<Value> &header_values,
                                    const string &body) {
	child_list_t<Value> struct_values;
	struct_values.push_back(make_pair("status", Value::INTEGER(status_code)));
	struct_values.push_back(
	    make_pair("headers", Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, header_keys, header_values)));
	struct_values.push_back(make_pair("body", Value::BLOB_RAW(body)));
	return Value::STRUCT(std::move(struct_values));
}

// http_response STRUCT type
static LogicalType CreateHttpResponseType() {
	child_list_t<LogicalType> struct_children;
	struct_children.push_back(make_pair("status", LogicalType::INTEGER));
	struct_children.push_back(make_pair("headers", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)));
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

	for (idx_t i = 0; i < count; i++) {
		auto url = url_vec.GetValue(i).GetValue<string>();
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string body;
		PerformHttpRequestCore(context, url, "HEAD", Value(), status_code, header_keys, header_values, body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, body));
	}
}

// http_head(url, headers)
static void HttpHeadScalar2(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto count = args.size();
	auto &context = state.GetContext();

	for (idx_t i = 0; i < count; i++) {
		auto url = url_vec.GetValue(i).GetValue<string>();
		auto headers = headers_vec.GetValue(i);
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string body;
		PerformHttpRequestCore(context, url, "HEAD", headers, status_code, header_keys, header_values, body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, body));
	}
}

// http_head(url, headers, params)
static void HttpHeadScalar3(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto &params_vec = args.data[2];
	auto count = args.size();
	auto &context = state.GetContext();

	for (idx_t i = 0; i < count; i++) {
		auto url = url_vec.GetValue(i).GetValue<string>();
		auto headers = headers_vec.GetValue(i);
		auto params = params_vec.GetValue(i);
		url = BuildUrlWithParams(url, params);
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string body;
		PerformHttpRequestCore(context, url, "HEAD", headers, status_code, header_keys, header_values, body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, body));
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

	for (idx_t i = 0; i < count; i++) {
		auto url = url_vec.GetValue(i).GetValue<string>();
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string body;
		PerformHttpRequestCore(context, url, "GET", Value(), status_code, header_keys, header_values, body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, body));
	}
}

// http_get(url, headers)
static void HttpGetScalar2(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto count = args.size();
	auto &context = state.GetContext();

	for (idx_t i = 0; i < count; i++) {
		auto url = url_vec.GetValue(i).GetValue<string>();
		auto headers = headers_vec.GetValue(i);
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string body;
		PerformHttpRequestCore(context, url, "GET", headers, status_code, header_keys, header_values, body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, body));
	}
}

// http_get(url, headers, params)
static void HttpGetScalar3(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto &params_vec = args.data[2];
	auto count = args.size();
	auto &context = state.GetContext();

	for (idx_t i = 0; i < count; i++) {
		auto base_url = url_vec.GetValue(i).GetValue<string>();
		auto headers = headers_vec.GetValue(i);
		auto params = params_vec.GetValue(i);
		auto url = BuildUrlWithParams(base_url, params);
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string body;
		PerformHttpRequestCore(context, url, "GET", headers, status_code, header_keys, header_values, body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, body));
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

	for (idx_t i = 0; i < count; i++) {
		auto url = url_vec.GetValue(i).GetValue<string>();
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string body;
		PerformHttpRequestCore(context, url, "DELETE", Value(), status_code, header_keys, header_values, body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, body));
	}
}

// http_delete(url, headers)
static void HttpDeleteScalar2(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto count = args.size();
	auto &context = state.GetContext();

	for (idx_t i = 0; i < count; i++) {
		auto url = url_vec.GetValue(i).GetValue<string>();
		auto headers = headers_vec.GetValue(i);
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string body;
		PerformHttpRequestCore(context, url, "DELETE", headers, status_code, header_keys, header_values, body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, body));
	}
}

// http_delete(url, headers, params)
static void HttpDeleteScalar3(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto &params_vec = args.data[2];
	auto count = args.size();
	auto &context = state.GetContext();

	for (idx_t i = 0; i < count; i++) {
		auto base_url = url_vec.GetValue(i).GetValue<string>();
		auto headers = headers_vec.GetValue(i);
		auto params = params_vec.GetValue(i);
		auto url = BuildUrlWithParams(base_url, params);
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string body;
		PerformHttpRequestCore(context, url, "DELETE", headers, status_code, header_keys, header_values, body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, body));
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

	for (idx_t i = 0; i < count; i++) {
		auto url = url_vec.GetValue(i).GetValue<string>();
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string response_body;
		PerformHttpRequestCore(context, url, "POST", Value(), "", "application/octet-stream", status_code, header_keys,
		                       header_values, response_body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, response_body));
	}
}

// http_post(url, headers)
static void HttpPostScalar2(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto count = args.size();
	auto &context = state.GetContext();

	for (idx_t i = 0; i < count; i++) {
		auto url = url_vec.GetValue(i).GetValue<string>();
		auto headers = headers_vec.GetValue(i);
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string response_body;
		PerformHttpRequestCore(context, url, "POST", headers, "", "", status_code, header_keys, header_values,
		                       response_body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, response_body));
	}
}

// http_post(url, headers, params)
static void HttpPostScalar3(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto &params_vec = args.data[2];
	auto count = args.size();
	auto &context = state.GetContext();

	for (idx_t i = 0; i < count; i++) {
		auto base_url = url_vec.GetValue(i).GetValue<string>();
		auto headers = headers_vec.GetValue(i);
		auto params = params_vec.GetValue(i);
		auto url = BuildUrlWithParams(base_url, params);
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string response_body;
		PerformHttpRequestCore(context, url, "POST", headers, "", "", status_code, header_keys, header_values,
		                       response_body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, response_body));
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

	for (idx_t i = 0; i < count; i++) {
		auto url = url_vec.GetValue(i).GetValue<string>();
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string response_body;
		PerformHttpRequestCore(context, url, "PUT", Value(), "", "application/octet-stream", status_code, header_keys,
		                       header_values, response_body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, response_body));
	}
}

// http_put(url, headers)
static void HttpPutScalar2(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto count = args.size();
	auto &context = state.GetContext();

	for (idx_t i = 0; i < count; i++) {
		auto url = url_vec.GetValue(i).GetValue<string>();
		auto headers = headers_vec.GetValue(i);
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string response_body;
		PerformHttpRequestCore(context, url, "PUT", headers, "", "", status_code, header_keys, header_values,
		                       response_body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, response_body));
	}
}

// http_put(url, headers, params)
static void HttpPutScalar3(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto &params_vec = args.data[2];
	auto count = args.size();
	auto &context = state.GetContext();

	for (idx_t i = 0; i < count; i++) {
		auto base_url = url_vec.GetValue(i).GetValue<string>();
		auto headers = headers_vec.GetValue(i);
		auto params = params_vec.GetValue(i);
		auto url = BuildUrlWithParams(base_url, params);
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string response_body;
		PerformHttpRequestCore(context, url, "PUT", headers, "", "", status_code, header_keys, header_values,
		                       response_body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, response_body));
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

	for (idx_t i = 0; i < count; i++) {
		auto url = url_vec.GetValue(i).GetValue<string>();
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string response_body;
		PerformHttpRequestCore(context, url, "PATCH", Value(), "", "application/octet-stream", status_code, header_keys,
		                       header_values, response_body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, response_body));
	}
}

// http_patch(url, headers)
static void HttpPatchScalar2(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto count = args.size();
	auto &context = state.GetContext();

	for (idx_t i = 0; i < count; i++) {
		auto url = url_vec.GetValue(i).GetValue<string>();
		auto headers = headers_vec.GetValue(i);
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string response_body;
		PerformHttpRequestCore(context, url, "PATCH", headers, "", "", status_code, header_keys, header_values,
		                       response_body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, response_body));
	}
}

// http_patch(url, headers, params)
static void HttpPatchScalar3(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &headers_vec = args.data[1];
	auto &params_vec = args.data[2];
	auto count = args.size();
	auto &context = state.GetContext();

	for (idx_t i = 0; i < count; i++) {
		auto base_url = url_vec.GetValue(i).GetValue<string>();
		auto headers = headers_vec.GetValue(i);
		auto params = params_vec.GetValue(i);
		auto url = BuildUrlWithParams(base_url, params);
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string response_body;
		PerformHttpRequestCore(context, url, "PATCH", headers, "", "", status_code, header_keys, header_values,
		                       response_body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, response_body));
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

	for (idx_t i = 0; i < count; i++) {
		auto url = url_vec.GetValue(i).GetValue<string>();
		auto params = params_vec.GetValue(i);
		auto form_body = BuildFormEncodedBody(params);
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string response_body;
		PerformHttpRequestCore(context, url, "POST", Value(), form_body, "application/x-www-form-urlencoded",
		                       status_code, header_keys, header_values, response_body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, response_body));
	}
}

// http_post_form(url, params, headers)
static void HttpPostFormScalar3(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &url_vec = args.data[0];
	auto &params_vec = args.data[1];
	auto &headers_vec = args.data[2];
	auto count = args.size();
	auto &context = state.GetContext();

	for (idx_t i = 0; i < count; i++) {
		auto url = url_vec.GetValue(i).GetValue<string>();
		auto params = params_vec.GetValue(i);
		auto headers = headers_vec.GetValue(i);
		auto form_body = BuildFormEncodedBody(params);
		int32_t status_code;
		vector<Value> header_keys, header_values;
		string response_body;
		PerformHttpRequestCore(context, url, "POST", headers, form_body, "application/x-www-form-urlencoded",
		                       status_code, header_keys, header_values, response_body);
		result.SetValue(i, BuildHttpResponseValue(status_code, header_keys, header_values, response_body));
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

	return_types.push_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	names.push_back("headers");

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
	vector<Value> header_keys, header_values;
	string response_body;

	PerformHttpRequestCore(context, bind_data.url, bind_data.method, bind_data.headers, bind_data.body,
	                       bind_data.content_type, status_code, header_keys, header_values, response_body);

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::INTEGER(status_code));
	output.SetValue(1, 0, Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, header_keys, header_values));
	output.SetValue(2, 0, Value::BLOB_RAW(response_body));
}

//------------------------------------------------------------------------------
// REGISTRATION
//------------------------------------------------------------------------------

static void LoadInternal(ExtensionLoader &loader) {
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

	// Register http_head scalar functions
	ScalarFunctionSet http_head_scalar("http_head");
	http_head_scalar.AddFunction(ScalarFunction({LogicalType::VARCHAR}, http_response_type, HttpHeadScalar1));
	http_head_scalar.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::ANY}, http_response_type, HttpHeadScalar2));
	ScalarFunction http_head_3({LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY}, http_response_type,
	                           HttpHeadScalar3, HttpHeadScalar3Bind);
	http_head_scalar.AddFunction(http_head_3);
	loader.RegisterFunction(http_head_scalar);

	// Register http_get scalar functions
	ScalarFunctionSet http_get_scalar("http_get");
	http_get_scalar.AddFunction(ScalarFunction({LogicalType::VARCHAR}, http_response_type, HttpGetScalar1));
	http_get_scalar.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::ANY}, http_response_type, HttpGetScalar2));
	ScalarFunction http_get_3({LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY}, http_response_type,
	                          HttpGetScalar3, HttpGetScalar3Bind);
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

	// Register http_delete scalar functions
	ScalarFunctionSet http_delete_scalar("http_delete");
	http_delete_scalar.AddFunction(ScalarFunction({LogicalType::VARCHAR}, http_response_type, HttpDeleteScalar1));
	http_delete_scalar.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::ANY}, http_response_type, HttpDeleteScalar2));
	ScalarFunction http_delete_3({LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY}, http_response_type,
	                             HttpDeleteScalar3, HttpDeleteScalar3Bind);
	http_delete_scalar.AddFunction(http_delete_3);
	loader.RegisterFunction(http_delete_scalar);

	// Register http_delete table function (with named parameters)
	TableFunction http_delete_table("http_delete", {LogicalType::VARCHAR}, HttpTableFunction, HttpDeleteTableBind,
	                                HttpTableInit);
	http_delete_table.named_parameters["headers"] = LogicalType::ANY;
	http_delete_table.named_parameters["params"] = LogicalType::ANY;
	loader.RegisterFunction(http_delete_table);

	// Register http_post scalar functions
	ScalarFunctionSet http_post_scalar("http_post");
	http_post_scalar.AddFunction(ScalarFunction({LogicalType::VARCHAR}, http_response_type, HttpPostScalar1));
	http_post_scalar.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::ANY}, http_response_type, HttpPostScalar2));
	ScalarFunction http_post_3({LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY}, http_response_type,
	                           HttpPostScalar3, HttpPostScalar3Bind);
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

	// Register http_put scalar functions
	ScalarFunctionSet http_put_scalar("http_put");
	http_put_scalar.AddFunction(ScalarFunction({LogicalType::VARCHAR}, http_response_type, HttpPutScalar1));
	http_put_scalar.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::ANY}, http_response_type, HttpPutScalar2));
	ScalarFunction http_put_3({LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY}, http_response_type,
	                          HttpPutScalar3, HttpPutScalar3Bind);
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

	// Register http_patch scalar functions
	ScalarFunctionSet http_patch_scalar("http_patch");
	http_patch_scalar.AddFunction(ScalarFunction({LogicalType::VARCHAR}, http_response_type, HttpPatchScalar1));
	http_patch_scalar.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::ANY}, http_response_type, HttpPatchScalar2));
	ScalarFunction http_patch_3({LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY}, http_response_type,
	                            HttpPatchScalar3, HttpPatchScalar3Bind);
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

	// Register http_post_form scalar functions (application/x-www-form-urlencoded)
	ScalarFunctionSet http_post_form_scalar("http_post_form");
	http_post_form_scalar.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::ANY}, http_response_type, HttpPostFormScalar2));
	ScalarFunction http_post_form_3({LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY}, http_response_type,
	                                HttpPostFormScalar3, HttpPostFormScalar3Bind);
	http_post_form_scalar.AddFunction(http_post_form_3);
	loader.RegisterFunction(http_post_form_scalar);

	// Register http_post_form table function (with named parameters)
	TableFunction http_post_form_table("http_post_form", {LogicalType::VARCHAR}, HttpTableFunction,
	                                   HttpPostFormTableBind, HttpTableInit);
	http_post_form_table.named_parameters["headers"] = LogicalType::ANY;
	http_post_form_table.named_parameters["params"] = LogicalType::ANY;
	loader.RegisterFunction(http_post_form_table);
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
