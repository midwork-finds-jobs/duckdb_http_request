"""Local HTTP mock server for http_request tests (stdlib only).

Routes (any method):
  /get           -> 200 JSON {url, args, headers}
  /redirect/N    -> 302 to /redirect/N-1, finally /get
  /range         -> fixed 4096-byte payload, honours "Range: bytes=a-b" with 206
  anything else  -> echo JSON {method, path, headers, parsedQueryParams, rawBody, parsedBody}
HEAD returns the same headers without a body.
"""

import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qsl, urlsplit

RANGE_PAYLOAD = bytes(i % 256 for i in range(4096))


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        sys.stderr.write("mock: " + (fmt % args) + "\n")

    def _read_body(self):
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def _send(self, status, body, content_type="application/json", extra=None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for key, value in (extra or {}).items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, status, obj, extra=None):
        self._send(status, json.dumps(obj).encode(), extra=extra)

    def _handle(self):
        url = urlsplit(self.path)
        raw = self._read_body()
        headers = {k: v for k, v in self.headers.items()}
        query = dict(parse_qsl(url.query, keep_blank_values=True))

        if url.path == "/get":
            return self._json(200, {"url": self.path, "args": query, "headers": headers})

        if url.path.startswith("/redirect/"):
            try:
                remaining = int(url.path.rsplit("/", 1)[1])
            except ValueError:
                return self._json(400, {"error": "bad redirect count"})
            target = "/get" if remaining <= 1 else "/redirect/%d" % (remaining - 1)
            return self._send(302, b"", content_type="text/plain", extra={"Location": target})

        if url.path == "/range":
            spec = self.headers.get("Range", "")
            if spec.startswith("bytes="):
                start_s, _, end_s = spec[len("bytes=") :].partition("-")
                start = int(start_s)
                end = min(int(end_s) if end_s else len(RANGE_PAYLOAD) - 1, len(RANGE_PAYLOAD) - 1)
                chunk = RANGE_PAYLOAD[start : end + 1]
                return self._send(
                    206,
                    chunk,
                    content_type="application/octet-stream",
                    extra={"Content-Range": "bytes %d-%d/%d" % (start, end, len(RANGE_PAYLOAD))},
                )
            return self._send(200, RANGE_PAYLOAD, content_type="application/octet-stream")

        text = raw.decode("utf-8", errors="replace")
        parsed_body = {}
        if self.headers.get("Content-Type", "").startswith("application/x-www-form-urlencoded"):
            parsed_body = dict(parse_qsl(text, keep_blank_values=True))
        return self._json(
            200,
            {
                "method": self.command,
                "path": url.path,
                "headers": headers,
                "parsedQueryParams": query,
                "rawBody": text,
                "parsedBody": parsed_body,
            },
        )

    do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = do_HEAD = do_OPTIONS = _handle


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8787
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
