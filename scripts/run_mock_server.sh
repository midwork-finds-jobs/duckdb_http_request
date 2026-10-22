#!/usr/bin/env bash
# Start the local HTTP mock server used by the tests in the background.
set -euo pipefail
PORT="${HTTP_REQUEST_TEST_PORT:-8787}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if command -v uv >/dev/null 2>&1; then
  RUN=(uv run --no-project "$DIR/test/mock_server/echo_server.py")
else
  RUN=(python3 "$DIR/test/mock_server/echo_server.py")
fi
nohup "${RUN[@]}" "$PORT" >"${TMPDIR:-/tmp}/http_request_mock.log" 2>&1 &
echo $! >"${TMPDIR:-/tmp}/http_request_mock.pid"
for _ in $(seq 1 50); do
  if curl -fsS "http://127.0.0.1:${PORT}/get" >/dev/null 2>&1; then
    echo "mock server listening on ${PORT}"
    exit 0
  fi
  sleep 0.2
done
echo "mock server failed to start" >&2
cat "${TMPDIR:-/tmp}/http_request_mock.log" >&2 || true
exit 1
