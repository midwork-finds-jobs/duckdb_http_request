#!/usr/bin/env bash
# Stop the local HTTP mock server.
PIDFILE="${TMPDIR:-/tmp}/http_request_mock.pid"
if [ -f "$PIDFILE" ]; then
  kill "$(cat "$PIDFILE")" 2>/dev/null || true
  rm -f "$PIDFILE"
fi
