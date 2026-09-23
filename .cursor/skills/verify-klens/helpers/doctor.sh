#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

pid=$(read_pid)
pid_alive "$pid" || die "pid $pid is not running"
comm=$(tr -d '[:space:]' </proc/"$pid"/comm)
[[ "$comm" == "klens" ]] || die "pid $pid comm is $comm, expected klens"

port=$(tr -d '[:space:]' <"$RUN_DIR/port")
url=$(recorded_url)

owner=""
if command -v lsof >/dev/null 2>&1; then
  owner=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN -t 2>/dev/null | head -n 1 || true)
fi
if [[ -z "$owner" ]] && command -v ss >/dev/null 2>&1; then
  owner=$(ss -ltnpH "sport = :$port" 2>/dev/null | sed -n 's/.*pid=\([0-9]*\).*/\1/p' | head -n 1 || true)
fi
[[ "$owner" == "$pid" ]] || die "port $port is owned by ${owner:-nobody}, expected pid $pid"

[[ "$(http_code "$url/health")" == "204" ]] || die "GET /health is not 204"
[[ "$(http_code "$url/ready")" == "204" ]] || die "GET /ready is not 204"
[[ "$(http_code "$url/")" == "200" ]] || die "GET / is not 200"

topic_name=$(tr -d '[:space:]' <"$RUN_DIR/topic")
python3 - "$url" "$topic_name" <<'PY'
import json
import sys
import urllib.error
import urllib.request

url, topic = sys.argv[1], sys.argv[2]

def get(path):
    with urllib.request.urlopen(url + path, timeout=5) as response:
        body = response.read()
        return response.status, response.headers.get_content_type(), body

status, content_type, body = get("/")
text = body.decode("utf-8", "replace")
if "<title>klens</title>" not in text:
    raise SystemExit("GET / does not include <title>klens</title>")

status, _, body = get("/login")
login = body.decode("utf-8", "replace")
if status != 200 or "<title>klens</title>" not in login:
    raise SystemExit(f"GET /login returned {status} without the klens shell")

try:
    with urllib.request.urlopen(url + "/api/auth/login", timeout=5) as response:
        login_status = response.status
except urllib.error.HTTPError as error:
    login_status = error.code
if login_status != 404:
    raise SystemExit(f"GET /api/auth/login returned {login_status}, expected 404 when auth is omitted")

with urllib.request.urlopen(url + "/api/auth/me", timeout=5) as response:
    me = json.load(response)
if me.get("enabled") is not False or me.get("user") is not None:
    raise SystemExit(f"/api/auth/me is {me}, expected enabled false and user null")

with urllib.request.urlopen(url + "/api/clusters", timeout=5) as response:
    clusters = json.load(response)
local = next((item for item in clusters if item.get("cluster") == "local"), None)
if local is None:
    raise SystemExit("/api/clusters has no local cluster")
if local.get("ready") is not True:
    raise SystemExit(f"local cluster is not ready: {local.get('topology')}")
topology = local.get("topology") or {}
if not topology.get("updatedAt"):
    raise SystemExit("topology.updatedAt is empty")
if topology.get("lastError"):
    raise SystemExit(f"topology.lastError is set: {topology['lastError']}")

with urllib.request.urlopen(url + "/api/clusters/local/topics", timeout=5) as response:
    page = json.load(response)
names = [row.get("name") for row in page.get("rows", [])]
if topic not in names:
    raise SystemExit(f"{topic} is missing from /api/clusters/local/topics")
PY

printf 'doctor ok %s pid %s\n' "$url" "$pid"
