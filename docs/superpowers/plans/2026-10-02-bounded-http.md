# Bounded HTTP (timeout + pagination guard) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every Delta Sharing HTTP call fail fast on a stalled server, and make every paged response fail loudly on a looping or runaway server. Then a scheduled caller can list shares through `delta_share_list` safely.

**Architecture:**
- Timeout: the curl handle gets httpfs' four timeout options once, in the `DeltaSharingClient` constructor, from `http_timeout`.
- Paging: a small `PageGuard` in `delta_sharing_client.cpp` watches every paging loop (`PerformPaginatedGet`, `QueryTable`, `QueryTableChanges`):
  - it rejects a next-page token or link the server already returned
  - it stops once the response passes `delta_sharing_max_pages` pages
- Both guards fail with an `HTTPException`; neither returns a partial result.

**Tech Stack:**
- C++17 DuckDB extension (v1.5.5) with libcurl
- sqllogictest
- bash + Python 3 stdlib for the integration script and stub server

**Spec:** `docs/superpowers/specs/2026-10-02-bounded-http-design.md`

## Global Constraints

- Base: upstream `main` `b757180`. Keep the diff out of PR #40's hunks (the secret lookup at the top of `FromConfig`, function registrations) and PR #42's hunks (the `base_read` serialize lines).
- Timeout options must be exactly httpfs v1.5's:
  - `CONNECTTIMEOUT = http_timeout`
  - `TIMEOUT = 0`
  - `LOW_SPEED_LIMIT = 1024`
  - `LOW_SPEED_TIME = http_timeout`
- `http_timeout` default when unset: `HTTPParams::DEFAULT_TIMEOUT_SECONDS` (30).
- New setting `delta_sharing_max_pages`: `UBIGINT`, default `1000`, `0` = no limit.
- The timeout hint text is exactly ` (http_timeout: N seconds; raise it with SET http_timeout)`.
- Text going upstream (code comments, README, commit message, PR) has no "we/our/us", and plain wording. No share names, endpoints or tenant details from the live test share.
- Gates, all of which must pass:
  - `GEN=ninja make release`
  - `./build/release/test/unittest --test-dir . "test/sql/*"`
  - `./build/release/duckdb -c "select 1"`
  - `./test/integration/run_http_bounds_tests.sh`
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `test/integration/misbehaving_sharing_server.py` | create | Stub sharing server. The first path segment picks a misbehaviour (echo, cycle, endless, three pages, self-link, stall). `GET /requests` reports how many requests it has answered. |
| `test/integration/run_http_bounds_tests.sh` | create | Starts the server and runs each case under a watchdog. Checks the error text, the number of requests and the elapsed seconds. |
| `src/include/delta_sharing_client.hpp` | modify | `DELTA_SHARING_DEFAULT_MAX_PAGES`; `http_timeout` and `max_pages` on `DeltaSharingProfile` |
| `src/delta_sharing_client.cpp` | modify | Read both settings in `FromConfig`; `ApplyTimeouts`; the timeout hint; `PageGuard` wired into the three loops |
| `src/duckdb_delta_sharing_extension.cpp` | modify | Register `delta_sharing_max_pages` |
| `test/sql/duckdb_delta_sharing.test` | modify | Pin the setting's default |
| `README.md` | modify | Configuration table rows, plus a "Timeouts and Paging" section |

---

### Task 1: Misbehaving server + integration script (RED, with control)

**Files:**
- Create: `test/integration/misbehaving_sharing_server.py`
- Create: `test/integration/run_http_bounds_tests.sh`

**Interfaces:**
- Produces:
  - Server modes `echo`, `cycle`, `endless`, `pages3`, `link`, `stall`, chosen by the first path segment of the secret's ENDPOINT.
  - `GET /requests` → plain-text count.
  - Script env vars: `DUCKDB_PATH`, `EXT_PATH`, `WATCHDOG` (seconds, default 30).
  - Expected error substrings, which Tasks 2 and 3 must produce verbatim:
    - `returned a next page token it had already returned`
    - `returned a next page link it had already returned`
    - `was still paging after N pages (delta_sharing_max_pages)`
    - `Timeout was reached`
    - `http_timeout: 2 seconds`

- [ ] **Step 1: Write the server**

```python
"""Delta Sharing server that misbehaves, for testing the extension's HTTP limits.

The first path segment picks the behaviour; the rest is a normal sharing API path,
so a secret with ENDPOINT 'http://127.0.0.1:<port>/<mode>' selects a mode:
  echo     listings return the page token they were sent (first page: "t0")
  cycle    listings alternate page tokens: a, b, a, ...
  endless  listings always return a new page token
  pages3   listings take three pages, then finish
  link     table queries and change feeds name themselves as the next page
  stall    accepts the request and never answers
GET /requests returns how many requests the server has answered (not counting itself).

Usage: misbehaving_sharing_server.py
Prints the bound port on the first line of stdout, then serves until killed.
"""
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

SCHEMA = json.dumps({"type": "struct", "fields": [
    {"name": "id", "type": "long", "nullable": True, "metadata": {}}]})
TABLE_LINES = [
    {"protocol": {"minReaderVersion": 1}},
    {"metaData": {"id": "t", "format": {"provider": "parquet"},
                  "schemaString": SCHEMA, "partitionColumns": []}},
]
NEXT_TOKEN = {
    "echo": lambda token: token or "t0",
    "cycle": lambda token: "b" if token == "a" else "a",
    "endless": lambda token: str(int(token or "0") + 1),
    "pages3": lambda token: {"": "p2", "p2": "p3"}.get(token, ""),
}

answered = 0
lock = threading.Lock()


class MisbehavingServer(BaseHTTPRequestHandler):
    def _send(self, body, content_type, headers=()):
        data = body.encode()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        for name, value in headers:
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(data)

    def _handle(self):
        global answered
        url = urlsplit(self.path)
        if url.path == "/requests":
            self._send(str(answered), "text/plain")
            return
        with lock:
            answered += 1
        mode, _, rest = url.path.lstrip("/").partition("/")
        if mode == "stall":
            time.sleep(600)
            return
        if rest.endswith("/query") or rest.endswith("/changes"):
            link = [("Link", f'</{rest}>; rel="next"')] if mode == "link" else []
            self._send("\n".join(json.dumps(line) for line in TABLE_LINES),
                       "application/x-ndjson", link)
            return
        token = parse_qs(url.query).get("pageToken", [""])[0]
        body = {"items": [{"name": f"item{answered}"}]}
        next_token = NEXT_TOKEN.get(mode, lambda _: "")(token)
        if next_token:
            body["nextPageToken"] = next_token
        self._send(json.dumps(body), "application/json")

    def do_GET(self):
        self._handle()

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        self._handle()

    def log_message(self, *args):
        pass


server = ThreadingHTTPServer(("127.0.0.1", 0), MisbehavingServer)
server.daemon_threads = True
print(server.server_port, flush=True)
server.serve_forever()
```

- [ ] **Step 2: Write the script**

```bash
#!/bin/bash
set -eo pipefail

echo "========================================================="
echo "HTTP Limit Integration Tests (local misbehaving server)"
echo "========================================================="

# A server that keeps paging, or never answers, must end the query with an
# error instead of hanging it. Each case runs under a watchdog, so a build
# without the limits fails here instead of hanging the script.

cd "$(dirname "$0")/../.."

DUCKDB_PATH=${DUCKDB_PATH:-"./build/release/duckdb"}
EXT_PATH=${EXT_PATH:-"./build/release/extension/duckdb_delta_sharing/duckdb_delta_sharing.duckdb_extension"}
WATCHDOG=${WATCHDOG:-30}

if [ ! -f "$DUCKDB_PATH" ]; then
    echo "DuckDB executable not found. Please compile the extension first using 'make release'."
    exit 1
fi

if [ ! -f "$EXT_PATH" ]; then
    echo "Extension not found. Please compile the extension first using 'make release'."
    exit 1
fi

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

python3 test/integration/misbehaving_sharing_server.py > "${WORK_DIR}/port" &
SERVER_PID=$!
disown "$SERVER_PID"
trap 'kill $SERVER_PID 2>/dev/null; rm -rf "$WORK_DIR"' EXIT

for _ in $(seq 50); do
    [ -s "${WORK_DIR}/port" ] && break
    sleep 0.1
done
PORT=$(head -n 1 "${WORK_DIR}/port")
if [ -z "$PORT" ]; then
    echo "ERROR: misbehaving server did not start"
    exit 1
fi

answered() {
    curl -s "http://127.0.0.1:${PORT}/requests"
}

FAILED=0

# run_case <name> <mode> <expected text> <max requests> <max seconds> <sql>
run_case() {
    local name=$1 mode=$2 expected=$3 max_requests=$4 max_seconds=$5 sql=$6
    local before start output status=0 requests elapsed
    before=$(answered)
    start=$(date +%s)
    # httpfs provides the http_timeout setting
    output=$(perl -e 'alarm shift; exec @ARGV' "$WATCHDOG" \
        "$DUCKDB_PATH" -unsigned -csv -noheader -c "
LOAD httpfs;
LOAD '${EXT_PATH}';
CREATE SECRET (TYPE delta_sharing, PROVIDER config, ENDPOINT 'http://127.0.0.1:${PORT}/${mode}', BEARER_TOKEN 'test');
${sql}" 2>&1) || status=$?
    elapsed=$(( $(date +%s) - start ))
    requests=$(( $(answered) - before ))
    if [[ "$output" != *"$expected"* ]] || [ "$requests" -gt "$max_requests" ] || [ "$elapsed" -gt "$max_seconds" ]; then
        echo "FAIL: ${name} (exit ${status}, ${requests} requests, ${elapsed} s)"
        echo "$output" | tail -n 3
        FAILED=1
        return
    fi
    echo "PASS: ${name} (${requests} requests, ${elapsed} s)"
}

run_case "listing that returns the token it was sent" echo \
    "returned a next page token it had already returned" 2 5 \
    "SELECT count(*) FROM delta_share_list();"

run_case "listing that cycles between two tokens" cycle \
    "returned a next page token it had already returned" 3 5 \
    "SELECT count(*) FROM delta_share_list('s');"

run_case "listing that never stops paging" endless \
    "was still paging after 5 pages (delta_sharing_max_pages)" 5 5 \
    "SET delta_sharing_max_pages = 5; SELECT count(*) FROM delta_share_list('s', 'sc');"

run_case "three pages within a cap of 3" pages3 "items=3" 3 5 \
    "SET delta_sharing_max_pages = 3; SELECT 'items=' || count(*) FROM delta_share_list_all_tables('s');"

run_case "three pages over a cap of 2" pages3 \
    "was still paging after 2 pages (delta_sharing_max_pages)" 2 5 \
    "SET delta_sharing_max_pages = 2; SELECT count(*) FROM delta_share_list_all_tables('s');"

run_case "three pages with no cap" pages3 "items=3" 3 5 \
    "SET delta_sharing_max_pages = 0; SELECT 'items=' || count(*) FROM delta_share_list_all_tables('s');"

run_case "table query that links to itself" link \
    "returned a next page link it had already returned" 2 5 \
    "SELECT count(*) FROM delta_share_read('s', 'sc', 't');"

run_case "change feed that links to itself" link \
    "returned a next page link it had already returned" 2 5 \
    "SELECT count(*) FROM delta_share_change_data_feed('s', 'sc', 't', 0);"

run_case "server that stalls after accepting" stall \
    "http_timeout: 2 seconds" 1 6 \
    "SET http_timeout = 2; SELECT count(*) FROM delta_share_list();"

if [ "$FAILED" -ne 0 ]; then
    echo "HTTP limit integration tests FAILED"
    exit 1
fi
echo "HTTP limit integration tests passed"
```

Then `chmod +x test/integration/run_http_bounds_tests.sh`.

- [ ] **Step 3: RED. Run it on the current (upstream `main`) build.**

Run: `WATCHDOG=10 ./test/integration/run_http_bounds_tests.sh`
Expected: exit 1, with every case FAIL.
- `echo`, `cycle`, `link` and `stall` hit the 10 s watchdog (exit 142), with thousands of requests for the paging modes.
- The `SET delta_sharing_max_pages` cases fail fast on the unknown setting.
- "three pages with no cap" fails the same way, because the setting doesn't exist yet.

Fix any case that fails for a reason other than a missing limit before moving on. Watch for a wrong path, a missing `/metadata` handler, or parse errors.

- [ ] **Step 4: Control on the published build (`a9ab9dd`, same client code as upstream `main`)**

Run: `DUCKDB_PATH=/opt/homebrew/bin/duckdb EXT_PATH=$HOME/.duckdb/extensions/v1.5.5/osx_arm64/duckdb_delta_sharing.duckdb_extension WATCHDOG=12 ./test/integration/run_http_bounds_tests.sh`
Expected: the same FAIL pattern as Step 3. Save the request counts and seconds for the echo, link and stall cases as the "before" numbers for the PR, in `scratchpad/control.txt`.

- [ ] **Step 5: Commit**

```bash
git add test/integration/misbehaving_sharing_server.py test/integration/run_http_bounds_tests.sh
git commit -m "test: add misbehaving-server tests for HTTP timeout and paging limits"
```

### Task 2: Request timeout (httpfs 1:1, plus hint)

**Files:**
- Modify: `src/include/delta_sharing_client.hpp`, after `std::string current_query;` in `DeltaSharingProfile`
- Modify: `src/delta_sharing_client.cpp`:
  - `FromConfig`, after the `ca_cert_file` block
  - the anonymous namespace, after `ApplyCertPath`
  - the constructor
  - the `PerformRequest` error branch

**Interfaces:**
- Produces: `DeltaSharingProfile::http_timeout` (`uint64_t`, default `HTTPParams::DEFAULT_TIMEOUT_SECONDS`).

- [ ] **Step 1: Profile field.** In `delta_sharing_client.hpp`, add `#include "duckdb/common/http_util.hpp"` after `#include "duckdb.hpp"`. Then add after `std::string current_query;`:

```cpp
    // httpfs' `http_timeout` (seconds); bounds connecting and stalled transfers the same way httpfs does.
    uint64_t http_timeout = HTTPParams::DEFAULT_TIMEOUT_SECONDS;
```

- [ ] **Step 2: Read the setting.** In `FromConfig`, directly after the `ca_cert_file` block, add:

```cpp
    Value http_timeout_value;
    if (context.TryGetCurrentSetting("http_timeout", http_timeout_value) && !http_timeout_value.IsNull()) {
        profile.http_timeout = http_timeout_value.GetValue<uint64_t>();
    }
```

- [ ] **Step 3: Apply the timeouts.** Add after `ApplyCertPath` in the anonymous namespace:

```cpp
// Same options as duckdb-httpfs' curl client (src/httpfs_curl_client.cpp):
//   connect         → fails after `http_timeout` seconds
//   stalled transfer → fails once it moves under 1 KB/s for `http_timeout` seconds
//   whole transfer  → no limit, so a large response that keeps arriving completes
void ApplyTimeouts(CURL *curl, uint64_t timeout) {
    const long seconds = static_cast<long>(timeout);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, seconds);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 0L);
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1024L);
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, seconds);
}
```

In the constructor, after `ApplyCertPath((CURL *)curl_, profile_.ca_cert_file);`:

```cpp
    ApplyTimeouts((CURL *)curl_, profile_.http_timeout);
```

- [ ] **Step 4: Hint.** In `PerformRequest`, after the certificate-hint `if` block and before `response.success = false;`:

```cpp
        if (res == CURLE_OPERATION_TIMEDOUT) {
            response.error_message += " (http_timeout: " + std::to_string(profile_.http_timeout) +
                                      " seconds; raise it with SET http_timeout)";
        }
```

- [ ] **Step 5: Build, then run the stall case**

Run: `GEN=ninja make release && WATCHDOG=20 ./test/integration/run_http_bounds_tests.sh`
Expected: `PASS: server that stalls after accepting (1 requests, 2 s)`, give or take a second. The paging cases still FAIL; that's Task 3.

- [ ] **Step 6: Commit**

```bash
git add src/include/delta_sharing_client.hpp src/delta_sharing_client.cpp
git commit -m "fix(client): bound every request with httpfs' http_timeout"
```

### Task 3: Pagination guard + `delta_sharing_max_pages`

**Files:**
- Modify: `src/include/delta_sharing_client.hpp`, the constant plus a profile field
- Modify: `src/delta_sharing_client.cpp`:
  - `FromConfig`
  - a new `PageGuard` in a second anonymous namespace placed after `GetNextPageLink`, outside `#ifndef __EMSCRIPTEN__`
  - `PerformPaginatedGet`, `QueryTable`, `QueryTableChanges`
- Modify: `src/duckdb_delta_sharing_extension.cpp`, the setting registration after the telemetry option
- Test: `test/sql/duckdb_delta_sharing.test`

**Interfaces:**
- Consumes: `DeltaSharingProfile` from Task 2.
- Produces:
  - `DELTA_SHARING_DEFAULT_MAX_PAGES` (`constexpr uint64_t`, 1000)
  - `DeltaSharingProfile::max_pages` (`uint64_t`)
  - `PageGuard(std::string error_prefix, std::string next_kind, uint64_t max_pages)` with `void Next(const std::string &next)`

- [ ] **Step 1: Failing sqllogictest.** Append to `test/sql/duckdb_delta_sharing.test`:

```
# Paging stops after this many pages (0 = no limit); the paging itself is
# covered by test/integration/run_http_bounds_tests.sh.
query I
SELECT current_setting('delta_sharing_max_pages');
----
1000
```

Run: `./build/release/test/unittest --test-dir . "test/sql/duckdb_delta_sharing.test"`
Expected: FAIL, with an unrecognized configuration parameter.

- [ ] **Step 2: Constant and field.** In `delta_sharing_client.hpp`, above `struct DeltaSharingProfile`:

```cpp
// Default for the `delta_sharing_max_pages` setting.
static constexpr uint64_t DELTA_SHARING_DEFAULT_MAX_PAGES = 1000;
```

After the `http_timeout` field:

```cpp
    // `delta_sharing_max_pages`: most pages one listing or query may take (0 = no limit).
    uint64_t max_pages = DELTA_SHARING_DEFAULT_MAX_PAGES;
```

- [ ] **Step 3: Register the setting.** In `LoadInternal`, after the `delta_sharing_query_telemetry_enabled` option:

```cpp
    config.AddExtensionOption("delta_sharing_max_pages",
        "Most pages a Delta Sharing listing or query may take before it fails (0 = no limit)",
        LogicalType::UBIGINT,
        Value::UBIGINT(DELTA_SHARING_DEFAULT_MAX_PAGES));
```

- [ ] **Step 4: Read it.** In `FromConfig`, after the `http_timeout` block:

```cpp
    Value max_pages_value;
    if (context.TryGetCurrentSetting("delta_sharing_max_pages", max_pages_value) && !max_pages_value.IsNull()) {
        profile.max_pages = max_pages_value.GetValue<uint64_t>();
    }
```

- [ ] **Step 5: `PageGuard`.** Add `#include <unordered_set>` next to `<map>`. After `GetNextPageLink`, add:

```cpp
namespace {

// Ends a paged response that would never finish, with an error rather than a
// partial result:
//   repeat → the server returned a next page it already returned, so following
//            it would loop forever (botocore fails the same way)
//   cap    → more than `delta_sharing_max_pages` pages (0 = no limit)
class PageGuard {
public:
    PageGuard(std::string error_prefix, std::string next_kind, uint64_t max_pages)
        : error_prefix_(std::move(error_prefix)), next_kind_(std::move(next_kind)), max_pages_(max_pages) {
    }

    //! Call with each next page token or link, before requesting that page.
    void Next(const std::string &next) {
        if (!seen_.insert(next).second) {
            throw HTTPException(error_prefix_ + ": the server returned a next page " + next_kind_ +
                                " it had already returned (after page " + std::to_string(pages_) +
                                "); stopping, because following it would never end.");
        }
        if (max_pages_ > 0 && pages_ >= max_pages_) {
            throw HTTPException(error_prefix_ + ": the server was still paging after " + std::to_string(max_pages_) +
                                " pages (delta_sharing_max_pages); stopping rather than returning a partial "
                                "result. Raise delta_sharing_max_pages, or set it to 0 for no limit.");
        }
        pages_++;
    }

private:
    std::string error_prefix_;
    std::string next_kind_;
    uint64_t max_pages_;
    uint64_t pages_ = 1;
    std::unordered_set<std::string> seen_;
};

} // namespace
```

- [ ] **Step 6: Wire `PerformPaginatedGet`.** Replace the loop body's token handling so the guard throws outside the parse `try`. The `try` re-wraps every `std::exception` as a `SerializationException`.

```cpp
JsonValue DeltaSharingClient::PerformPaginatedGet(const std::string &path, int max_results, const std::string &page_token) {
    json all_items = json::array();
    std::string current_token = page_token;
    PageGuard guard("Paginated GET error on " + path, "token", profile_.max_pages);

    while (true) {
        std::string query_params;
        if (max_results > 0) {
            query_params += "maxResults=" + std::to_string(max_results);
        }
        if (!current_token.empty()) {
            if (!query_params.empty()) query_params += "&";
            query_params += "pageToken=" + current_token;
        }

        auto response = PerformRequest("GET", path, query_params);
        if (!response.success) {
            throw HTTPException("Paginated GET error on " + path + ": request failed. " + response.error_message);
        }

        std::string next_token;
        try {
            auto j = json::parse(response.body);
            if (j.contains("items") && j["items"].is_array()) {
                for (auto &item : j["items"]) {
                    all_items.push_back(item);
                }
            }

            if (j.contains("nextPageToken") && j["nextPageToken"].is_string()) {
                next_token = j["nextPageToken"].get<std::string>();
            }
        } catch (const std::exception &e) {
            throw SerializationException("Paginated GET error on " + path + ": failed to parse response. " + std::string(e.what()));
        }

        if (next_token.empty()) {
            break;
        }
        guard.Next(next_token);
        current_token = next_token;
    }

    return JsonValue::FromInternal(&all_items);
}
```

- [ ] **Step 7: Wire `QueryTable` and `QueryTableChanges`.** In each, declare the guard next to `bool first_page = true;`:

```cpp
    PageGuard guard("QueryTable error", "link", profile_.max_pages);
```

(Use `"QueryTableChanges error"` in `QueryTableChanges`.) Then change the "Check for next page" block in both to:

```cpp
        // Check for next page
        next_url = GetNextPageLink(response.headers);
        if (next_url.empty()) {
            break;
        }
        guard.Next(next_url);
        first_page = false;
```

- [ ] **Step 8: Build and run everything**

Run: `GEN=ninja make release && ./build/release/test/unittest --test-dir . "test/sql/*" && ./build/release/duckdb -c "select 1" && ./test/integration/run_http_bounds_tests.sh`
Expected:
- the unittest passes (all assertions)
- `select 1` prints 1
- every integration case PASSes, with request counts echo 2, cycle 3, endless 5, pages3 3/2/3, link 2/2, stall 1

- [ ] **Step 9: Commit**

```bash
git add src/include/delta_sharing_client.hpp src/delta_sharing_client.cpp src/duckdb_delta_sharing_extension.cpp test/sql/duckdb_delta_sharing.test
git commit -m "fix(client): fail paged responses that repeat a page or exceed delta_sharing_max_pages"
```

### Task 4: README

**Files:**
- Modify: `README.md`. Add rows to the "⚙️ Configuration Options" table, and a section after "🔐 TLS Certificates".

- [ ] **Step 1:** Add these table rows after the `delta_sharing_query_telemetry_enabled` row:

```markdown
| `delta_sharing_max_pages` | `UBIGINT` | Most pages one listing or query may take before it fails (`0` = no limit) | `1000` |
| `http_timeout` | `UBIGINT` | `httpfs`' setting, in seconds: how long a connection attempt, or a transfer moving under 1 KB/s, may take before the request fails | `30` |
```

- [ ] **Step 2:** Add the section after the TLS Certificates paragraph, before `---`:

```markdown
### ⏱️ Timeouts and Paging
Every request uses `httpfs`' `http_timeout` the way `httpfs` does: connecting may take that many seconds, and a response that slows to under 1 KB/s for that long fails. A large response that keeps arriving is never cut off. With `httpfs` not loaded, the timeout is 30 seconds. `SET http_timeout = 0` turns the stall check off.

Paged responses (listings, and table queries a server splits across pages) end with an error, never a partial result, when:
- the server returns a next page it already returned, which would never end, or
- the response takes more than `delta_sharing_max_pages` pages (`SET delta_sharing_max_pages = 0` for no limit).
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: describe http_timeout and delta_sharing_max_pages"
```

### Task 5: Review, live regression, merge check, PR branch

- [ ] **Step 1: Code review.** Dispatch the code-reviewer agent on `git diff b757180...HEAD -- src test README.md`. Fix CRITICAL/HIGH findings, re-run the Task 3 Step 8 gates, and commit.
- [ ] **Step 2: Live regression (read-only).** Export the live test share's credentials from the local env script, which is not in the repo. Then run `CREATE SECRET (TYPE delta_sharing, PROVIDER env)`, the four listings and a read of a small known table with the new build. Expected: the same counts as the published build. Keep the share name and endpoint out of anything public.
- [ ] **Step 3: Merge check.** For each of `origin/gro-12-upstream-pr` and `origin/fix-41-empty-table-segfault`, run `git merge-tree --write-tree --name-only HEAD <branch>`. Expected: exit 0 and no conflicted paths.
- [ ] **Step 4: Squash the PR branch.** `git switch -c gro-131-upstream-pr b757180`, then `git checkout gro-131-upstream-bound-duckdb-delta-sharing-http-timeout-pagination -- src test README.md`. Commit once with a conventional message (no first-person plural), then push both branches to `origin` (King's fork).
- [ ] **Step 5: Drafts for King.** Write the upstream issue draft (short title, problem plus repro numbers, "Proposed solutions" each linked to its precedent) and the PR draft ("Fixes #N", test plan, before and after numbers, the "one delta from httpfs" note). Save both to the scratchpad and show King. Post nothing until he approves.
- [ ] **Step 6: Linear.** Attach both fork branches to GRO-131 and post a status comment. Once King approves and the upstream PR is open, attach it and move GRO-131 to In Review.
