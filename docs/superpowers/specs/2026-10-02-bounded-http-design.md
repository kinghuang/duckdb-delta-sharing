# Bounded HTTP: request timeout and pagination guard (GRO-131)

## Problem

The extension's HTTP client (`src/delta_sharing_client.cpp`) bounds nothing.

- `PerformRequest` sets no curl timeout options, and ignores `http_timeout`.
  A server that accepts the connection and then stalls blocks the query indefinitely.
  A consumer measured 150 s with `http_timeout = 5` before its own harness gave up.
- The paging loops follow the server for as long as it keeps paging:
  - `PerformPaginatedGet` (`nextPageToken`) backs `ListShares`, `ListSchemas`, `ListTables` and `ListAllTables`.
  - `QueryTable` and `QueryTableChanges` follow `Link: <…>; rel="next"`.

  A server that echoes its token drew 68,214 requests in 12 s.

## Decisions (King, 2026-10-02)

| Area | Behaviour | Precedent |
|---|---|---|
| Timeout | `CONNECTTIMEOUT = http_timeout`; `LOW_SPEED_LIMIT = 1024` B/s for `LOW_SPEED_TIME = http_timeout` s; `TIMEOUT = 0` (no whole-transfer cap). | duckdb-httpfs v1.5 curl client, 1:1 ([`httpfs_curl_client.cpp#L185-L190`](https://github.com/duckdb/duckdb-httpfs/blob/b26737e884725a5ab384faca783fe00bdfb36fd6/src/httpfs_curl_client.cpp#L185-L190)) |
| Timeout default | `http_timeout` when set (httpfs' setting); otherwise `HTTPParams::DEFAULT_TIMEOUT_SECONDS` (30). | httpfs registers `http_timeout` with that same default ([`httpfs_extension.cpp#L49-L50`](https://github.com/duckdb/duckdb-httpfs/blob/b26737e884725a5ab384faca783fe00bdfb36fd6/src/httpfs_extension.cpp#L49-L50)); `ca_cert_file` is read the same way in this file. |
| Timeout hint | A `CURLE_OPERATION_TIMEDOUT` error gets `(http_timeout: N seconds; raise it with SET http_timeout)`. **The one delta from httpfs.** Offer to drop it. | Same shape as the certificate hint already in `PerformRequest` (#35). |
| Repeat guard | Always on: fail when a server returns a next-page token or link it already returned in this listing or query. It keeps a set, so it also catches cycles like `a, b, a`. | botocore `PaginationError`: "The same next token was received twice" ([`paginate.py#L322-L329`](https://github.com/boto/botocore/blob/9f5baa9742a6e65121479e786d6fba24b7ff940e/botocore/paginate.py#L322-L329)). |
| Page cap | New setting `delta_sharing_max_pages` (UBIGINT): default **1000**, `0` = no limit. Exceeding it fails, and the error names the setting. | No DuckDB extension caps pages. duckdb-iceberg's REST listing and the Spark and Python Delta Sharing clients loop without limit. The cap is the new policy this PR adds. |
| Failure | Both guards throw `HTTPException`; neither ever returns a partial result. List binds re-wrap it as `IOException("ListBind error: …")`, as they do for every client error. | Matches every other failure in these loops. |

Why 1000: a live Databricks test share needs 1 page per listing at the server's default page size, and a downstream consumer's own client caps listings at 100 pages. 1000 leaves a 10× margin over that and still stops a runaway server in seconds.

Out of scope:
- retries (`http_retries`)
- proxy settings
- the Emscripten/XHR path, which keeps no timeout because a synchronous XHR in a window cannot set one
- the page guard does apply to the Emscripten path, because it sits above `PerformRequest`

## Interaction with PR #40 (GRO-12) and PR #42 (GRO-135)

The branch starts from upstream `main` (`b757180`) and stays out of their hunks.

| File | #40 changes | #42 changes | This PR changes |
|---|---|---|---|
| `src/delta_sharing_client.cpp` | the secret lookup at the top of `FromConfig` | none | the settings read after `ca_cert_file` in `FromConfig`, the client constructor, the `PerformRequest` error branch, the three paging loops |
| `src/include/delta_sharing_client.hpp` | an include, plus a second `FromConfig` overload | none | new profile fields after `current_query`, plus a constant |
| `src/duckdb_delta_sharing_extension.cpp` | named parameters on the function registrations | the `base_read` serialize lines | one `AddExtensionOption` after the telemetry option |
| `README.md` | a new section 3 under Getting Started | none | the Configuration Options table, plus a section after TLS Certificates |

Plan check: `git merge-tree` of this PR branch against `gro-12-upstream-pr` and against `fix-41-empty-table-segfault` must come back clean. Likely merge order: #40 and #42 first (opened earlier), then this one; if anything conflicts, rebase this one.

Downstream adoption notes and impact on other consumers are tracked outside the repo.
