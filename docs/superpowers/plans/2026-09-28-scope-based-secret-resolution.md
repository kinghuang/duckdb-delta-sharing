# Scope-based Secret Resolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Change how `delta_share_*` functions choose their `delta_sharing` secret. Each call resolves the secret deterministically: by SCOPE when it passes `endpoint :=`, by name when it passes `secret :=`, and by a defined default rule when it passes neither. This replaces "last secret wins".

**Architecture:** A new resolution unit (`delta_sharing_secret_resolution.{hpp,cpp}`) is the only place a secret gets chosen. It delegates all ranking to DuckDB's `SecretManager::LookupSecret` and `GetSecretByName`, and adds an endpoint guard. `DeltaSharingProfile::FromConfig` calls it. The table functions register two named parameters and pass them through. The read and CDF binds strip those parameters before forwarding to the inner `read_parquet` bind.

**Tech Stack:** C++17, DuckDB v1.5.5 extension API (the `duckdb` submodule at `d8cdaa33`), sqllogictest, CMake/ninja.

**Spec:** `docs/superpowers/specs/2026-09-28-scope-based-secret-resolution-design.md`

## Global Constraints

- DuckDB v1.5.5 (the submodule is pinned; don't bump it).
- The parameters are `endpoint` and `secret`, both `LogicalType::VARCHAR` and both optional. They go on `delta_share_list`, `delta_share_list_all_tables`, every `delta_share_read` overload and every `delta_share_change_data_feed` overload. `delta_share_list_files` gets none, because it's a scalar function.
- Default rule:
  - ① `LookupSecret(txn, "", "delta_sharing")`
  - ② otherwise, the sole `delta_sharing` secret
  - ③ otherwise an `InvalidConfigurationException` naming the secrets
- Endpoint guard: the requested `X` must equal the secret's `ENDPOINT` or extend it at a `/` boundary, after stripping trailing slashes from both. Matching is literal and case-sensitive, like DuckDB's `MatchScore`.
- Requests go to `X` when it's given, otherwise to the secret's `ENDPOINT`.
- All resolution errors are `InvalidConfigurationException`, raised before any HTTP call. They never include a bearer token.
- Secret format and `CREATE SECRET` behaviour stay unchanged.
- Existing tests must stay green: `test/sql/duckdb_delta_sharing.test` and `test/sql/variant.test`.
- Style: match `src/` (4-space indent, `namespace duckdb`, `std::move` for returns). Only use comments to explain a non-obvious WHY.
- Outward actions need King's explicit OK: anything on `prequel-co/*` and any community-extensions request. Pushing branches to `kinghuang/duckdb-delta-sharing` is fine.
- Spec and plan commits (`docs/superpowers/`) never go to upstream.

## Build and test commands (used by every task)

- Build: `GEN=ninja make release` (incremental after the first cold build).
- One test file: `./build/release/test/unittest --test-dir . "test/sql/secret_resolution.test"`
- All tests: `./build/release/test/unittest --test-dir . "test/sql/*"`
- Static CLI load order (sqllogictests don't cover this): `./build/release/duckdb -c "select 1"`

## How tests see which secret was chosen, without a network

Each secret's `ENDPOINT` uses its own `.invalid` host. RFC 6761 guarantees that name never resolves, so the call fails offline with curl's `Could not resolve host: <host>`, which identifies the secret.

With `endpoint :=`, requests always go to `X`, so the host can't tell the candidates apart. Those tests give every candidate an `ENDPOINT` on `elsewhere.invalid`, which never covers `X`. The guard error then names the secret the lookup chose: `secret 'narrow' has ENDPOINT …`.

## File Structure

| File | Responsibility |
|---|---|
| Create `src/include/delta_sharing_secret_resolution.hpp` | `DeltaSharingSecretRequest`, `ResolvedDeltaSharingSecret`, `ResolveDeltaSharingSecret`, `EndpointCovers` |
| Create `src/delta_sharing_secret_resolution.cpp` | The resolution rules, the guard, and named-parameter parsing and stripping |
| Modify `CMakeLists.txt:60-66` | Add the new source to `EXTENSION_SOURCES` |
| Modify `src/include/delta_sharing_client.hpp:12-24` | `FromConfig(context, request)` overload |
| Modify `src/delta_sharing_client.cpp:213-243` | `FromConfig` calls the resolver in place of the `AllSecrets` loop |
| Modify `src/duckdb_delta_sharing_extension.cpp` | Register the parameters, pass the request in each bind, strip it before the inner `read_parquet` bind |
| Create `test/sql/secret_resolution.test` | The offline resolution matrix |
| Modify `README.md` | A "Choosing a secret" section |

---

### Task 1: Resolution unit and the default rule

**Files:**
- Create: `src/include/delta_sharing_secret_resolution.hpp`
- Create: `src/delta_sharing_secret_resolution.cpp`
- Modify: `CMakeLists.txt:60-66`
- Modify: `src/include/delta_sharing_client.hpp:1-24`
- Modify: `src/delta_sharing_client.cpp:213-243`
- Test: `test/sql/secret_resolution.test`

**Interfaces:**
- Produces:
  - `struct DeltaSharingSecretRequest { string endpoint; string secret; }`, plus the static helpers `FromNamedParameters(const named_parameter_map_t &)`, `WithoutRequestParameters(const named_parameter_map_t &)` and `AddNamedParameters(TableFunction &)`. They're defined here and used in Tasks 2 and 3.
  - `struct ResolvedDeltaSharingSecret { unique_ptr<SecretEntry> entry; string endpoint; const KeyValueSecret &Secret() const; }`
  - `ResolvedDeltaSharingSecret ResolveDeltaSharingSecret(ClientContext &, const DeltaSharingSecretRequest &)`
  - `bool EndpointCovers(const string &secret_endpoint, const string &requested)`
  - `DeltaSharingProfile::FromConfig(ClientContext &, const DeltaSharingSecretRequest &)`. The existing `FromConfig(ClientContext &)` delegates to it with `{}`.

- [ ] **Step 1: Write the failing test (default rule only)**

Create `test/sql/secret_resolution.test`:

```
# name: test/sql/secret_resolution.test
# description: which delta_sharing secret a delta_share_* call resolves
# group: [sql]

require parquet

require duckdb_delta_sharing

# Every ENDPOINT is its own `.invalid` host (RFC 6761: never resolves), so a
# call fails offline with curl's "Could not resolve host: <host>" — the host
# names the secret the call resolved. Persistent secrets land in local_file
# storage (tie-break 20), standing in for an attached store against memory (10).

statement ok
SET secret_directory='__TEST_DIR__/secret_resolution';

# --- Default rule (no endpoint := / secret :=) ------------------------------

statement error
SELECT * FROM delta_share_list();
----
Please configure Delta Sharing via a secret

statement ok
CREATE SECRET alpha (TYPE delta_sharing, ENDPOINT 'https://alpha.invalid/ds', BEARER_TOKEN 'a');

statement error
SELECT * FROM delta_share_list();
----
Could not resolve host: alpha.invalid

# ① an unscoped secret beats a scoped one, whatever their names
statement ok
CREATE SECRET zeta (TYPE delta_sharing, ENDPOINT 'https://zeta.invalid/ds', BEARER_TOKEN 'z', SCOPE 'https://zeta.invalid/ds');

statement error
SELECT * FROM delta_share_list();
----
Could not resolve host: alpha.invalid

# ① between unscoped secrets, memory beats a persistent storage
statement ok
CREATE PERSISTENT SECRET aaa_stored (TYPE delta_sharing, ENDPOINT 'https://stored.invalid/ds', BEARER_TOKEN 's');

statement error
SELECT * FROM delta_share_list();
----
Could not resolve host: alpha.invalid

statement ok
DROP SECRET alpha;

statement error
SELECT * FROM delta_share_list();
----
Could not resolve host: stored.invalid

# ③ only scoped secrets, more than one: refuse rather than guess
statement ok
DROP PERSISTENT SECRET aaa_stored;

statement ok
CREATE SECRET yotta (TYPE delta_sharing, ENDPOINT 'https://yotta.invalid/ds', BEARER_TOKEN 'y', SCOPE 'https://yotta.invalid/ds');

statement error
SELECT * FROM delta_share_list();
----
2 delta_sharing secrets are scoped and none is unscoped (yotta, zeta)

# ② exactly one secret, even a scoped one: use it
statement ok
DROP SECRET yotta;

statement error
SELECT * FROM delta_share_list();
----
Could not resolve host: zeta.invalid

# delta_share_list_files (scalar: no named parameters) follows the same rule
statement error
SELECT delta_share_list_files('s', 'sc', 't');
----
Could not resolve host: zeta.invalid

statement ok
DROP SECRET zeta;
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `./build/release/test/unittest --test-dir . "test/sql/secret_resolution.test"`
Expected: FAIL.
- Legacy last-wins picks `zeta` where the test expects `alpha` (scan order is by name within memory).
- It picks one secret where the test expects the ③ error.

- [ ] **Step 3: Write the header**

Create `src/include/delta_sharing_secret_resolution.hpp`:

```cpp
#pragma once

#include "duckdb.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

// Which delta_sharing secret a delta_share_* call asked for, from its
// `endpoint :=` and `secret :=` named parameters. Both empty = the default rule.
struct DeltaSharingSecretRequest {
    string endpoint;
    string secret;

    static DeltaSharingSecretRequest FromNamedParameters(const named_parameter_map_t &named_parameters);
    // A copy without `endpoint`/`secret`, for binds that forward to read_parquet.
    static named_parameter_map_t WithoutRequestParameters(const named_parameter_map_t &named_parameters);
    static void AddNamedParameters(TableFunction &function);
};

struct ResolvedDeltaSharingSecret {
    unique_ptr<SecretEntry> entry;
    // The requested endpoint when one was given, otherwise the secret's ENDPOINT.
    string endpoint;

    const KeyValueSecret &Secret() const;
};

ResolvedDeltaSharingSecret ResolveDeltaSharingSecret(ClientContext &context, const DeltaSharingSecretRequest &request);

// Whether `requested` is `secret_endpoint` or extends it at a '/' boundary,
// ignoring trailing slashes on both.
bool EndpointCovers(const string &secret_endpoint, const string &requested);

} // namespace duckdb
```

- [ ] **Step 4: Write the implementation**

Create `src/delta_sharing_secret_resolution.cpp`:

```cpp
#include "delta_sharing_secret_resolution.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <algorithm>

namespace duckdb {

static constexpr const char *DELTA_SHARING_SECRET_TYPE = "delta_sharing";
static constexpr const char *ENDPOINT_PARAMETER = "endpoint";
static constexpr const char *SECRET_PARAMETER = "secret";
static constexpr const char *NO_SECRET_MESSAGE =
    "LoadProfile error: Please configure Delta Sharing via a secret: CREATE SECRET (TYPE delta_sharing, PROVIDER "
    "config, ENDPOINT '...', BEARER_TOKEN '...') or CREATE SECRET (TYPE delta_sharing, PROVIDER env)";

static string RequireNonEmpty(const Value &value, const string &parameter) {
    if (value.IsNull() || value.ToString().empty()) {
        throw InvalidConfigurationException("delta_sharing: %s := must be a non-empty string", parameter);
    }
    return value.ToString();
}

DeltaSharingSecretRequest DeltaSharingSecretRequest::FromNamedParameters(const named_parameter_map_t &named_parameters) {
    DeltaSharingSecretRequest request;
    auto endpoint = named_parameters.find(ENDPOINT_PARAMETER);
    if (endpoint != named_parameters.end()) {
        request.endpoint = RequireNonEmpty(endpoint->second, ENDPOINT_PARAMETER);
    }
    auto secret = named_parameters.find(SECRET_PARAMETER);
    if (secret != named_parameters.end()) {
        request.secret = RequireNonEmpty(secret->second, SECRET_PARAMETER);
    }
    return request;
}

named_parameter_map_t DeltaSharingSecretRequest::WithoutRequestParameters(const named_parameter_map_t &named_parameters) {
    named_parameter_map_t result;
    for (auto &parameter : named_parameters) {
        if (!StringUtil::CIEquals(parameter.first, ENDPOINT_PARAMETER) &&
            !StringUtil::CIEquals(parameter.first, SECRET_PARAMETER)) {
            result.emplace(parameter.first, parameter.second);
        }
    }
    return result;
}

void DeltaSharingSecretRequest::AddNamedParameters(TableFunction &function) {
    function.named_parameters[ENDPOINT_PARAMETER] = LogicalType::VARCHAR;
    function.named_parameters[SECRET_PARAMETER] = LogicalType::VARCHAR;
}

static string WithoutTrailingSlashes(const string &url) {
    auto end = url.find_last_not_of('/');
    return end == string::npos ? string() : url.substr(0, end + 1);
}

bool EndpointCovers(const string &secret_endpoint, const string &requested) {
    auto base = WithoutTrailingSlashes(secret_endpoint);
    auto target = WithoutTrailingSlashes(requested);
    if (base.empty()) {
        return false;
    }
    return target == base || StringUtil::StartsWith(target, base + "/");
}

const KeyValueSecret &ResolvedDeltaSharingSecret::Secret() const {
    auto key_value = dynamic_cast<const KeyValueSecret *>(entry->secret.get());
    if (!key_value) {
        throw InvalidConfigurationException("delta_sharing: secret '%s' is not a key-value secret",
                                            entry->secret->GetName());
    }
    return *key_value;
}

static bool IsDeltaSharing(const SecretEntry &entry) {
    return entry.secret && StringUtil::CIEquals(entry.secret->GetType(), DELTA_SHARING_SECRET_TYPE);
}

// The default rule, used when a call names neither an endpoint nor a secret.
//
//   ① the best unscoped secret: `LookupSecret("")` matches only secrets without
//     a SCOPE, ranked by DuckDB (storage tie-break offset, then name)
//   ② else the sole delta_sharing secret, scoped or not
//   ③ else refuse — picking among scoped secrets would be a guess
static unique_ptr<SecretEntry> ResolveDefault(SecretManager &manager, CatalogTransaction transaction) {
    auto unscoped = manager.LookupSecret(transaction, "", DELTA_SHARING_SECRET_TYPE);
    if (unscoped.HasMatch()) {
        return std::move(unscoped.secret_entry);
    }
    vector<SecretEntry> candidates;
    for (auto &entry : manager.AllSecrets(transaction)) {
        if (IsDeltaSharing(entry)) {
            candidates.push_back(entry);
        }
    }
    if (candidates.empty()) {
        throw InvalidConfigurationException(NO_SECRET_MESSAGE);
    }
    if (candidates.size() == 1) {
        return make_uniq<SecretEntry>(candidates[0]);
    }
    vector<string> names;
    for (auto &candidate : candidates) {
        names.push_back(candidate.secret->GetName());
    }
    std::sort(names.begin(), names.end());
    throw InvalidConfigurationException(
        "delta_sharing: %s delta_sharing secrets are scoped and none is unscoped (%s); pass endpoint := or "
        "secret := to choose one",
        to_string(names.size()), StringUtil::Join(names, ", "));
}

static unique_ptr<SecretEntry> ResolveByName(SecretManager &manager, CatalogTransaction transaction,
                                             const string &name) {
    auto entry = manager.GetSecretByName(transaction, name);
    if (!entry) {
        throw InvalidConfigurationException("delta_sharing: secret := '%s' does not exist", name);
    }
    if (!IsDeltaSharing(*entry)) {
        throw InvalidConfigurationException("delta_sharing: secret := '%s' is a %s secret, not delta_sharing", name,
                                            entry->secret->GetType());
    }
    return entry;
}

static unique_ptr<SecretEntry> ResolveByEndpoint(SecretManager &manager, CatalogTransaction transaction,
                                                 const string &endpoint) {
    auto match = manager.LookupSecret(transaction, endpoint, DELTA_SHARING_SECRET_TYPE);
    if (!match.HasMatch()) {
        throw InvalidConfigurationException("delta_sharing: no delta_sharing secret's SCOPE matches endpoint := '%s'",
                                            endpoint);
    }
    return std::move(match.secret_entry);
}

static string SecretEndpoint(const KeyValueSecret &secret) {
    Value value;
    if (!secret.TryGetValue("endpoint", value) || value.IsNull()) {
        return string();
    }
    return value.ToString();
}

ResolvedDeltaSharingSecret ResolveDeltaSharingSecret(ClientContext &context, const DeltaSharingSecretRequest &request) {
    auto &manager = SecretManager::Get(context);
    auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

    ResolvedDeltaSharingSecret resolved;
    if (!request.secret.empty()) {
        resolved.entry = ResolveByName(manager, transaction, request.secret);
    } else if (!request.endpoint.empty()) {
        resolved.entry = ResolveByEndpoint(manager, transaction, request.endpoint);
    } else {
        resolved.entry = ResolveDefault(manager, transaction);
    }

    auto secret_endpoint = SecretEndpoint(resolved.Secret());
    if (request.endpoint.empty()) {
        resolved.endpoint = secret_endpoint;
        return resolved;
    }
    // Never send a secret's bearer to a URL outside its own ENDPOINT: an
    // unscoped secret matches every endpoint := at score 0.
    if (!EndpointCovers(secret_endpoint, request.endpoint)) {
        throw InvalidConfigurationException(
            "delta_sharing: secret '%s' has ENDPOINT '%s', which does not cover endpoint := '%s'; give each secret "
            "a SCOPE, or pass secret := to choose one",
            resolved.Secret().GetName(), secret_endpoint, request.endpoint);
    }
    resolved.endpoint = request.endpoint;
    return resolved;
}

} // namespace duckdb
```

- [ ] **Step 5: Register the source in CMake**

In `CMakeLists.txt`, add one line to `EXTENSION_SOURCES` after `src/delta_sharing_client.cpp`:

```cmake
    src/delta_sharing_client.cpp
    src/delta_sharing_secret_resolution.cpp
```

- [ ] **Step 6: Route `FromConfig` through the resolver**

In `src/include/delta_sharing_client.hpp`, add `#include "delta_sharing_secret_resolution.hpp"` after `#include "delta_sharing_json.hpp"`. Then replace:

```cpp
    static DeltaSharingProfile FromConfig(ClientContext &context);
```

with:

```cpp
    static DeltaSharingProfile FromConfig(ClientContext &context);
    static DeltaSharingProfile FromConfig(ClientContext &context, const DeltaSharingSecretRequest &request);
```

In `src/delta_sharing_client.cpp`, replace everything from `DeltaSharingProfile DeltaSharingProfile::FromConfig(ClientContext &context) {` through the closing `}` of the endpoint `if/else` block (the line after `profile.endpoint = endpoint_value.ToString();` … `throw …;` `}`) with:

```cpp
DeltaSharingProfile DeltaSharingProfile::FromConfig(ClientContext &context) {
    return FromConfig(context, DeltaSharingSecretRequest());
}

DeltaSharingProfile DeltaSharingProfile::FromConfig(ClientContext &context, const DeltaSharingSecretRequest &request) {
    DeltaSharingProfile profile;

    auto resolved = ResolveDeltaSharingSecret(context, request);
    const KeyValueSecret *ds_secret = &resolved.Secret();

    if (resolved.endpoint.empty()) {
        throw InvalidConfigurationException("LoadProfile error: Please configure Delta Sharing via a secret: CREATE SECRET (TYPE delta_sharing, PROVIDER config, ENDPOINT '...', BEARER_TOKEN '...') or CREATE SECRET (TYPE delta_sharing, PROVIDER env)");
    }
    profile.endpoint = resolved.endpoint;
```

Leave the rest of the function as it is: the `Value token_value;` block onward, which reads `bearer_token` from `ds_secret`, the settings, and the trailing-slash trim.

- [ ] **Step 7: Build and run the test until it passes**

Run: `GEN=ninja make release && ./build/release/test/unittest --test-dir . "test/sql/secret_resolution.test"`
Expected: `All tests passed`.

If `SET secret_directory` fails with "Changing Secret Manager settings after the secret manager is used is not allowed", move it above `require duckdb_delta_sharing`.

- [ ] **Step 8: Run the full suite and the static CLI**

Run: `./build/release/test/unittest --test-dir . "test/sql/*" && ./build/release/duckdb -c "select 1"`
Expected: all tests pass, and the CLI prints `1`.

- [ ] **Step 9: Commit**

```bash
git add CMakeLists.txt src/include/delta_sharing_secret_resolution.hpp src/delta_sharing_secret_resolution.cpp src/include/delta_sharing_client.hpp src/delta_sharing_client.cpp test/sql/secret_resolution.test
git commit -m "feat: resolve the delta_sharing secret deterministically, not last-wins"
```

---

### Task 2: `endpoint :=` and `secret :=` on the listing functions

**Files:**
- Modify: `src/duckdb_delta_sharing_extension.cpp`, in `ListBind` (~L74), `AllTablesBind` (~L160) and `LoadInternal` (the `list` / `all_tables` registration)
- Test: `test/sql/secret_resolution.test` (append)

**Interfaces:**
- Consumes (from Task 1):
  - `DeltaSharingSecretRequest::FromNamedParameters`
  - `DeltaSharingSecretRequest::AddNamedParameters`
  - `DeltaSharingProfile::FromConfig(context, request)`

- [ ] **Step 1: Append the failing tests**

Append to `test/sql/secret_resolution.test`:

```
# --- endpoint := (longest SCOPE prefix; requests go to the endpoint) ---------
# With endpoint :=, requests go to X whatever the secret, so these secrets'
# ENDPOINTs sit on elsewhere.invalid, which never covers X: the guard error
# then names the secret the lookup chose.

statement ok
CREATE SECRET wide (TYPE delta_sharing, ENDPOINT 'https://elsewhere.invalid/w', BEARER_TOKEN 'w', SCOPE 'https://shared.invalid/');

statement ok
CREATE SECRET narrow (TYPE delta_sharing, ENDPOINT 'https://elsewhere.invalid/n', BEARER_TOKEN 'n', SCOPE 'https://shared.invalid/ds');

statement error
SELECT * FROM delta_share_list(endpoint := 'https://shared.invalid/ds');
----
secret 'narrow' has ENDPOINT 'https://elsewhere.invalid/n', which does not cover endpoint := 'https://shared.invalid/ds'

statement ok
DROP SECRET wide;

statement ok
DROP SECRET narrow;

# A scoped match beats an unscoped secret, whatever the storage (pinned: this
# is DuckDB's scoring, and the reason callers holding both pass secret :=).
statement ok
CREATE SECRET session_unscoped (TYPE delta_sharing, ENDPOINT 'https://elsewhere.invalid/u', BEARER_TOKEN 'u');

statement ok
CREATE PERSISTENT SECRET store_scoped (TYPE delta_sharing, ENDPOINT 'https://elsewhere.invalid/s', BEARER_TOKEN 's', SCOPE 'https://shared.invalid/ds');

statement error
SELECT * FROM delta_share_list(endpoint := 'https://shared.invalid/ds');
----
secret 'store_scoped'

# At equal SCOPE, memory beats a persistent storage
statement ok
CREATE SECRET session_scoped (TYPE delta_sharing, ENDPOINT 'https://elsewhere.invalid/m', BEARER_TOKEN 'm', SCOPE 'https://shared.invalid/ds');

statement error
SELECT * FROM delta_share_list(endpoint := 'https://shared.invalid/ds');
----
secret 'session_scoped'

statement ok
DROP SECRET session_scoped;

statement ok
DROP PERSISTENT SECRET store_scoped;

statement ok
DROP SECRET session_unscoped;

# The guard passes: requests go to the endpoint
statement ok
CREATE SECRET covered (TYPE delta_sharing, ENDPOINT 'https://covered.invalid/ds/', BEARER_TOKEN 'c', SCOPE 'https://covered.invalid/ds');

statement error
SELECT * FROM delta_share_list(endpoint := 'https://covered.invalid/ds');
----
Could not resolve host: covered.invalid

statement error
SELECT * FROM delta_share_list(endpoint := 'https://covered.invalid/ds/sub/');
----
Could not resolve host: covered.invalid

# Extending the ENDPOINT without a '/' boundary is not covered
statement error
SELECT * FROM delta_share_list(endpoint := 'https://covered.invalid/dsx');
----
which does not cover endpoint := 'https://covered.invalid/dsx'

statement error
SELECT * FROM delta_share_list(endpoint := 'https://nowhere.invalid/ds');
----
no delta_sharing secret's SCOPE matches endpoint := 'https://nowhere.invalid/ds'

statement error
SELECT * FROM delta_share_list(endpoint := NULL);
----
endpoint := must be a non-empty string

# --- secret := (by name; SCOPE is ignored) ----------------------------------

statement ok
CREATE SECRET named (TYPE delta_sharing, ENDPOINT 'https://named.invalid/ds', BEARER_TOKEN 'x', SCOPE 'https://somewhere-else.invalid/');

statement error
SELECT * FROM delta_share_list(secret := 'named');
----
Could not resolve host: named.invalid

statement error
SELECT * FROM delta_share_list_all_tables('s', secret := 'named');
----
Could not resolve host: named.invalid

statement error
SELECT * FROM delta_share_list(secret := 'missing');
----
secret := 'missing' does not exist

statement ok
CREATE SECRET plain_http (TYPE http);

statement error
SELECT * FROM delta_share_list(secret := 'plain_http');
----
secret := 'plain_http' is a http secret, not delta_sharing

statement error
SELECT * FROM delta_share_list(secret := '');
----
secret := must be a non-empty string

# Both: chosen by name, then the guard applies to the endpoint
statement error
SELECT * FROM delta_share_list(secret := 'named', endpoint := 'https://covered.invalid/ds');
----
secret 'named' has ENDPOINT 'https://named.invalid/ds', which does not cover endpoint := 'https://covered.invalid/ds'

statement error
SELECT * FROM delta_share_list(secret := 'named', endpoint := 'https://named.invalid/ds/sub');
----
Could not resolve host: named.invalid

statement ok
DROP SECRET plain_http;

statement ok
DROP SECRET named;

statement ok
DROP SECRET covered;
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `./build/release/test/unittest --test-dir . "test/sql/secret_resolution.test"`
Expected: FAIL with a binder error like `Invalid named parameter "endpoint" for function delta_share_list`.

- [ ] **Step 3: Register the parameters and pass the request**

In `src/duckdb_delta_sharing_extension.cpp`:

Add `#include "delta_sharing_secret_resolution.hpp"` after `#include "delta_share_multi_file_reader.hpp"`.

In `ListBind` and in `AllTablesBind`, replace:

```cpp
        DeltaSharingProfile profile = DeltaSharingProfile::FromConfig(context);
```

with:

```cpp
        DeltaSharingProfile profile = DeltaSharingProfile::FromConfig(
            context, DeltaSharingSecretRequest::FromNamedParameters(input.named_parameters));
```

In `LoadInternal`, replace:

```cpp
    TableFunction list("delta_share_list", {}, ListFunction, ListBind);
    list.varargs = LogicalType::VARCHAR;
    DUCKDB_REGISTER_FUNCTION(db, list);

    TableFunction all_tables("delta_share_list_all_tables", {LogicalType::VARCHAR}, ListFunction, AllTablesBind);
    DUCKDB_REGISTER_FUNCTION(db, all_tables);
```

with:

```cpp
    TableFunction list("delta_share_list", {}, ListFunction, ListBind);
    list.varargs = LogicalType::VARCHAR;
    DeltaSharingSecretRequest::AddNamedParameters(list);
    DUCKDB_REGISTER_FUNCTION(db, list);

    TableFunction all_tables("delta_share_list_all_tables", {LogicalType::VARCHAR}, ListFunction, AllTablesBind);
    DeltaSharingSecretRequest::AddNamedParameters(all_tables);
    DUCKDB_REGISTER_FUNCTION(db, all_tables);
```

- [ ] **Step 4: Build and run the test until it passes**

Run: `GEN=ninja make release && ./build/release/test/unittest --test-dir . "test/sql/secret_resolution.test"`
Expected: `All tests passed`.

If `CREATE SECRET plain_http (TYPE http)` fails because `http` isn't registered in the unittest build, move the wrong-type case into `test/sql/secret_resolution_http.test` headed with `require httpfs`, and note that in the commit message.

- [ ] **Step 5: Run the full suite and the static CLI**

Run: `./build/release/test/unittest --test-dir . "test/sql/*" && ./build/release/duckdb -c "select 1"`
Expected: all tests pass, and the CLI prints `1`.

- [ ] **Step 6: Commit**

```bash
git add src/duckdb_delta_sharing_extension.cpp test/sql/secret_resolution.test
git commit -m "feat: add endpoint and secret parameters to the delta_share listing functions"
```

---

### Task 3: `delta_share_read` and `delta_share_change_data_feed`, with parameter stripping

**Files:**
- Modify: `src/duckdb_delta_sharing_extension.cpp`, in `ReadDeltaShareBind` (~L317, ~L382), `ReadDeltaShareCdfBind` (~L473, ~L544) and `LoadInternal` (after `base_read.named_parameters.erase("schema");`)
- Test: `test/sql/secret_resolution.test` (append)
- Scratch (not committed): `$SCRATCH/mock_sharing_server.py`, `$SCRATCH/mock_read.sql`, where `$SCRATCH` is this session's scratchpad directory

**Interfaces:**
- Consumes (from Task 1):
  - `DeltaSharingSecretRequest::FromNamedParameters`
  - `DeltaSharingSecretRequest::WithoutRequestParameters`
  - `DeltaSharingSecretRequest::AddNamedParameters`

- [ ] **Step 1: Append the failing tests**

Append to `test/sql/secret_resolution.test`:

```
# --- delta_share_read / delta_share_change_data_feed ------------------------

statement ok
CREATE SECRET reader (TYPE delta_sharing, ENDPOINT 'https://reader.invalid/ds', BEARER_TOKEN 'r', SCOPE 'https://reader.invalid/ds');

statement ok
CREATE SECRET other (TYPE delta_sharing, ENDPOINT 'https://other.invalid/ds', BEARER_TOKEN 'o', SCOPE 'https://other.invalid/ds');

statement error
SELECT * FROM delta_share_read('s', 'sc', 't');
----
2 delta_sharing secrets are scoped and none is unscoped (other, reader)

statement error
SELECT * FROM delta_share_read('s', 'sc', 't', secret := 'reader');
----
Could not resolve host: reader.invalid

statement error
SELECT * FROM delta_share_read('s', 'sc', 't', endpoint := 'https://other.invalid/ds');
----
Could not resolve host: other.invalid

statement error
SELECT * FROM delta_share_read('s', 'sc', 't', TIMESTAMP '2026-01-01 00:00:00', secret := 'reader');
----
Could not resolve host: reader.invalid

statement error
SELECT * FROM delta_share_change_data_feed('s', 'sc', 't', 0, secret := 'reader');
----
Could not resolve host: reader.invalid

statement error
SELECT * FROM delta_share_change_data_feed('s', 'sc', 't', endpoint := 'https://other.invalid/ds');
----
Could not resolve host: other.invalid

statement ok
DROP SECRET reader;

statement ok
DROP SECRET other;
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `./build/release/test/unittest --test-dir . "test/sql/secret_resolution.test"`
Expected: FAIL. The first new case already passes, since Task 1 covers the default rule. The `secret :=` cases fail with `Invalid named parameter "secret" for function delta_share_read`.

- [ ] **Step 3: Register the parameters, pass the request, strip it before `read_parquet`**

In `LoadInternal`, directly after `base_read.named_parameters.erase("schema");`, add the line below. Every `read_*` and `cdf_*` overload copies `base_read`, so they all inherit it.

```cpp
    DeltaSharingSecretRequest::AddNamedParameters(base_read);
```

In `ReadDeltaShareBind`, replace:

```cpp
    DeltaSharingProfile profile = DeltaSharingProfile::FromConfig(context);
```

with:

```cpp
    DeltaSharingProfile profile = DeltaSharingProfile::FromConfig(
        context, DeltaSharingSecretRequest::FromNamedParameters(input.named_parameters));
```

Make the same replacement in `ReadDeltaShareCdfBind`.

In both binds, replace the inner bind input:

```cpp
    TableFunctionBindInput inner_input(inputs_list, input.named_parameters, input.input_table_types, input.input_table_names, read_parquet.function_info.get(), input.binder, read_parquet, input.ref);
```

with:

```cpp
    // read_parquet rejects options it doesn't know, so endpoint/secret stop here.
    auto parquet_named_parameters = DeltaSharingSecretRequest::WithoutRequestParameters(input.named_parameters);
    TableFunctionBindInput inner_input(inputs_list, parquet_named_parameters, input.input_table_types, input.input_table_names, read_parquet.function_info.get(), input.binder, read_parquet, input.ref);
```

- [ ] **Step 4: Build and run the tests until they pass**

Run: `GEN=ninja make release && ./build/release/test/unittest --test-dir . "test/sql/*" && ./build/release/duckdb -c "select 1"`
Expected: all tests pass, and the CLI prints `1`.

- [ ] **Step 5: Prove stripping and "requests go to the endpoint" against a local mock server (scratch)**

The offline tests stop at the HTTP call, before the inner `read_parquet` bind. So stripping, and the path half of "requests go to X", need a server.

This mock serves Delta Sharing only under `/ds/sub/`. It logs every request's path and bearer token, and serves one local parquet file.

Create `$SCRATCH/mock_sharing_server.py`:

```python
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

PARQUET, LOG = sys.argv[1], sys.argv[2]
PREFIX = "/ds/sub"
SCHEMA = json.dumps({"type": "struct", "fields": [
    {"name": "id", "type": "long", "nullable": True, "metadata": {}}]})


class Handler(BaseHTTPRequestHandler):
    def _log(self):
        with open(LOG, "a") as log:
            log.write(f"{self.command} {self.path} {self.headers.get('Authorization')}\n")

    def _send(self, status, body, content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.end_headers()
        self.wfile.write(body.encode())

    def do_GET(self):
        self._log()
        if self.path.startswith(PREFIX + "/shares"):
            return self._send(200, json.dumps({"items": [{"name": "s"}]}))
        self._send(404, "{}")

    def do_POST(self):
        self._log()
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.path == PREFIX + "/shares/s/schemas/sc/tables/t/query":
            import os
            lines = [
                {"protocol": {"minReaderVersion": 1}},
                {"metaData": {"id": "t", "format": {"provider": "parquet"},
                              "schemaString": SCHEMA, "partitionColumns": []}},
                {"file": {"url": PARQUET, "id": "f1", "partitionValues": {},
                          "size": os.path.getsize(PARQUET)}},
            ]
            return self._send(200, "\n".join(json.dumps(l) for l in lines), "application/x-ndjson")
        self._send(404, "{}")

    def log_message(self, *args):
        pass


HTTPServer(("127.0.0.1", 18765), Handler).serve_forever()
```

Create `$SCRATCH/mock_read.sql`:

```sql
CREATE SECRET session_unscoped (TYPE delta_sharing, ENDPOINT 'http://127.0.0.1:18765/ds', BEARER_TOKEN 'session-token');
CREATE SECRET store_like (TYPE delta_sharing, ENDPOINT 'http://127.0.0.1:18765/ds', BEARER_TOKEN 'store-token', SCOPE 'http://127.0.0.1:18765/ds');
SELECT count(*) AS by_endpoint FROM delta_share_read('s', 'sc', 't', endpoint := 'http://127.0.0.1:18765/ds/sub');
SELECT count(*) AS by_name FROM delta_share_read('s', 'sc', 't', secret := 'session_unscoped', endpoint := 'http://127.0.0.1:18765/ds/sub', binary_as_string := true);
SELECT count(*) AS listed FROM delta_share_list(secret := 'session_unscoped', endpoint := 'http://127.0.0.1:18765/ds/sub');
```

Run:

```bash
SCRATCH=<this session's scratchpad directory>
./build/release/duckdb -c "COPY (SELECT range AS id FROM range(3)) TO '$SCRATCH/t.parquet'"
rm -f "$SCRATCH/requests.log"
python3 "$SCRATCH/mock_sharing_server.py" "$SCRATCH/t.parquet" "$SCRATCH/requests.log" &
MOCK_PID=$!; sleep 1
./build/release/duckdb < "$SCRATCH/mock_read.sql"
kill $MOCK_PID
cat "$SCRATCH/requests.log"
```

Expected:
- `by_endpoint = 3`, `by_name = 3`, `listed = 1`.
- `binary_as_string` proves read_parquet options still pass through, while `endpoint`/`secret` don't reach it.
- The log shows only `/ds/sub/…` paths.
- The `by_endpoint` query line carries `Bearer store-token`: the scoped match beats the unscoped one.
- The `by_name` and `listed` lines carry `Bearer session-token`.

If the multi-file reader refuses a local path as a file URL, serve the parquet over the same mock instead: add a `GET /files/t.parquet` branch that returns the bytes, and set `"url"` to `http://127.0.0.1:18765/files/t.parquet`. httpfs then has to be loaded as described in [[build-verify-workflow]]. Record whichever form worked in the task report.

- [ ] **Step 6: Commit**

```bash
git add src/duckdb_delta_sharing_extension.cpp test/sql/secret_resolution.test
git commit -m "feat: add endpoint and secret parameters to delta_share_read and delta_share_change_data_feed"
```

---

### Task 4: README "Choosing a secret"

**Files:**
- Modify: `README.md`. Put the new section directly after the section that documents `CREATE SECRET (TYPE delta_sharing, …)`; find it with `grep -n "CREATE SECRET" README.md`.

- [ ] **Step 1: Add the section**

````markdown
### Choosing a secret

A session can hold several `delta_sharing` secrets. The table functions
(`delta_share_list`, `delta_share_list_all_tables`, `delta_share_read`,
`delta_share_change_data_feed`) take two optional named parameters that choose one:

| Call | Secret used | Requests go to |
|---|---|---|
| no parameter | the unscoped secret (ranked the DuckDB way: temporary before persistent, then by name); otherwise the only secret; otherwise an error | the secret's `ENDPOINT` |
| `endpoint := 'https://…'` | the secret whose `SCOPE` is the longest prefix of the endpoint (unscoped secrets match everything, at the lowest rank) | the given endpoint |
| `secret := 'name'` | that secret | the secret's `ENDPOINT` |
| both | the named secret | the given endpoint |

When `endpoint` is given, it must equal the chosen secret's `ENDPOINT` or extend
it with more path segments. Otherwise the call fails, so a secret's bearer token
is only ever sent under its own `ENDPOINT`.

```sql
CREATE SECRET sales (TYPE delta_sharing, ENDPOINT 'https://sharing.example.com/sales',
    BEARER_TOKEN '…', SCOPE 'https://sharing.example.com/sales');
CREATE SECRET ops (TYPE delta_sharing, ENDPOINT 'https://sharing.example.com/ops',
    BEARER_TOKEN '…', SCOPE 'https://sharing.example.com/ops');

SELECT * FROM delta_share_list(endpoint := 'https://sharing.example.com/ops');
SELECT * FROM delta_share_read('share', 'schema', 'table', secret := 'sales');
```

`delta_share_list_files` is a scalar function and always uses the no-parameter rule.
````

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document choosing a delta_sharing secret"
```

---

### Task 5: Local verification against a Postgres-backed store

The sqllogictests prove the rules against `memory` (10) and `local_file` (20). This task checks them against a duckdb-postgres secret storage (25 and up), the kind SensorUp's catalog store uses. Nothing is committed. The results go into the PR's test plan.

- [ ] **Step 1: Start a throwaway Postgres**

```bash
docker run -d --rm --name gro12-pg -e POSTGRES_PASSWORD=pg -p 55432:5432 postgres:17
sleep 3
```

- [ ] **Step 2: Run the storage check**

Create `$SCRATCH/pg_store.sql`:

```sql
INSTALL postgres; LOAD postgres;
ATTACH 'host=127.0.0.1 port=55432 user=postgres password=pg dbname=postgres' AS store (TYPE postgres, SECRET_STORAGE_TABLE 'public.duckdb_secrets');
CREATE PERSISTENT SECRET store_scoped IN postgres_store (TYPE delta_sharing, ENDPOINT 'https://elsewhere.invalid/s', BEARER_TOKEN 's', SCOPE 'https://shared.invalid/ds');
CREATE SECRET session_unscoped (TYPE delta_sharing, ENDPOINT 'https://session.invalid/ds', BEARER_TOKEN 'u');
SELECT name, storage, scope FROM duckdb_secrets() ORDER BY name;
-- ① default rule: the unscoped session secret wins over the store secret
SELECT * FROM delta_share_list();
```

```sql
-- in a second invocation, after the first, since the first errors out by design:
INSTALL postgres; LOAD postgres;
ATTACH 'host=127.0.0.1 port=55432 user=postgres password=pg dbname=postgres' AS store (TYPE postgres, SECRET_STORAGE_TABLE 'public.duckdb_secrets');
CREATE SECRET session_unscoped (TYPE delta_sharing, ENDPOINT 'https://session.invalid/ds', BEARER_TOKEN 'u');
-- endpoint := : the store's scoped secret beats the unscoped session secret (the hazard)
SELECT * FROM delta_share_list(endpoint := 'https://shared.invalid/ds');
-- secret := : the session secret, pinned by name (movement's safe form)
SELECT * FROM delta_share_list(secret := 'session_unscoped');
-- equal SCOPE: memory beats the store
CREATE SECRET session_scoped (TYPE delta_sharing, ENDPOINT 'https://elsewhere.invalid/m', BEARER_TOKEN 'm', SCOPE 'https://shared.invalid/ds');
SELECT * FROM delta_share_list(endpoint := 'https://shared.invalid/ds');
```

Run each file with `./build/release/duckdb -unsigned -bail -f <file>`. For the second file, run it without `-bail`, one statement per `-c`, so each expected error prints.

Expected:
- `duckdb_secrets()` lists `store_scoped` in storage `postgres_store`.
- ① → `Could not resolve host: session.invalid`.
- `endpoint :=` → `secret 'store_scoped' has ENDPOINT`.
- `secret :=` → `session.invalid`.
- Equal SCOPE → `secret 'session_scoped'`.

The build reports the tagged `v1.5.5`, so `INSTALL postgres` fetches the published core build. If that fails on a version mismatch, load a published binary directly with `SET allow_extensions_metadata_mismatch=true`, as in [[build-verify-workflow]].

- [ ] **Step 3: Tear down**

```bash
docker stop gro12-pg
```

- [ ] **Step 4: Live read (only with King's go-ahead in chat)**

Source `~/Documents/Oxy Delta Sharing/env-vars.sh`, which mints a 1-hour token into `DELTA_SHARING_ENDPOINT`/`DELTA_SHARING_BEARER_TOKEN` and touches no tenant store. Then run, with the known table from the [[oxy-delta-sharing-test-share]] memory:

```sql
CREATE SECRET live (TYPE delta_sharing, PROVIDER env);
CREATE SECRET decoy (TYPE delta_sharing, ENDPOINT 'https://decoy.invalid/ds', BEARER_TOKEN 'd', SCOPE 'https://decoy.invalid/ds');
SELECT count(*) FROM delta_share_list();                                 -- ① picks the unscoped live secret
SELECT count(*) FROM delta_share_read('<share>', '<schema>', '<table>', secret := 'live');
SELECT count(*) FROM delta_share_read('<share>', '<schema>', '<table>', endpoint := getenv('DELTA_SHARING_ENDPOINT'));
```

`getenv` is CLI-only. If it's unavailable, substitute the endpoint value from the environment in the shell. Keep the share name, endpoint and tenant details out of every PR and comment.

A check against the Postgres store on the king-test tenant (`tnt-itzdko35`) would mean writing a `delta_sharing` secret into its store. That's a separate ask to King, not part of this task.

---

### Task 6: Push, PR drafts, and Linear (gated)

- [ ] **Step 1: Split the upstream branch from the docs commits**

```bash
git checkout -b gro-12-upstream-pr upstream/main
git cherry-pick <Task 1..4 commit shas, in order>
git diff upstream/main --stat   # must list no docs/superpowers/ paths
./build/release/test/unittest --test-dir . "test/sql/*"
```

Rebuild first if cherry-picking changed anything relative to the tested tree.

- [ ] **Step 2: Push both branches to the fork (allowed)**

```bash
git push -u origin gro-12-upstream-scope-based-secret-resolution-in-duckdb-delta
git push -u origin gro-12-upstream-pr
```

- [ ] **Step 3: Draft the upstream PR text and show King, but don't open it**

- Outside-contributor voice (no "we/our" about prequel's code or CI), plain and brief.
- Sections: problem (last-wins, arbitrary across storages), the table of rules, the behaviour change (only the multi-secret cases), tests, and a test plan with checkboxes.
- End with the "Generated with Claude Code" line.
- No SensorUp, tenant or share details, and no forge-mvp line (that goes in the Linear record).

- [ ] **Step 4: Linear**

- Attach the fork branch URL to GRO-12.
- Draft the bounded-HTTP issue for King: timeout honouring `http_timeout`, plus a pagination cap / repeated-`nextPageToken` guard, citing movement's evidence (68,214 requests in 12 s, a 150 s stall).
- Flag, don't fix: `QueryTable` writes every request body to `/tmp/duckdb_delta_sharing_request.json` (`src/delta_sharing_client.cpp`, the `debug_file` block).
- Create neither issue until King approves.

- [ ] **Step 5: After King approves the text**

- Open the upstream PR from `kinghuang:gro-12-upstream-pr` against `prequel-co/duckdb-delta-sharing:main`.
- Attach it to GRO-12.
- Move GRO-12 to In Review.
- The community-extensions release ask and movement's `secret :=` PR each wait for their own OK.
