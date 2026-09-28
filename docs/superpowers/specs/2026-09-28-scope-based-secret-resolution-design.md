# Scope-based secret resolution for `delta_share_*` (GRO-12)

Status: approved design (King, 2026-09-28). Linear: GRO-12 under GRO-11.
This spec is on the fork branch only. It stays out of the upstream PR, which
carries only code, tests and the README.

## Problem

`DeltaSharingProfile::FromConfig` (`src/delta_sharing_client.cpp`) loops over
`SecretManager::AllSecrets()` and keeps the last `delta_sharing` secret it sees.
That loop has three problems:

- It never looks at SCOPE and never selects by name.
- `AllSecrets()` walks an unordered map of storages, so across storages "last"
  is effectively arbitrary.
- The endpoint comes from the secret, so there is nothing to scope-match against.

A session holding secrets for two sharing servers can't choose between them.

## DuckDB facts this design rests on (v1.5.5 source + duckdb-postgres)

- `BaseSecret::MatchScore(path)` works like this:
  - A secret with no SCOPE, or with an empty-string SCOPE, scores `0` against
    every path.
  - A prefix that matches scores `len(prefix)`.
  - A secret with no matching prefix doesn't match at all.
- `SecretStorage::SelectBestMatch` and `SecretManager::LookupSecret` rank a
  match as `100 × score − storage tie_break_offset`, and a tie within one
  storage goes to the lower name. The offsets:
  - `memory`/temporary → 10
  - `local_file` → 20
  - duckdb-postgres `PostgresSecretStorage` → 25 and up, one per attached store
  - DuckDB 2.0's `connection` storage (duckdb#23527) → 5
- `tie_break_offset` is `protected`, so an extension can't rebuild that ranking
  itself. **`LookupSecret` is the only ranking authority we use.**
- `SecretManager::GetSecretByName(txn, name)` searches every storage and raises
  `Ambiguity detected` when a name appears in more than one.

Consequences:
- At equal SCOPE, a session secret beats a store secret.
- Any SCOPE that prefix-matches beats an unscoped secret, whatever storage each
  is in.

## Rules

Named parameters, both `VARCHAR` and both optional, on `delta_share_list`,
`delta_share_list_all_tables`, every `delta_share_read` overload and every
`delta_share_change_data_feed` overload:

- `endpoint` — the sharing server URL to call, which also selects a secret by SCOPE
- `secret` — select a secret by name

| Call | Secret chosen | Requests go to |
|---|---|---|
| neither | ① `LookupSecret('', 'delta_sharing')`, which only unscoped secrets can match, ranked by DuckDB · ② else, if exactly one `delta_sharing` secret exists, that one · ③ else error | the secret's `ENDPOINT` |
| `endpoint := X` | `LookupSecret(X, 'delta_sharing')`: the longest SCOPE prefix wins, and an unscoped secret matches at 0 | `X` |
| `secret := N` | `GetSecretByName(N)`, which must exist and be of type `delta_sharing` | the secret's `ENDPOINT` |
| both | by name | `X` |

Selecting by name ignores SCOPE, because naming the secret is the explicit
choice. The endpoint guard below still applies.

**Endpoint guard.** Whenever `X` is given, it must equal the chosen secret's
`ENDPOINT` or extend it at a `/` boundary, after stripping trailing slashes from
both. Otherwise the call errors and names both URLs. It never prints the token.

- Why: an unscoped secret matches every `X`, so without the guard, an
  `endpoint :=` call to server B would send server A's bearer to B. With the
  guard, a bearer only ever goes to a URL under its own secret's `ENDPOINT`.
  The guard also fits the catalog rule that an endpoint URL prefix-extends
  its credential URL.

**Precedence, stated plainly.**
- With no parameter, an unscoped secret is preferred over scoped ones.
- With `endpoint :=`, any matching SCOPE beats an unscoped secret.
- Two unscoped secrets for different servers can't be told apart by `endpoint :=`:
  DuckDB's tie-break picks one and the guard turns a wrong pick into an error
  telling the user to add SCOPE or pass `secret :=`.

**`delta_share_list_files`** is a scalar function, and DuckDB has no named
parameters for those, so it uses rule ①–③.

**Rejected alternatives.**
- Keeping last-wins: it's nondeterministic across storages.
- Erroring on any ambiguity: that breaks sessions holding one session secret
  alongside scoped store secrets.
- Defaulting SCOPE to ENDPOINT at CREATE, the httpfs `s3://` precedent:
  - rows already in a store stay unscoped;
  - a trailing-slash mismatch flips precedence;
  - the guard already gives the safety.

## Behaviour change for existing callers

- **One `delta_sharing` secret:** unchanged.
- **Several secrets:**
  - Unscoped ones are picked deterministically, where today's pick is arbitrary.
  - A session holding only scoped secrets and calling without a parameter now
    gets error ③ where it used to get an arbitrary pick.
- **Secret format and CREATE:** unchanged.

## Structure

- New `src/delta_sharing_secret_resolution.{hpp,cpp}`:
  - `DeltaSharingSecretRequest { endpoint, secret }`
  - `ResolveDeltaSharingSecret(context, request)` returns the chosen secret plus
    the effective endpoint
  - the pure `EndpointCovers(secret_endpoint, requested)`
- `DeltaSharingProfile::FromConfig(context, request)` calls the resolver.
  `FromConfig(context)` becomes `FromConfig(context, {})`.
- The read and CDF binds strip `endpoint`/`secret` before forwarding
  `named_parameters` to the inner `read_parquet` bind.
- README: a "Choosing a secret" section.

## Errors

Every failure is raised at bind time, before any HTTP call, as
`InvalidConfigurationException`:

- no secret (the existing message)
- ③ several scoped secrets and none unscoped
- `secret :=` missing, or of another type
- `endpoint :=` has no match
- the guard fails

## Tests

TDD, in `test/sql/secret_resolution.test`.

Which secret a call chose is observable offline: give each secret a distinct
`.invalid` host (RFC 6761), and the curl error names the host that was
contacted.

- **Default rule:**
  - one unscoped secret
  - unscoped beats scoped
  - memory beats `local_file` at equal SCOPE, with a `PERSISTENT` secret in a
    per-test `secret_directory` standing in for session vs store
  - ③
  - a single scoped secret
- **`endpoint :=`:**
  - longest prefix wins
  - scoped beats unscoped (pinned)
  - a guard failure
  - the effective URL is `X`
- **`secret :=`:**
  - found
  - missing
  - wrong type
  - with `endpoint :=` and the guard
- **Stripping:** `delta_share_read(…, endpoint := …)` binds past parquet.

Beyond the sqllogictest suite:
- The existing suite stays green.
- The static CLI `duckdb -c "select 1"` works.
- A local Postgres secret-storage check.
- One live read on the king-test tenant (`tnt-itzdko35`) only.

## Downstream adoption (SensorUp)

- **movement-integration** is the only runtime that calls `delta_share_*`. It's
  on DuckDB 1.5.5, and its image runs `INSTALL … FROM community` unpinned.
  - It creates an unscoped memory secret, `source_delta_sharing`, and calls
    without parameters, so rule ① picks it. **No change is required.**
  - A follow-up PR, made after the community release and approved, pins
    `secret := 'source_delta_sharing'`.
  - Don't add SCOPE to that secret while calling without a parameter: ①
    becomes ③ once the store holds a scoped secret.
  - Don't call with `endpoint :=` alone while the ARN path coexists: a store
    secret scoped to that endpoint would win.
- **catalog-utilities `credential-extensions` layer** (DuckDB 1.5.4, sha256
  `e100d271…`) only runs `CREATE SECRET`. **No pin bump is needed.**
- **forge-mvp:** no impact. Its Delta Sharing transport is plain HTTP and uses
  no extension.
- **GRO-18:** can call `secret := <credential.usable_secret_name>`, which is
  deterministic alongside the ARN session secret.
- **GRO-102:** once the ARN path is gone, `endpoint :=` alone is safe.

## Out of scope

- Bounded HTTP (a timeout and a pagination cap). That gets a separate Linear
  issue and a separate upstream PR.
- A vendored build, which is the fallback only if upstream stalls and would get
  its own ticket.
