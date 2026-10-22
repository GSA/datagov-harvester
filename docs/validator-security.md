# Validator security

`datagov-harvest-validator` (`validator_api/` + `dcatus_validation/`) is a public, unauthenticated API that fetches attacker-chosen URLs from inside cloud.gov and parses attacker-supplied JSON. This page lists the protections that make that safe, where each one lives, and the rules to keep when changing the code. For deployment and routing, see "datagov-harvest-validator" in [developer.md](developer.md).

## Threat model

Anyone on the internet can call `POST /api/v1/validate`, either with a URL for the validator to fetch or with pasted JSON. The protections below are aimed at four threats:

- **Server-side request forgery (SSRF):** using the validator's URL fetch to reach cloud.gov internal routes (`*.apps.internal`), cloud metadata, or other private services, and reading their responses back through validation errors.
- **Resource exhaustion:** large, slow, deeply nested, or error-heavy submissions that tie up the few worker slots (3 workers × 2 threads per instance) or exhaust memory.
- **Information disclosure:** stack traces, exception text, or fetched content leaking into responses or logs.
- **Lateral movement:** a compromise of the validator reaching harvester data, secrets, or Cloud Foundry.

## Isolation

- **Separate Cloud Foundry app.** Validation runs in `datagov-harvest-validator`, not in the admin app, so its load and any compromise stay in their own failure domain (GSA/data.gov#6293).
- **No database, secrets, or CF credentials.** The app binds only `datagov-harvest-validator-secrets`, which holds just the New Relic license key. It never binds `datagov-harvest-secrets` or the database.
- **Import guard.** `tests/unit/test_validator_import_clean.py` fails if `validator_api` or `dcatus_validation` imports `app`, `harvester`, or `database`. It checks this both by running a real validation in a clean subprocess and with a static AST scan, so a lazy import inside a handler is caught too.
- **Internal route only.** The app's only route is `*.apps.internal`. Public traffic reaches it only through `datagov-harvest-proxy`, via the network policy added in `.github/workflows/release-space.yml`, and only on the paths in `proxy/nginx-common.conf`.

## SSRF protections (`dcatus_validation/fetch.py`)

Every URL fetch, including every redirect hop, goes through these checks:

1. **Scheme and port allowlist.** Only `http` and `https` are accepted, and only on ports 80 and 443 (`ALLOWED_FETCH_PORTS`). Any other port would let callers use the validator to probe services on public hosts, since its different refusals tell open, closed and filtered ports apart.
2. **No embedded credentials.** URLs with `user:pass@` are refused, and the credentials are never logged.
3. **Every resolved address must be public.** The hostname is resolved, and if *any* address is non-global, the request is refused. The non-global check covers loopback, RFC 1918, link-local and cloud metadata (`169.254.0.0/16`), CGNAT (`100.64.0.0/10`), multicast, reserved, and documentation ranges. It also unwraps IPv4 hidden inside IPv6 (IPv4-mapped, NAT64, 6to4, Teredo, IPv4-compatible), which Python's `is_global` alone gets wrong.
4. **DNS pinning.** The connection goes to the exact IP that passed the check (`_PinnedAddressAdapter`). The hostname is kept only for the `Host` header and TLS SNI/verification, so a DNS-rebinding answer between the check and the connect can't redirect the request.
5. **Redirects re-checked by hand.** `requests` would follow redirects without re-checking them, so redirects are disabled and each `Location` goes back through steps 1–4. Chains are capped at 5 hops.
6. **No ambient proxies or `.netrc`.** Each fetch uses a fresh `Session` with `trust_env = False`, so environment proxy settings can't take over hostname resolution after the check.
7. **Fails closed.** Private addresses are allowed only when `ALLOW_PRIVATE_ADDRESSES=true`. Only local `docker compose` sets it, so it can fetch the sample catalogs. Never set it on cloud.gov.

Tests: `tests/unit/dcatus_validation/test_fetch.py`, especially `TestPrivateAddressDefault`, `test_nonstandard_ports_are_refused`, `test_hostname_resolution_is_pinned_for_the_connection`, `test_fetch_json_from_url_rejects_redirect_to_private_address_in_prod`, and `test_pinned_get_ignores_ambient_credentials_and_proxies`.

## Resource limits

All limits are in `dcatus_validation/limits.py` unless noted.

| Limit | Value | Enforced by |
| --- | --- | --- |
| Catalog document size | 10MB (`MAX_UPLOAD_BYTES`) | Fetch: the `Content-Length` header, then a running byte count while streaming, so an untruthful header can't get past it. Paste: the route. |
| Request body | 20MB + 1KB (`MAX_REQUEST_BYTES`) | Flask `MAX_CONTENT_LENGTH`. It has room for a JSON-escaped 10MB paste. |
| Proxy body | 21M | `client_max_body_size` in `proxy/nginx-common.conf`. It sits just above the app limit so the app's JSON 413 is what callers see. |
| Fetch time | 12s total, including DNS and every redirect hop | `FETCH_TIMEOUT_SECONDS` in `fetch.py`. Each connect attempt is capped at 4s, with up to 3 attempts. The socket timeout shrinks before every body read so a late stall can't extend it. |
| JSON nesting | 100 levels | Checked by a character scan *before* `json.loads`, so a deep document can't exhaust the stack. |
| JSON numbers | finite only | `NaN`/`Infinity` and integers too large to parse are refused. |
| Validation errors returned | 1000, with `validation_incomplete: true` | `MAX_VALIDATION_ERRORS`. This stops a small document from expanding into millions of errors. |
| Result text | identifiers 1000 chars, messages 4000 | Truncated in `validator_api/api.py`. |

Larger documents are under discussion in GSA/data.gov#6460. Raising `MAX_UPLOAD_MB` also has to keep the fetch and validation time inside CloudFront's 30s request ceiling, and keep each worker's memory within `validator_memory_quota`.

## Information disclosure

- **Only safe literals reach the submitter.** `InvalidCatalogSource.public_message` is built only from literals and numbers. Never interpolate another exception's text into it: `json.JSONDecodeError.doc`, for example, holds the whole fetched document, which the submitter may not otherwise be able to read. JSON parse errors report only line and column (`invalid_json_message`).
- **Unexpected errors are generic.** Anything unanticipated returns a fixed 500 message, and the log records only the exception *type*, because `requests` exception text can include signed URLs or credentials.
- **Logged URLs are redacted.** Fetch targets are logged as scheme, host, and port plus a 12-character SHA-256 hash for correlation (`_target_for_log`). Paths, query strings, and credentials are never logged.

## HTTP hardening

- **Headers.** Talisman sets HSTS and a CSP scoped to what the Swagger docs need (`validator_api/__init__.py`, with baseline options shared with the admin app in `config/security.py`).
- **No caching.** POSTs and error responses get `Cache-Control: private, no-store`, so one submitter's results are never cached for another.
- **Safe redirect.** The unversioned `/api/...` redirect builds a relative `/api/v1/...` target and refuses anything that parses with a scheme or host. CodeQL alert 46 on it is a false positive (see the PR #984 discussion).
- **Backend identification.** `X-Served-By` lets synthetic monitoring confirm the proxy actually reached this app.

## Health check

- `GET /health` on the internal route is Cloud Foundry's http health check (`manifest.yml`).
- `GET /validator/health` is the same check through the proxy's public route, for uptime monitoring.
- Both return `{"status": "ok"}`. The app builds every schema validator at startup (`load_schema_validators`), so an instance whose schemas are missing or broken (for example, the `_external/dcat-us` submodule wasn't checked out) fails to start instead of passing the health check and then failing requests.

## Rules for changes

- Any new outbound request must go through `fetch_json_from_url`, or reuse its target validation and `_pinned_get`. Never call `requests` directly on user input.
- Never put exception text, fetched content, or raw URLs into a response or a log line.
- Any new route must be added to `proxy/nginx-common.conf` (and `nginx-local-proxy.conf`) explicitly. The proxy forwards nothing to the validator by default.
- Keep `validator_api` and `dcatus_validation` free of `app`, `harvester`, and `database` imports. The import guard enforces this, so don't weaken it.
- Add a test with each new limit or check, next to the existing ones in `tests/unit/dcatus_validation/` and `tests/integration/validator_api/`.
