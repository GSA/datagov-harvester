# Validator security

`datagov-harvest-validator` (`validator_api/` + `dcatus_validation/`) is a public, unauthenticated API that fetches attacker-chosen URLs from inside cloud.gov and parses attacker-supplied JSON. This page lists the protections that make that safe, where each one lives, and the rules to keep when changing the code. For deployment and routing, see "datagov-harvest-validator" in [developer.md](developer.md).

## Threat model

Anyone on the internet can call `POST /api/v1/validate`, either with a URL for the validator to fetch or with pasted JSON in `json_text`, and `POST /api/v1/validate/document`, whose request body is the catalog itself (how the `/validate/` page sends pasted text and uploaded files; `schema` and `fetch_method=paste|upload` are query parameters). The protections below are aimed at four threats:

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

1. **Scheme allowlist.** Only `http` and `https` are accepted.
2. **No embedded credentials.** URLs with `user:pass@` are refused, and the credentials are never logged.
3. **Known unwanted domains are blocked before DNS.** `nip.io`, `rbndr.us`, `sslip.io`, and `traefik.me`, including their subdomains, are refused. The match uses the IDNA hostname the resolver would use, accepts a trailing DNS dot, and requires a domain boundary so similar looking publisher domains remain eligible. The list is intentionally narrow; add domains when the network team confirms they cause unwanted lookup or egress traffic (GSA/data.gov#6419).
4. **Every resolved address must be public.** The hostname is resolved, and if *any* address is non-global, the request is refused. The non-global check covers loopback, RFC 1918, link-local and cloud metadata (`169.254.0.0/16`), CGNAT (`100.64.0.0/10`), multicast, reserved, and documentation ranges. It also unwraps IPv4 hidden inside IPv6 (IPv4-mapped, NAT64, 6to4, Teredo, IPv4-compatible), which Python's `is_global` alone gets wrong.
5. **The egress proxy enforces it again.** On cloud.gov every fetch goes through the validator's egress proxy (below), which resolves the hostname itself and allows only HTTPS, and its egress space's security groups don't reach private networks. That is what stops a DNS-rebinding answer, one that changes between the check above and the connect, from reaching an internal address.
6. **Redirects re-checked by hand.** `requests` would follow redirects without re-checking them, so redirects are disabled and each `Location` goes back through steps 1–5. Chains are capped at 5 hops.
7. **Proxy from the environment, like the harvester.** Fetches use plain `requests`, which reads `http_proxy`/`https_proxy`; `.profile` sets them from `proxy_url`. If `proxy_url` is ever missing, fetches go direct and only the checks above apply. A DNS-rebinding hostname could then reach what the app space's `trusted_local_networks_egress` allows: HTTPS to some brokered-service ranges (`10.10.x`). Keep the proxy enabled in every space.
8. **Fails closed.** Private addresses are allowed only when `ALLOW_PRIVATE_ADDRESSES=true`. Only local `docker compose` sets it, so it can fetch the sample catalogs. Never set it on cloud.gov.

## Egress proxy

On cloud.gov the validator's fetches go through its own [cg-egress-proxy](https://github.com/GSA-TTS/cg-egress-proxy) in the `<space>-egress` space. It allows every domain but only port 443, and the egress space's security groups don't allow private networks, so it can't reach internal addresses. Its lists are in GSA/data.gov (`egress/acl/datagov-harvest-validator.<space>.*.acl`), and it's deployed by that repo's "enable egress proxy" workflow; see "Validator egress proxy" in [developer.md](developer.md).

Tests: `tests/unit/dcatus_validation/test_fetch.py`, especially `TestPrivateAddressDefault`, `TestEgressProxy`, and `test_fetch_json_from_url_rejects_redirect_to_private_address_in_prod`.

## Resource limits

All limits are in `dcatus_validation/limits.py` unless noted.

| Limit | Value | Enforced by |
| --- | --- | --- |
| Catalog document size | 150MB (`MAX_UPLOAD_BYTES`) | Fetch: the `Content-Length` header, then a running byte count while streaming, so an untruthful header can't get past it. Paste: the route. |
| Request body | 225MB + 1KB (`MAX_REQUEST_BYTES`) | Flask `MAX_CONTENT_LENGTH`, for both endpoints. It has room for a JSON-escaped 150MB paste in `json_text`: escaping adds 1–23% to real catalogs, and 1.5× covers that. A document made mostly of quotes or backslashes (up to 2×) gets the validator's JSON 413. `/validate/document` bodies aren't escaped, so a body over 150MB there is a document over the limit (400), and only one over 225MB gets the 413. |
| Proxy body | 226M | `client_max_body_size` in `proxy/nginx-common.conf`. It sits just above the app limit so the app's JSON 413 is what callers see. `/validate/document` alone would need only ~151M, but both endpoints share the cap while `/validate` still takes pasted catalogs inside JSON. |
| Fetch time | 12s total, including DNS and every redirect hop | `FETCH_TIMEOUT_SECONDS` in `fetch.py`. Each connect attempt is capped at 4s, with up to 3 attempts. The deadline is checked between body reads; a single stalled read can overrun it by up to one read timeout. |
| JSON nesting | 100 levels (`MAX_DOCUMENT_NESTING_DEPTH`) | A walk after parsing (`validate._check_catalog_nesting`) refuses anything deeper before validation starts. On Python 3.14 `json.loads` parses deep documents without exhausting the stack (about 47,000 levels in a gunicorn worker thread on Linux) and raises `RecursionError` beyond that, which is refused the same way. |
| JSON numbers | finite only | `NaN`/`Infinity` and integers too large to parse are refused. |
| Large documents at once | 1 per instance over 50MB (`LARGE_DOCUMENT_BYTES`) | A file lock in `validator_api/api.py`, shared by the instance's gunicorn workers, held while the document is parsed and validated. Another large one gets a JSON 503 with `Retry-After: 60`. |
| Validation errors returned | 1000, with `validation_incomplete: true` | `MAX_VALIDATION_ERRORS`. This stops a small document from expanding into millions of errors. |
| Result text | identifiers 1000 chars, messages 4000 | Truncated in `validator_api/api.py`. |

The document limit was raised from 10MB to 150MB in GSA/data.gov#6460. That covers nearly every agency catalog without sizing the service for the largest outlier, a ~600MB Census file. What it costs:

- **Memory.** On Linux, a worker validating a 150MB catalog peaks near 1GB, most of it the parsed catalog as Python objects (measured at 100MB, 616MB for ASCII text and 721MB otherwise, and extrapolated to 150MB). A worker drops back to ~200MB afterwards. So the 2G `validator_memory_quota` (3 workers per instance) fits one 150MB submission at a time, ~1.4GB in total, but not two, which is why documents over 50MB are validated one at a time per instance (see the table). Cloud Foundry restarts an instance that goes over its quota.
- **Time.** Parsing and validating a 150MB catalog takes ~2s (see "Fast path" below), well inside CloudFront's 30s limit, which doesn't count the upload. A URL submission still has to download within the 12s fetch budget, which for 150MB takes a server sending ~100Mbps.

Changing `MAX_UPLOAD_MB` means rechecking both of these, and keeping `client_max_body_size` in `proxy/nginx-common.conf` above `MAX_REQUEST_BYTES`.

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

## Fast path

Validation is ~20 times faster than Python jsonschema alone, with the same results (GSA/data.gov#6460):

- **Validation.** jsonschema-rs only decides which records (1.1) or top-level items (3.0) are valid. Python jsonschema validates everything else, so every error message, its path and order, and the 1,000-error cap still come from Python jsonschema. Keywords the two libraries treat differently (`pattern`, `format`, `uniqueItems`, `enum`, `const`) call back into Python jsonschema, the rest are a reviewed list (`fast_path.RUST_KEYWORDS`), and a schema using any other keyword gets no fast path.
- **Tests.** `tests/unit/dcatus_validation/test_equivalence.py` and `tests/integration/validator_api/test_validate_api.py` run every example catalog, thousands of mutations and a Hypothesis fuzzer through both the fast path and Python jsonschema alone, and require identical results.

## Rules for changes

- Any new outbound request must go through `fetch_json_from_url`, or reuse its target validation and `_http_get`. Never call `requests` directly on user input.
- Never put exception text, fetched content, or raw URLs into a response or a log line.
- Any new route must be added to `proxy/nginx-common.conf` (and `nginx-local-proxy.conf`) explicitly. The proxy forwards nothing to the validator by default.
- Keep `validator_api` and `dcatus_validation` free of `app`, `harvester`, and `database` imports. The import guard enforces this, so don't weaken it.
- Add a test with each new limit or check, next to the existing ones in `tests/unit/dcatus_validation/` and `tests/integration/validator_api/`.
- Never take a message, path or count from jsonschema-rs. It only answers valid or invalid, and Python jsonschema reports every error. Adding a keyword to `fast_path.RUST_KEYWORDS` needs the same review and differential tests as the existing ones.
