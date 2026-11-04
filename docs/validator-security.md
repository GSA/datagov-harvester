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
5. **DNS pinning.** The connection goes to the exact IP that passed the check (`_PinnedAddressAdapter`). The hostname is kept only for the `Host` header and TLS SNI/verification, so a DNS-rebinding answer between the check and the connect can't redirect the request.
6. **Redirects re-checked by hand.** `requests` would follow redirects without re-checking them, so redirects are disabled and each `Location` goes back through steps 1–5. Chains are capped at 5 hops.
7. **No ambient proxies or `.netrc`.** Each fetch uses a fresh `Session` with `trust_env = False`, so environment proxy settings can't take over hostname resolution after the check.
8. **Fails closed.** Private addresses are allowed only when `ALLOW_PRIVATE_ADDRESSES=true`. Only local `docker compose` sets it, so it can fetch the sample catalogs. Never set it on cloud.gov.

Tests: `tests/unit/dcatus_validation/test_fetch.py`, especially `TestPrivateAddressDefault`, `test_hostname_resolution_is_pinned_for_the_connection`, `test_fetch_json_from_url_rejects_redirect_to_private_address_in_prod`, and `test_pinned_get_ignores_ambient_credentials_and_proxies`.

## Resource limits

All limits are in `dcatus_validation/limits.py` unless noted.

| Limit | Value | Enforced by |
| --- | --- | --- |
| Catalog document size | 150MB (`MAX_UPLOAD_BYTES`) | Fetch: the `Content-Length` header, then a running byte count while streaming, so an untruthful header can't get past it. Paste: the route. |
| Request body | 225MB + 1KB (`MAX_REQUEST_BYTES`) | Flask `MAX_CONTENT_LENGTH`, for both endpoints. It has room for a JSON-escaped 150MB paste in `json_text`: escaping adds 1–23% to real catalogs, and 1.5× covers that. A document made mostly of quotes or backslashes (up to 2×) gets the validator's JSON 413. `/validate/document` bodies aren't escaped, so a body over 150MB there is a document over the limit (400), and only one over 225MB gets the 413. |
| Proxy body | 226M | `client_max_body_size` in `proxy/nginx-common.conf`. It sits just above the app limit so the app's JSON 413 is what callers see. `/validate/document` alone would need only ~151M, but both endpoints share the cap while `/validate` still takes pasted catalogs inside JSON. |
| Fetch time | 12s total, including DNS and every redirect hop | `FETCH_TIMEOUT_SECONDS` in `fetch.py`. Each connect attempt is capped at 4s, with up to 3 attempts. The socket timeout shrinks before every body read so a late stall can't extend it. |
| JSON nesting | 100 levels (`MAX_DOCUMENT_NESTING_DEPTH`) | orjson parses up to its own limit of 1024 levels without recursing, then a walk after parsing (`validate._check_catalog_nesting`) refuses anything deeper than 100 before validation starts. Anything orjson refuses, including a deeper document, goes to the pure-Python parser, whose character scan refuses it *before* `json.loads`, so a deep document can't exhaust the stack either way. |
| JSON numbers | finite only | `NaN`/`Infinity` and integers too large to parse are refused. |
| Validation errors returned | 1000, with `validation_incomplete: true` | `MAX_VALIDATION_ERRORS`. This stops a small document from expanding into millions of errors. |
| Result text | identifiers 1000 chars, messages 4000 | Truncated in `validator_api/api.py`. |

The document limit was raised from 10MB to 150MB in GSA/data.gov#6460. That covers nearly every agency catalog without sizing the service for the largest outlier, a ~600MB Census file. What it costs:

- **Memory.** Peak RSS of one gunicorn worker (gthread, 2 threads) validating a pasted catalog, on Linux as cloud.gov runs it (Python 3.14 in a Debian container). The 50MB and 100MB peaks were measured; the 150MB column, which follows from them because the peak grows linearly with size, is extrapolated:

  | Part of the peak, 150MB catalog | Before the paste route dropped its extra copies | Now |
  | --- | --- | --- |
  | Idle worker | ~130MB | ~130MB |
  | Raw request body, cached by werkzeug | ~165MB | released before parsing |
  | Pasted text, decoded from the body | ~150MB (~300MB once any character is past U+00FF) | released before parsing |
  | UTF-8 copy that orjson parses | ~150MB, text not pure ASCII only | ~150MB, the one copy kept |
  | Parsed catalog as Python objects | ~690MB | ~690MB |
  | **Peak, 150MB (extrapolated)** | **~1.13GB; ~1.5–1.6GB not pure ASCII** | **~0.97GB either way** |
  | Peak, 100MB (measured) | 800MB; 1,005MB not pure ASCII | 690MB; 695MB not pure ASCII |
  | Peak, 50MB (measured) | 466MB | 411MB |

  The route keeps a single copy of a pasted catalog, its UTF-8 bytes, while it parses it (`_take_pasted_document` in `validator_api/api.py`). `/validate/document`, which the page uses, reads the body once without caching it and parses that, so it holds one copy too, and the browser sends the file without reading it into memory. `tests/integration/validator_api/test_paste_memory.py` checks both. A URL fetch already held one copy, so it peaks about the same. Most of the peak is the parsed catalog itself: each ~1.1KB record becomes ~5KB of Python strings and dicts, the cost of validating with Python jsonschema at all. After a large request a worker drops back to ~170–210MB. (macOS keeps several hundred MB more, so measurements on a Mac overstate this.)

  For an instance (3 workers and a ~30MB master) under `validator_memory_quota`, 2G in every space, extrapolated from the above:
  - **One 150MB submission at a time fits:** ~1.4GB in total.
  - **Two at once don't fit.** In one worker, both catalogs can be alive together (~1.8GB in that worker, ~2.25GB for the instance). How much they overlap depends on timing, because the threads share one GIL; two 50MB pastes measured 458MB against 411MB for one. In two different workers, it's ~2.2GB.

  Cloud Foundry restarts an instance that goes over its quota.
- **Time.** On the fast path (see "Fast path and its kill switch" below), parsing and validating a 150MB catalog takes ~2s on a fast laptop (1.7–2.1s), and ~2–2.5s through the API. That is well inside CloudFront's 30s request ceiling, with room for slower production CPUs. A URL submission still has to download within the 12s fetch budget, which for 150MB takes a server sending ~100Mbps. With the kill switch on, validation is back to 0.2–0.5 s/MB on a fast laptop (31–79s for 150MB; this repo's earlier production estimate was 0.5–1.2 s/MB). Catalogs past roughly 25–100MB can then run past the 30s ceiling, and the submitter sees a timeout even though the worker finishes the validation.

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

## Fast path and its kill switch

Validation takes two paths to the same answer (GSA/data.gov#6460):

- **Parsing.** orjson parses the document. Whenever orjson refuses one (a byte order mark, UTF-16 or UTF-32, an escaped lone surrogate, NaN, a syntax error, nesting past 1024) or returns something `json.loads` wouldn't (an integer past 64 bits, which orjson turns into a float), the pure-Python parser parses it again. So every document parses to the same value, or fails with the same error and position, as before.
- **Validation.** jsonschema-rs decides which records (1.1) or datasets and other top-level items (3.0) are valid. Python jsonschema then runs exactly as before on everything else. For 3.0 that means the same whole-catalog run, skipping only the `$ref`s into items jsonschema-rs found valid. Every error message, its path and order, and the 1,000-error cap still come from Python jsonschema alone.
- **Agreement by construction.** jsonschema-rs is only ever trusted to say "valid". Where the libraries differ, it calls back into Python jsonschema's own code: `pattern` (ECMA-262 against Python `re`), `format` (rfc3987 and the date checkers), and `uniqueItems`, `enum` and `const` (number equality). The remaining keywords are a reviewed list (`fast_path.RUST_KEYWORDS`). A schema that uses any other validation keyword gets no fast path, and `tests/unit/dcatus_validation/test_fast_path.py` fails. A document containing anything a JSON parser can't produce (a tuple, say, from a direct caller) also gets no fast path.
- **Tests.** `tests/unit/dcatus_validation/test_equivalence_*.py` and `tests/integration/validator_api/test_fast_path_api.py` run every example catalog, every GSA/dcat-us example, thousands of systematic mutations and a Hypothesis fuzzer through both paths and require identical results. Set `DCATUS_EQUIVALENCE_EXAMPLES` (e.g. `20000`) for a longer Hypothesis run. `scripts/benchmark_validation.py` measures both paths on large generated catalogs.

**Memory.** The fast path is about 20 times faster but peaks higher while parsing. orjson needs the document as UTF-8 and builds its own parse tree before creating Python objects. For a 150MB catalog that adds roughly 100–170MB at the parse peak (measured by URL with `scripts/benchmark_validation.py --api`). Pasted text no longer needs a UTF-8 copy of its own: the route parses the UTF-8 bytes it already made. The resulting gunicorn worker peaks, and what they mean for `validator_memory_quota`, are under "Memory" in [Resource limits](#resource-limits).

**Kill switch.** Set `DCATUS_VALIDATION_FAST_PATH=false` and restart (`cf set-env datagov-harvest-validator DCATUS_VALIDATION_FAST_PATH false`, then `cf restart datagov-harvest-validator`) to go back to the pure-Python parser and validator, with no deploy. Only `true` (the default when unset) keeps the fast path on, so a mistyped value also turns it off.

## Rules for changes

- Any new outbound request must go through `fetch_json_from_url`, or reuse its target validation and `_pinned_get`. Never call `requests` directly on user input.
- Never put exception text, fetched content, or raw URLs into a response or a log line.
- Any new route must be added to `proxy/nginx-common.conf` (and `nginx-local-proxy.conf`) explicitly. The proxy forwards nothing to the validator by default.
- Keep `validator_api` and `dcatus_validation` free of `app`, `harvester`, and `database` imports. The import guard enforces this, so don't weaken it.
- Add a test with each new limit or check, next to the existing ones in `tests/unit/dcatus_validation/` and `tests/integration/validator_api/`.
- Never take a message, path or count from jsonschema-rs. It only answers valid or invalid, and Python jsonschema reports every error. Adding a keyword to `fast_path.RUST_KEYWORDS` needs the same review and differential tests as the existing ones.
