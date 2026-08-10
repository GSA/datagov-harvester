# search

Vendored verbatim from [GSA/datagov_data_access](https://github.com/GSA/datagov_data_access) at tag `1.1.0`
(commit `36d1440`), as part of reintegrating harvester's DB and OpenSearch code
(GSA/data.gov#6209). Only import paths were rewritten (`datagov_data_access.search.*` -> `search.*`).

One behavioral change has been made since vendoring: `OpenSearchClient.from_environment()` and
`__init__()` take `ensure_index=True`, so a caller can connect without the constructor creating the
`datasets` index. `flask search rebuild-index` needs that — it creates the index itself with a longer
`request_timeout` and idempotent retry handling, and a create-on-connect would race it. This matches
`for_host()`-era upstream (GSA/datagov_data_access#11, 1.3.0); a future re-vendor from that tag or
later already includes it.

The `reader.py` and `queries/` (including `queries/filters/`) modules are catalog-facing search/filter/
aggregation logic that harvester does not exercise directly — harvester only writes to OpenSearch
(`client.py`, `writer.py`, `documents.py`, `transforms.py`, `spatial.py`) plus one reader method
(`OpenSearchReader.scan_index`, used by `flask search compare`). They were vendored as-is rather than trimmed
because `reader.py` imports the whole `queries/` package at module level, and slimming would mean editing
vendored code and permanently diverging from the library, which `datagov-catalog` still depends on. Slimming
this down to only what harvester needs is the scope of GSA/data.gov#6211.
