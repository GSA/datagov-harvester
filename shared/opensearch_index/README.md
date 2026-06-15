# OpenSearch index writer

This directory is the canonical implementation for creating and updating the
`datasets` OpenSearch index.

It is intentionally self-contained and must not import Flask, SQLAlchemy, or
application modules. Dataset and organization objects are accepted by
duck-typed attributes so the package can be copied into `datagov-catalog`
tests without bringing harvester application dependencies with it.

Production ownership belongs to `datagov-harvester`. The catalog application
must use its own read-only OpenSearch code in production. Catalog tests may
vendor an exact copy of this directory to create their test index.

When the writer changes:

1. Copy this entire directory to the catalog test vendor directory.
2. Compare the two directories recursively.
3. Run both repositories' OpenSearch tests.

Do not make catalog-specific edits in the copied directory. Test adapters and
fixtures belong outside the vendored directory.
