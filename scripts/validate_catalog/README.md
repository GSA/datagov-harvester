# Validate a catalog URL

The hosted `/validate` page caps uploads at 10MB (`app/constants.py`
`MAX_UPLOAD_MB`). `validate_catalog.py` calls the same validation logic
directly, with no size limit, for catalogs too large to check that way.

## validate_catalog.py

```
docker compose exec app python3 scripts/validate_catalog/validate_catalog.py <url> [--schema SCHEMA] [--output FILE]
```

`--schema` defaults to `"dcatus3.0 catalog"`. Other options:
`"dcatus1.1: federal dataset"`, `"dcatus1.1: non-federal dataset"`.

`--output`/`-o` writes the full error list to a file instead of the
terminal; the dataset/error counts still print either way. Use a path
under the repo (the container's `/app`) rather than `/tmp`, since only the
repo directory is mounted from the host.

Example:

```
docker compose exec app python3 scripts/validate_catalog/validate_catalog.py \
  https://gis-kingcounty.opendata.arcgis.com/api/feed/dcat-us/3.0.json \
  --output king-county-errors.txt
```
