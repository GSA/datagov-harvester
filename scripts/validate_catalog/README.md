# Validate a catalog URL

The hosted `/validate` page caps uploads at 10MB (`app/constants.py`
`MAX_UPLOAD_MB`). `validate_catalog.py` calls the same validation logic
directly, with no size limit, for catalogs too large to check that way.

## validate_catalog.py

```
docker compose exec app python3 scripts/validate_catalog/validate_catalog.py <url> [schema]
```

`schema` defaults to `"dcatus3.0 catalog"`. Other options:
`"dcatus1.1: federal dataset"`, `"dcatus1.1: non-federal dataset"`.

Example:

```
docker compose exec app python3 scripts/validate_catalog/validate_catalog.py \
  https://gis-kingcounty.opendata.arcgis.com/api/feed/dcat-us/3.0.json
```
