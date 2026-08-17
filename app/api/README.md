# app/api

JSON/OpenAPI routes for the harvester's admin app, versioned under `/api/v1/...`. Registration lives in `app/routes.py` (`API_VERSIONS`); Swagger UI is at `/openapi/docs`.

See the [Harvester APIs wiki page](https://github.com/GSA/data.gov/wiki/Harvester-APIs) for how this API relates to `api.gsa.gov` and `api.data.gov`, and for the full versioning policy this README summarizes.

## Versioning policy

Stay on `v1` by default. Bump only the specific endpoint that needs a breaking change; leave every other endpoint on `v1`. Do not bump the whole API for a change that only touches one endpoint.

Hitting `/api/<path>` without a version prefix redirects (308) to whichever version is last in `API_VERSIONS`, currently `v1`. That means today's redirect is invisible, but once a `v2` exists, anything still calling the unprefixed path moves onto it automatically. Callers should always pin to an explicit version (`/api/v1/...`).

**Breaking, needs a new version:**
- Removing or renaming a field or endpoint
- Changing a field's type or meaning
- Changing required parameters or the shape of a response
- Tightening validation on input that previously succeeded

**Non-breaking, no version bump:**
- Adding a new endpoint
- Adding a new optional field
- Adding a new enum value existing clients can ignore

**To move one endpoint to a new version:**
1. Add the changed endpoint under `/api/v2/<endpoint>`; leave `/api/v1/<endpoint>` serving the old behavior unchanged.
2. Write a new, separate test suite for the `v2` endpoint. `v1` and `v2` tests are not shared, since the two versions' behavior differs by definition.
3. Leave every other endpoint registered only under `v1`.
4. Update the wiki page and the endpoint's Swagger docs with the version split.

Note: `API_VERSIONS` in `app/routes.py` currently mounts the entire `api` blueprint per version, which versions the whole API, not individual endpoints. Landing one endpoint on `v2` while the rest stay on `v1` needs that mechanism to change to per-route overrides first.
