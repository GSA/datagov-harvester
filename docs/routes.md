# Routes for Harvester-admin

This document tries to list the URL structure for this web app.

Route handlers live under the `app/api/` and `app/main/` packages: HTML pages on
the `main` blueprint and JSON/OpenAPI routes on the `api` blueprint.

The JSON API and the web frontend are served from separate URLs. The web
frontend (HTML pages and form submissions) lives at the un-prefixed paths
below, while **every** route on the `api` blueprint is served under an `/api`
prefix (applied once via `url_prefix="/api"`, not hand-typed per route). JSON
callers authenticate with the `X-API-Key` header.

Authenticated mutation endpoints on the `api` blueprint use `@api.doc(hide=True)`
and are omitted from the public Swagger docs at `/openapi/docs`.

## Routes

- `/login`: Construct the Login.gov URL and redirect to it. Evidently, login
  is not required.
- `/logout`: Take the user out of the session store. Login should not be
  required.
- `/callback`: Second step in the Login.gov OIDC flow coming back from
  Login.gov. Validates tokens, verifies users in the database and logs them
  in if they exist. Can't be login-required because it needs to be accessible
  before login completes.
- `/`: Redirect to "View Organizations", login not required.


- `/api/organizations/`: Lists organizations as JSON, GET only, no login required
- `/organization_list/`: HTML list of organizations, GET only, no login required
- `/api/organization_list/`: JSON list of organizations, GET only, no login
  required
- `/organization/add`: HTML form to add a new organization. GET renders the
  `edit_data` template, POST handles the form submission. Login required.
- `/api/organization/add`: Add a new organization via JSON. POST only. Login
  required (JSON API).
- `/organization/<identifier>`: HTML detail page for a single org (by UUID or
  slug), GET only, no login required
- `/organization/<identifier>`: POST handles the authenticated edit/delete
  action form submitted from the detail page. Login required.
- `/api/organization/<identifier>`: JSON detail for a single org (by UUID or
  slug), GET only, no login required
- `/api/organization/<id>`: DELETE an organization. Login required, JSON API.
- `/organization/edit/<id>`: HTML form to edit an organization. GET renders the
  `edit_data` template, POST handles the form submission. Login required.
- `/api/organization/edit/<id>`: Update an organization via JSON. POST only.
  Login required (JSON API).


- `/harvest_source/add`: HTML form to add a new harvest source. GET renders the
  `edit_data` template, POST handles the form submission. Login required.
- `/api/harvest_source/add`: Add a new harvest source via JSON. POST only.
  Login required (JSON API).
- `/harvest_source/<id>`: Details for a single harvest source. GET only, no
  login required
- `/api/harvest_sources/`: List of harvest sources, GET only, no login required
- `/harvest_source/edit/<id>`: HTML form to edit an existing source. GET renders
  the `edit_data` template, POST handles the form submission. Login required.
- `/api/harvest_source/edit/<id>`: Update an existing source via JSON. POST
  only. Login required (JSON API).
- `/api/harvest_source/<id>`: Delete a harvest source. DELETE only, login-required
- `/api/harvest_source/harvest/<id>/<type>`: trigger a harvest of this source.
  GET only. Login required.


- `/api/harvest_job/add`: Add a new harvest job via JSON. POST only. Login is
  required.
- `/harvest_job/<id>`: HTML detail page for a job, GET, no login required
- `/api/harvest_job/<id>`: JSON detail for a job, GET, no login required
- `/api/harvest_job/<id>`: PUT, update an existing harvest job, Login required.
- `/api/harvest_job/<id>`: DELETE, delete a harvest job, Login required.
- `/api/harvest_job/cancel/<id>`: cancel a given job, GET and POST, login required
- `/api/harvest_jobs/`: List all harvest jobs as JSON, GET only, no login
  required
- `/api/harvest_job/<id>/errors/<type>`: CSV download errors for this job, GET
  only, no login required

- `/api/harvest_record/<id>`: Details for a single harvest record, GET only, no
  login required
- `/api/harvest_record/<id>/raw`: Raw details for a harvest record, GET only, no
  login required
- `/api/harvest_records/`: List all harvest records, GET only, no login required
- `/api/harvest_record/add`: Add a new harvest record via JSON. POST only.
  Login required.
- `/api/harvest_record/<id>/errors`: List errors for a harvest record, GET only,
  no login required
- `/api/harvest_job_errors/`: List harvest job errors as JSON, GET only, no
  login required
- `/api/harvest_record_errors/`: List harvest record errors as JSON, GET only,
  no login required
- `/api/harvest_error/<id>`: Details for a harvest error, GET only, no login required


- `/metrics/`: recent harvest job details, GET only, no login required

## Notes

- TODO: `/organization/<id>/edit` might be more natural
- TODO: Use DELETE verb instead of `/organization/delete/<id>`
- TODO: `/harvest_source/<id>/edit` might be more natural
- TODO: Use DELETE verb instead of `/harvest_source/delete/<id>`
- TODO: `/harvest_job/<id>/cancel` might be more natural
- Probably don't need a `/harvest_job/<id>/delete` endpoint?

- `/harvest_source/<id>/harvest/<type>` is a bit weird. Can we make this a method
  under `/harvest_job/` like `/harvest_job/add/<source_id>`?
- `/harvest_job/<id>/errors/<type>` seems weird for specifying just a single
  error type. If type is missing, should we show return all the error types?
  Is there a page for showing errors in web/HTML instead of CSV? Would
  `errors_download` or `errors_csv` make its purpose more clear?
- `/harvest_record/<id>` and `/harvest_records`, these appear to be JSON only.
  Should we add HTML web versions of these?
- `/harvest_error` and `/harvest_error/<id>` appear to be JSON only. Should we
  add HTML web versions of these?
