# Routes for Harvester-admin

This document tries to list the URL structure for this web app.

This web app multiplexes the API routes and the web frontend onto the
same URLs using Flask's `is_json` to differentiate what type of request
it is.

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


- `/organizations/`: Lists organizations, GET only, no login required
- `/organization/add`: Add a new organization. Login-required. POST
  with JSON updates in the DB. POST with form data adds or checks for errors.
  GET should load the `edit_data` template.
- `/organization/<id>`: Detail page for a single org, GET only, no login
  required
- `/organization/config/edit/<id>`: Edit an organization, POSTing JSON updates
  the DB, POSTing a form updates and checks for errors. GET should load the
  `edit_data` template
- `/organization/config/edit/<id>`: Edit an organization, POSTing JSON updates
  the DB, POSTing a form updates and checks for errors. GET should load the
  `edit_data` template. Login required.
- `/organization/config/delete/<id>`: Delete an organization. POST only,
  login-required


- `/harvest_source/add`: Add a new harvest source. POST takes JSON or form
  data and GET renders `edit_data` template. Login required
- `/harvest_source/<id>`: Details for a single harvest source. GET only, no
  login required
- `/harvest_sources/`: List of harvest sources, GET only, no login required
- `/harvest_source/config/edit/<id>`: edit an existing source. POST takes JSON
  or form data and GET renders `edit_data` template. Login required
- `/harvest_source/config/delete/<id>`: Delete a harvest source. POST only,
  login-required
- `/harvest_source/harvest/<id>/<type>`: trigger a harvest of this source.
  GET only. Login required.


- `/harvest_job/add`: Add a new harvest job. POST only. Login is required
- `/harvest_job/<id>`: Details on a job, GET, no login required
- `/harvest_job/<id>`: PUT, update an existing harvest job, Login required.
- `/harvest_job/<id>`: DELETE, delete a harvest job, Login required.
- `/harvest_job/cancel/<id>`: cancel a given job, GET and POST, login required
- `/harvest_job/`: Details on every job that has errors???, GET only, no login
  required
- `harvest_job/<id>/errors/<type>`: CSV download errors for this job, GET only, no
  login required

- `/harvest_record/<id>`: Details for a single harvest record, GET only, no
  login required
- `/harvest_record/<id>/raw`: Raw details for a harvest record, GET only, no
  login required
- `/harvest_records/`: List all harvest records, GET only, no login required
- `/harvest_record/add`: Add a new harvest record. POST with JSON creates.
  What GET supposed to do???. Login required
- `/harvest_record/<id>/errors`: List errors for a harvest record, GET only,
  no login required
- `/harvest_error/`: List all harvest errors, GET only, no login required
- `/harvest_error/<id>`: Details for a harvest error, GET only, no login required


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
