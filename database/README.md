# Data.gov Harvester Database

The data.gov harvester database is defined in the models.py.

Most tables are assumed to be populated on a user basis,
however the locations table is meant to be pre-populated.
The items for the locations table can be found in
[Google Drive](https://drive.google.com/drive/u/1/folders/1V5QXo-o-DN1yXkXhBFtPUhelDIGrPAbs).

To load this file into a cloud.gov database, run the following commands.
You should be logged into cloud.gov, see 
https://github.com/GSA/data.gov/wiki/cloud.gov
if unsure how to do that.
You'll want to run them in separate bash's, as the SSH port
forwarding persists.

`./cf_connect.sh`

`./load_locations.sh`

There is probably an improvement to be made to this process,
however running cf ssh in the background always crashed. So
this is split into 2 distinct processes.

If you need to load this to the local DB, simply overwrite the
credentials with what is in the `.env` file at the root of this
project.

## Validator place-name data

The public validator has no database binding, so it answers place-name lookups
from `dcatus_validation/reference_data/location_names.sqlite`: every row of this
table's `display_name` (no geometry) behind a trigram index, matched the way the
harvester's `ILIKE '%name%'` lookup does (see `dcatus_validation/places.py`). Whenever the locations table is reloaded, rebuild it from prod and
commit the result. With `cf target -s prod` and the tunnel from
`./cf_connect.sh` open in another shell:

`./export_validator_locations.sh`

Or against any other database:

`poetry run python scripts/refresh_location_names.py postgresql+psycopg://user:pass@host:port/db`
