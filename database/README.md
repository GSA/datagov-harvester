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