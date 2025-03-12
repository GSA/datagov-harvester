# Data.gov Harvester Database

The data.gov harvester database is defined in the models.py.

Most tables are assumed to be populated on a user basis,
however the locations table is meant to be pre-populated.
The items for the locations table can be found in
[Google Drive](https://drive.google.com/drive/u/1/folders/1V5QXo-o-DN1yXkXhBFtPUhelDIGrPAbs).

To load this file into a database, run the following command
(assuming psql is setup locally, and database is configured
and connection is setup with variable `DATABASE_URI` in bash)

./load_csv.sh locations.csv locations