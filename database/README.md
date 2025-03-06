# Data.gov Harvester Database

The data.gov harvester database is defined in the models.py.

There is an complete table dependancy called `locations`. This
table is built from the `locations.csv` file in this directory.

To load this file into a database, run the following command
(assuming psql is setup locally, and database is configured
and connection is setup with variable `DATABASE_URI` in bash)

./load_csv.sh locations.csv locations