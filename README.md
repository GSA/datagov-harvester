This repository contains the Airflow Infrastructure code for Datagov's Harvester 2.0, and is intended to follow best practises for running Airflow in production.

## Background

We use Airflow's Official Production image as a base: IMAGE URL
We extend that image and push it to the GHCR.
The same image is used in the Cloud.gov manifest as well as the local docker-compose.yml
Every effort has been made to ensure the local docker config mirrors the production configuration


## Setup

### Non-Automated Tasks for Cloud.gov
1. Push the apps to cloud.gov
2. Initialize and configure a PostgresRDB instance
    2.1 size / config
3. Bind that DB to the apps
4. Extract the Postgres URI after running `cf env $APPNAME` and looking under the VCAP_SERVICES
5. **TEMP** Copy the PostgresURI string to your shell environment as POSTGRES_URI_STRING
6. Add a network policy to allow the webserver to talk to the scheduler: 
    - `cf add-network-policy airflow-test-webserver airflow-test-scheduler -s development --protocol tcp --port 8080`

#TODO: finish this README

