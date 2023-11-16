This repository contains the Airflow Infrastructure code for Datagov's Harvester 2.0, and is intended to follow best practises for running Airflow in production.

## Background

For cloud.gov (CF), we build Airflow using CF's Python Buildpak
For local development, the offical Airflow Docker image is used.

## Setup

### Non-Automated Tasks for Cloud.gov
1. Push the apps to cloud.gov
2. Initialize and configure a PostgresRDB instance
    2.1 size / config
3. Bind that to the apps using `cf bind-service {AIRFLOW_WEBSERVER_APP_NAME} {AIRFLOW_DB_NAME}`
6. Add a network policy to allow the webserver to talk to the scheduler: 
    - `cf add-network-policy {AIRFLOW_WEBSERVER_APP_NAME}{AIRFLOW_SCHEDULER_APP_NAME} -s development --protocol tcp --port 8080`
7. Repush your app for the buildpak to bind the DB
