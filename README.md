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

## Develop

### Local Development 

> [!NOTE]
> `devcontainer.json` file is WIP. Currently debugging works but you have to follow extra steps to install Python extension in the container.

1. Launch project with `make up` or `docker-compose up -d`
2. Install "Dev Containers" extension `ms-vscode-remote.remote-containers`
3. Open VS Code’s command pallet (ctrl+shift+p) and type: `Dev-Containers: Attach to a Running Container…`
4. Attach to `/datagov-harvester-airflow-scheduler-1`
5. Once the container has launched, install the Python extension
5.1 This is temporary until the devcontainer.json file is respected
6. Once that is done, you want to copy the below template into `launch.json`, overwriting `{DAG_ID}` with the dag_id of the dag you wish to test.
7. Set breakpoints in your DAG file
7.1 Note that this does not work with dynamically generated DAGs AFAIK
8. Run Airflow Test from your Run and Debug menu and wait for the debugger to hit your breakpoint. Sometimes it takes awhile.

```
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Airflow Test",
            "type": "python",
            "request": "launch",
            "program": "/home/airflow/.local/bin/airflow",
            "console": "integratedTerminal",
            "args": [
                "dags",
                "test",
                "{DAG_ID}",
                "2023-08-17"
            ]
        }
    ]
}
```
