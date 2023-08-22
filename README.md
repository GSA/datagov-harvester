This Airflow ETL test was built with the following guides:

## Background

-   https://towardsdatascience.com/run-airflow-docker-1b83a57616fb
    -   for airflow setup
-   https://www.freecodecamp.org/news/orchestrate-an-etl-data-pipeline-with-apache-airflow/
    -   TODO: still need to wire in actual services
-   https://davidgriffiths-data.medium.com/debugging-airflow-in-a-container-with-vs-code-7cc26734444

## Setup

1. Initialize the Airflow client:

```
docker-compose up airflow-init
```

2. After that completes successfully, you start your containers as normal:

```
docker-compose up
```

3. Access the Airflow UI by visiting: `localhost:8080` using user:password :: `airflow:airflow`

## Debugging Remote Container

(using VS Code)

1. Install Dev Containers extension `ms-vscode-remote.remote-containers`
2. After you have installed the Remote — Container extension you can open up VS Code’s command pallet (ctrl+shift+p) and type: `Remote-Containers: Attach to a Running Container…`
3. Attach to `airflow-scheduler`
4. Open a terminal.
   4.1 If you receive the error, then you need to modify your dev container settings:

```
The terminal process failed to launch: Path to shell executable "/sbin/nologin" does not exist.
```

4.2 Run `Remote-Containers: Open Container Configuration File` from the Command Palette after attaching.
4.3 Add `"remoteUser": "airflow"` to the JSON
4.4 Close the Container window and reattach
4.5 You should now be able to open a terminal 5. Select the correct Python interpreter by opening the command pallete and choosing the global python executable instead of the recommended one.
5.1 NOTE: This fixes the error you may encounter when when running the debugger:

```
Exception has occurred: ModuleNotFoundError
No module named 'airflow'
  File "/home/airflow/.local/bin/airflow", line 5, in <module>
    from airflow.__main__ import main
ModuleNotFoundError: No module named 'airflow'
```

5. Create a launch.json. This configuration worked for me:

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
                "etl_twitter_pipeline",
                "2023-08-17"
            ]
        }
    ]
}
```

5.1 Note that the last the last arg is based on the date YYYY-MM-DD or your log records in the `airflow/logs/scheduler` directory. This is only generated after running the `etl_twitter_pipeline` DAG in the UI and allowing it to dump some logs.

6. WORKING CONFIGS

VS CODE LAUNCH CONFIG
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Airflow Test",
            "type": "python",
            "request": "launch",
            "program": "/home/airflow/.local/bin/airflow",
            "args": [
                "tasks",
                "test",
                "dcatus_pipeline",
                "extract_dcatus",
                "2023-08-17"
            ],
            "console": "integratedTerminal"
        }
    ]
}



CONTAINER CONFIG FILE
{
    "workspaceFolder": "/opt/airflow",
    "extensions": [
        "ms-python.python",
        "ms-python.vscode-pylance"
    ],
    "remoteUser": "airflow"
}


## TODO

    - seed proper `devcontainer.json` file into container
    - https://betterprogramming.pub/running-a-container-with-a-non-root-user-e35830d1f42a
