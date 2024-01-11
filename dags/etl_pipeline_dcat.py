import json
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

import harvester

CURRENT_DIR = Path(__file__).parent.absolute()
DAILY_HARVEST_SOURCE = CURRENT_DIR / "harvest-sources/daily_sample.json"

def create_dag(dag_id, schedule, default_args, tags, source):
    @dag(dag_id=dag_id, 
         schedule=schedule, 
         tags=tags, 
         default_args=default_args, 
         catchup=False
    )
    def etl_pipeline():
        def on_failure_callback(**context):
            ti = context['ti']
            print(f"task {ti.task_id } failed in dag { ti.dag_id } ")

        @task(on_failure_callback=on_failure_callback)
        def extract():
            return harvester.extract(source)
        
        @task(on_failure_callback=on_failure_callback)
        def transform():
            return harvester.transform(source)

        @task(on_failure_callback=on_failure_callback)
        def validate():
            return harvester.validate(source)
        
        @task(on_failure_callback=on_failure_callback)
        def load():
            return harvester.load(source)

        @task(on_failure_callback=on_failure_callback)
        def report(**kwargs):
            ti = kwargs['ti']
            dr = kwargs['dag_run']
            task_return_vals = kwargs['ti'].xcom_pull(key=None, task_ids=['extract', 'transform', 'validate', 'load'])
            return (
                f'Dag ID: {dr.dag_id}\n'
                f'Run ID: {ti.run_id}\n'
                f'DAG Run queued at: {dr.queued_at}\n'
                f'Extract reports: {task_return_vals[0]}\n'
                f'Transform reports: {task_return_vals[1]}\n'
                f'Validate reports: {task_return_vals[2]}\n'
                f'Load reports: {task_return_vals[3]}\n'
            )

        extract() >> transform() >> validate() >> load() >> report()

    etl_pipeline()

with DAILY_HARVEST_SOURCE.open() as fp:
    res = json.load(fp)
    sources = res['result']['results']
    for source in sources:
        title = source['name']
        url = source['url']
        dag_id = f"{title}_workflow"
        default_args = {"owner": "airflow", "start_date": datetime(2023, 7, 1)}
        schedule=None
        tags = ['dcat']

        create_dag(dag_id, schedule, default_args, tags, source)
