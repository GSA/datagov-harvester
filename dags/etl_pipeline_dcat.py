import json
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowSkipException
from airflow.utils.helpers import chain

from jsonschema import Draft202012Validator

import harvester

CURRENT_DIR = Path(__file__).parent.absolute()
CATALOG_SCHEMA = CURRENT_DIR / "schemas/catalog.json"
DATASET_SCHEMA = CURRENT_DIR / "schemas/dataset.json"

DAILY_HARVEST_SOURCE = CURRENT_DIR / "harvest-sources/daily_sample.json"

def create_dag(dag_id, schedule, default_args, tags, source):
    @dag(dag_id=dag_id, schedule=schedule, tags=tags, default_args=default_args, catchup=False)
    def etl_pipeline():
        def on_failure_callback(**context):
            ti = context['task_instance']
            print(f"task {ti.task_id } failed in dag { ti.dag_id } ")

        @task(task_id="extract_dcatus", on_failure_callback=on_failure_callback)
        def extract(*args):
            extracted_source = harvester.extract(source)
            return extracted_source["dataset"]

        @task_group(group_id="process_dcatus")
        def process_datasets(dcatus_record):
            # TODO determine correct trigger rule that will allow for a single mapped task to proceed
            # regardless of the status of its siblings
            @task(on_failure_callback=on_failure_callback)
            def compare(dataset):
                should_update = harvester.compare(dataset["identifier"])
                if should_update:
                    return dataset
                else:
                    raise AirflowSkipException(f"{dataset['identifier']} has no updates...")
            
            @task.short_circuit(on_failure_callback=on_failure_callback)
            def transform(dataset):
                # dataset = kwargs['ti'].xcom_pull('process_dcatus.compare')[kwargs['ti'].map_index]
                    try:
                        return harvester.transform(dataset)
                    except Exception as err:
                        print(err)
                        raise AirflowSkipException(f"Skipping {dataset['identifier']}; as I was told!")                        

            @task.short_circuit(on_failure_callback=on_failure_callback)
            def validate(dataset):
                # dataset = kwargs['ti'].xcom_pull('process_dcatus.transform')[kwargs['ti'].map_index]
                with open(DATASET_SCHEMA) as json_file:
                    dcatus_dataset_schema = json.load(json_file)
                validator = Draft202012Validator(dcatus_dataset_schema)
                try:
                    validator.validate(dataset)
                    return dataset
                except Exception as err:
                    print(err)
                    raise AirflowSkipException(f"Skipping {dataset['identifier']}; as I was told!")
            
            @task.short_circuit(on_failure_callback=on_failure_callback)
            def load(dataset):
                # dataset = kwargs['ti'].xcom_pull('process_dcatus.validate')[kwargs['ti'].map_index]
                return dataset

            dataset = compare(dataset=dcatus_record)
            dataset = transform(dataset)
            dataset = validate(dataset)
            load(dataset)
            # compare(dataset=dcatus_record) >> transform() >> validate() >> load()

        extracted_records=extract(url)
        process_datasets.expand(dcatus_record=extracted_records)

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
