import os
from app.interface import HarvesterDBInterface
from harvester.utils import CFHandler

DATABASE_URI = os.getenv("DATABASE_URI")

interface = HarvesterDBInterface()
cf_handler = CFHandler()

MAX_JOBS_COUNT = 3
APP_GUID = "f4ab7f86-bee0-44fd-8806-1dca7f8e215a"  # TODO: TEMP


def create_task(jobId):
    return {
        "app_guuid": APP_GUID,
        "command": f"python harvest.py {jobId}",
        "task_id": f"harvest-job-{jobId}",
    }


def load_manager():
    # confirm CF_INSTANCE_INDEX == 0 or bail

    # filter harvestjobs by pending / pending_manual
    jobs = interface.get_harvest_jobs_by_filter_multival(
        "status", ["pending", "pending_manual"]
    )
    print("db jobs")
    print(jobs)

    # get current list of all tasks
    current_tasks = cf_handler.get_all_app_tasks(APP_GUID)

    # filter out in_process tasks
    running_tasks = cf_handler.get_all_running_tasks(current_tasks)

    # confirm tasks < MAX_JOBS_COUNT or bail
    if MAX_JOBS_COUNT < running_tasks:
        return
    else:
        slots = MAX_JOBS_COUNT - running_tasks

    # sort jobs by pending_manual first
    jobs.sort(key=lambda x: x["status"], reverse=True)

    # slice off jobs to invoke
    jobs_to_invoke = jobs[:slots]

    # invoke cf_task with next job(s)
    # THEN mark that job(s) as running in the DB
    for job in jobs_to_invoke:
        task_contract = create_task(job["id"])
        print("task_contract")
        print(task_contract)
        task_status = cf_handler.start_task(**task_contract)
        print("task_status")
        print(task_status)
        interface.update_harvest_job(job["id"], {"status": "in_progress"})
