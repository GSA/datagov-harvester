import datetime
import uuid

from controller.dataset import db as dataset_db
from controller.job import bp, db


def extract(dataset):
    dataset["1_step_extract"] = "Done at " + str(datetime.datetime.now())
    return dataset


def validate(dataset):
    dataset["2_step_validate"] = "Done at " + str(datetime.datetime.now())
    return dataset


def transform(dataset):
    dataset["3_step_transform"] = "Done at " + str(datetime.datetime.now())
    return dataset


def load(dataset):
    dataset["4_step_load"] = "All done at " + str(datetime.datetime.now())
    return dataset


def process_dataset(dataset):
    ds = extract(dataset)
    valid_ds = validate(ds)
    new_ds = transform(valid_ds)
    final_ds = load(new_ds)
    final_ds["job_status"] = True
    return final_ds


# just for testing purpose
@bp.route("/run/", methods=["GET"])
def run():
    id = str(uuid.uuid4())
    db[id] = process_dataset(dataset_db)
    return {"data": db[id], "job_id": id}
