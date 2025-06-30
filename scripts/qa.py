import logging
import os
import shutil
import enum
from functools import wraps
from pathlib import Path
from time import time

import click

from lib.qa.organizations import compare_organizations
from lib.qa.harvest_sources import compare_harvest_sources
from lib.qa.datasets import compare_datasets

logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parents[0]
OUTPUT_DIR = SCRIPT_DIR / "qa_output"

# prepare output directory
if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)  # deletes dir and all contents
os.mkdir(OUTPUT_DIR)


## helpers functions
def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        logger.info(f"Function {f.__name__} took {te - ts:2.4f} seconds")
        return result

    return wrap


## classes
class Job(enum.StrEnum):
    all = enum.auto()
    organization = enum.auto()
    harvest_source = enum.auto()
    dataset = enum.auto()

    def __str__(self):
        return self.value


## main comparison functions


@click.command()
@click.option(
    "--job-type",
    type=click.Choice(Job),
    help="Choose a job",
    default=Job.all,
)
@timing
def process_job(job_type: str):
    if job_type == Job.all:
        compare_organizations(OUTPUT_DIR)
        compare_harvest_sources(OUTPUT_DIR)
        compare_datasets(OUTPUT_DIR)
    if job_type == Job.organization:
        compare_organizations(OUTPUT_DIR)
    if job_type == Job.harvest_source:
        compare_harvest_sources(OUTPUT_DIR)
    if job_type == Job.dataset:
        compare_datasets(OUTPUT_DIR)


if __name__ == "__main__":
    process_job()
