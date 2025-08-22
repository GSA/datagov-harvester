import enum
import logging
import os
from functools import wraps
from pathlib import Path
from time import time

import click
from lib.qa.datasets import compare_datasets
from lib.qa.harvest_sources import compare_harvest_sources
from lib.qa.organizations import compare_organizations

logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parents[0]
OUTPUT_DIR = SCRIPT_DIR / "qa_output"


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
@click.option(
    "--seed",
    type=int,
    help="Random seed value for reproduction",
    default=None,
)
@click.option(
    "--sample-size",
    type=int,
    help="Number of datasets to compare",
    default=25,
)
@timing
def process_job(job_type: str, seed: int, sample_size: int):
    username = os.getenv("DATAGOV_BASIC_AUTH_USER")
    password = os.getenv("DATAGOV_BASIC_AUTH_PASS")

    # if not all([username, password]):
    #     logger.critical(
    #         "basic auth credentials for catalog need to be set as env vars. exiting."
    #     )
    #     return

    if job_type == Job.all:
        compare_organizations(OUTPUT_DIR)
        compare_harvest_sources(OUTPUT_DIR)
        compare_datasets(OUTPUT_DIR, seed=seed, sample_size=sample_size)
    if job_type == Job.organization:
        compare_organizations(OUTPUT_DIR)
    if job_type == Job.harvest_source:
        compare_harvest_sources(OUTPUT_DIR)
    if job_type == Job.dataset:
        compare_datasets(OUTPUT_DIR, seed=seed, sample_size=sample_size)


if __name__ == "__main__":
    process_job()
