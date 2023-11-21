"""
### Dynamically map a task over an XCom output and reduce the results

Simple DAG that shows basic dynamic task mapping with a reduce step.

THIS IS A TEST OF STATIC DAG. DELETE WHENEVER WE DON'T NEED IT ANYMORE...
"""

from airflow.decorators import dag, task
from pendulum import datetime


@dag(start_date=datetime(2023, 5, 1), schedule=None, catchup=False, tags=['test'])
def map_and_reduce():
    # upstream task returning a list or dictionary
    @task
    def get_123():
        return [1, 2, 3]

    # dynamically mapped task iterating over list returned by an upstream task
    @task
    def multiply_by_y(x, y):
        return x * y

    multiplied_vals = multiply_by_y.partial(y=42).expand(x=get_123())

    # optional: having a reducing task after the mapping task
    @task
    def get_sum(vals):
        total = sum(vals)
        return total

    get_sum(multiplied_vals)


map_and_reduce()
