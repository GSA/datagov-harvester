import os


def is_running_on_cloud_foundry() -> bool:
    return "VCAP_APPLICATION" in os.environ
