import argparse
import hashlib
import json
import os
import xml.etree.ElementTree as ET
from typing import Union

import requests
from bs4 import BeautifulSoup
import sansjson
from cloudfoundry_client.client import CloudFoundryClient
from cloudfoundry_client.v3.tasks import TaskManager

# ruff: noqa: F841


def get_title_from_fgdc(xml_str: str) -> str:
    tree = ET.ElementTree(ET.fromstring(xml_str))
    return tree.find(".//title").text


def parse_args(args):
    parser = argparse.ArgumentParser(
        prog="Harvest Runner", description="etl harvest sources"
    )
    parser.add_argument("-j", "--jobid")

    return parser.parse_args(args)


def convert_set_to_list(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


def sort_dataset(d):
    return sansjson.sort_pyobject(d)


def dataset_to_hash(d):
    # TODO: check for sh1 or sha256?
    # https://github.com/GSA/ckanext-datajson/blob/a3bc214fa7585115b9ff911b105884ef209aa416/ckanext/datajson/datajson.py#L279
    return hashlib.sha256(json.dumps(d, sort_keys=True).encode("utf-8")).hexdigest()


def open_json(file_path):
    """open input json file as dictionary
    file_path (str)     :   json file path.
    """
    with open(file_path) as fp:
        return json.load(fp)


def download_file(url: str, file_type: str) -> Union[str, dict]:
    resp = requests.get(url)
    if 200 <= resp.status_code < 300:
        if file_type == ".xml":
            return resp.content
        return resp.json()


def traverse_waf(
    url, files=[], file_ext=".xml", folder="/", filters=["../", "dcatus/"]
):
    """Transverses WAF
    Please add docstrings
    """
    # TODO: add exception handling
    parent = os.path.dirname(url.rstrip("/"))

    folders = []

    res = requests.get(url)
    if res.status_code == 200:
        soup = BeautifulSoup(res.content, "html.parser")
        anchors = soup.find_all("a", href=True)

        for anchor in anchors:
            if (
                anchor["href"].endswith(folder)  # is the anchor link a folder?
                and not parent.endswith(
                    anchor["href"].rstrip("/")
                )  # it's not the parent folder right?
                and anchor["href"] not in filters  # and it's not on our exclusion list?
            ):
                folders.append(os.path.join(url, anchor["href"]))

            if anchor["href"].endswith(file_ext):
                # TODO: just download the file here
                # instead of storing them and returning at the end.
                files.append(os.path.join(url, anchor["href"]))

    for folder in folders:
        traverse_waf(folder, files=files, filters=filters)

    return files


def download_waf(files):
    """Downloads WAF
    Please add docstrings
    """
    output = []
    for file in files:
        output.append({"url": file, "content": download_file(file, ".xml")})

    return output


class CFHandler:
    def __init__(self, url: str, user: str, password: str):
        self.url = url
        self.user = user
        self.password = password
        self.setup()

    def setup(self):
        self.client = CloudFoundryClient(self.url)
        self.client.init_with_user_credentials(self.user, self.password)
        self.task_mgr = TaskManager(self.url, self.client)

    def start_task(self, app_guuid, command, task_id):
        return self.task_mgr.create(app_guuid, command, task_id)

    def stop_task(self, task_id):
        return self.task_mgr.cancel(task_id)

    def get_task(self, task_id):
        return self.task_mgr.get(task_id)

    def get_all_app_tasks(self, app_guuid):
        return [task for task in self.client.v3.apps[app_guuid].tasks()]

    def get_all_running_tasks(self, tasks):
        return sum(1 for _ in filter(lambda task: task["state"] == "RUNNING", tasks))

    def read_recent_app_logs(self, app_guuid, task_id=None):
        app = self.client.v2.apps[app_guuid]
        logs = filter(lambda lg: task_id in lg, [str(log) for log in app.recent_logs()])
        return "\n".join(logs)
