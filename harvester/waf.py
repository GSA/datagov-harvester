import requests
from bs4 import BeautifulSoup
import os


def traverse_waf(url, files=[], file_ext=".xml", folder="/", filters=[]):
    # TODO: add exception handling
    parent = os.path.dirname(url.rstrip("/"))

    res = requests.get(url)
    if res.status_code == 200:
        soup = BeautifulSoup(res.content, "html.parser")
        anchors = soup.find_all("a", href=True)

        folders = []
        for anchor in anchors:
            if (
                anchor["href"].endswith(folder)
                and not parent.endswith(anchor["href"].rstrip("/"))
                and anchor["href"] not in filters
            ):
                folders.append(os.path.join(url, anchor["href"]))

            if anchor["href"].endswith(file_ext):
                files.append(os.path.join(url, anchor["href"]))

    for folder in folders:
        traverse_waf(folder, files=files, filters=filters)

    return files


def download_waf(files):
    output = []
    for file in files:
        res = requests.get(file)
        if res.status_code == 200:
            output.append(res.content)

    return output
