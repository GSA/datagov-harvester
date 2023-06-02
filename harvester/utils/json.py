import json
import requests
# ruff: noqa: F841


def open_json(file_path):
    """open input json file as dictionary
    file_path (str)     :   json file path.
    """
    try:
        with open(file_path) as fp:
            return json.load(fp)
    except Exception as e:
        pass

def download_json(url):
    """download file and pull json from response
    url (str)   :   path to the file to be downloaded.
    """
    try:
        resp = requests.get(url)
    except requests.exceptions.RequestException as e:
        raise Exception(e)
    except requests.exceptions.JSONDecodeError as e:
        raise Exception(e)

    if resp.status_code != 200:
        raise Exception("non-200 status code")

    try:
        data = resp.json()
    except json.JSONDecodeError as e:
        raise Exception(e)

    return data
