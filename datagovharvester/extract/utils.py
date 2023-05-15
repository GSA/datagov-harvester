import requests
import json


def download_catalog(url):
    try:
        resp = requests.get(url)
    except requests.exceptions.RequestException as e:
        return e

    if resp.status_code != 200:
        return None

    try:
        data = resp.json()
    except json.JSONDecodeError as e:
        return e

    return data
