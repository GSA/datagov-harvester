import requests

# ruff: noqa: F841


def download_dcatus_catalog(url):
    try:
        return requests.get(url).json()
    except Exception as e:
        return e
