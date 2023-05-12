import requests


def download_catalog(url):
    resp = None
    error = None

    try:
        resp = requests.get(url)
    except requests.exceptions.RequestException as e:
        error = e

    return resp, error


def fetch_url(url):
    success = False

    resp, error_msg = download_catalog(url)

    if resp.status_code == 200:
        success = True

    data = resp.json()

    return data, success
