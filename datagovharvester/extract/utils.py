import requests


def extract_data(url):
    resp = None
    error = None

    try:
        resp = requests.get(url)
    except requests.exceptions.RequestException as e:
        error = e

    return resp, error
