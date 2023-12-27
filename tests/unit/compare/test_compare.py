from harvester.compare import compare


def test_compare():
    """tests compare"""

    harvest_source = {
        "1": "de955c1b-fa16-4b84-ad6c-f891ba276056",  # update
        "2": "6d500ebc-19f8-4541-82b0-f02ad24c82e3",  # do nothing
        "3": "9aeef506-fbc4-42e4-ad27-c2e7e9f0d1c5",  # create
    }

    ckan_source = {
        "1": "fcd3428b-0ba7-48da-951d-fe44606be556",
        "2": "6d500ebc-19f8-4541-82b0-f02ad24c82e3",
        "4": "dae9b42c-cfc5-4f71-ae97-a5b75234b14f",  # delete
    }

    compare_res = compare(harvest_source, ckan_source)

    assert len(compare_res["create"]) == 1
    assert len(compare_res["update"]) == 1
    assert len(compare_res["delete"]) == 1
