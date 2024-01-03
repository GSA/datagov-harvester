from harvester.compare import compare


def test_artificial_compare(artificial_data_sources):
    """tests artificial datasets compare"""

    compare_res = compare(*artificial_data_sources)

    assert len(compare_res["create"]) == 1
    assert len(compare_res["update"]) == 1
    assert len(compare_res["delete"]) == 1


def test_compare(data_sources):
    compare_res = compare(*data_sources)

    assert len(compare_res["create"]) == 1
    assert len(compare_res["update"]) == 3
    assert len(compare_res["delete"]) == 1
