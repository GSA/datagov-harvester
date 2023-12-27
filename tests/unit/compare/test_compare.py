from harvester.compare import compare


def test_compare(data_sources):
    """tests compare"""

    compare_res = compare(*data_sources)

    assert len(compare_res["create"]) == 1
    assert len(compare_res["update"]) == 1
    assert len(compare_res["delete"]) == 1
    assert "2" not in compare_res
