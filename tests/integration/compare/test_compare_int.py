from harvester.compare import compare


def test_compare(data_sources):
    compare_res = compare(*data_sources)

    assert len(compare_res["create"]) == 1
    assert len(compare_res["update"]) == 3
    assert len(compare_res["delete"]) == 1
