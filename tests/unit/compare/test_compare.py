from harvester import compare
from harvester.utils.util import dataset_to_hash, sort_dataset


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


def test_sort(data_sources_raw):
    harvest_source, ckan_source = data_sources_raw

    harvest_source_no_sort = harvest_source.copy()
    for k, v in harvest_source_no_sort.items():
        harvest_source_no_sort[k] = dataset_to_hash(v)

    for k, v in ckan_source.items():
        ckan_source[k] = dataset_to_hash(v)

    compare_res_no_sort = compare(harvest_source_no_sort, ckan_source)

    # more datasets need to be updated simply because we didn't sort them
    assert len(compare_res_no_sort["create"]) == 1
    assert len(compare_res_no_sort["update"]) == 6
    assert len(compare_res_no_sort["delete"]) == 1

    harvest_source_with_sort = harvest_source.copy()
    for k, v in harvest_source_with_sort.items():
        harvest_source_with_sort[k] = dataset_to_hash(sort_dataset(v))

    compare_res = compare(harvest_source_with_sort, ckan_source)

    # applying the sort lowers us back down to what we expect.
    assert len(compare_res["create"]) == 1
    assert len(compare_res["update"]) == 3
    assert len(compare_res["delete"]) == 1
