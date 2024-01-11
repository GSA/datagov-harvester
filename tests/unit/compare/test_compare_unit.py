import copy

from harvester import Source, compare
from harvester.utils.util import dataset_to_hash, sort_dataset


def test_artificial_compare(artificial_data_sources):
    """tests artificial datasets compare"""

    compare_res = compare(*artificial_data_sources)

    assert len(compare_res["create"]) == 1
    assert len(compare_res["update"]) == 1
    assert len(compare_res["delete"]) == 1


def test_compare(dcatus_compare_json_source, ckan_datasets_resp_json_source):
    """TODO what/why is this"""
    compare_res = compare(dcatus_compare_json_source, ckan_datasets_resp_json_source)

    assert len(compare_res["create"]) == 1
    assert len(compare_res["update"]) == 3
    assert len(compare_res["delete"]) == 1


def test_sort(dcatus_compare_json_source, ckan_datasets_resp_json_source):
    """TODO what/why is this"""

    harvest_source_no_sort = get_test_sort_compare(dcatus_compare_json_source)
    ckan_source = get_test_sort_compare(ckan_datasets_resp_json_source)
    compare_res_no_sort = compare(harvest_source_no_sort, ckan_source)
    # more datasets need to be updated simply because we didn't sort them
    assert len(compare_res_no_sort["create"]) == 1
    assert len(compare_res_no_sort["update"]) == 6
    assert len(compare_res_no_sort["delete"]) == 1

    harvest_source_with_sort = get_test_sort_compare(
        dcatus_compare_json_source, sort=True
    )
    compare_res = compare(harvest_source_with_sort, ckan_source)
    # applying the sort lowers us back down to what we expect.
    assert len(compare_res["create"]) == 1
    assert len(compare_res["update"]) == 3
    assert len(compare_res["delete"]) == 1


def get_test_sort_compare(source: Source, sort=False) -> Source:
    """Helper function to create new copy of source sorted/not sorted"""
    new_source = Source()
    for identifier in source.records:
        record = source.records[identifier]
        record_copy = copy.deepcopy(record)
        record_copy.raw_hash = (
            dataset_to_hash(sort_dataset(record.raw_metadata))
            if sort
            else dataset_to_hash(record.raw_metadata)
        )
        new_source.records[identifier] = record_copy

    return new_source
