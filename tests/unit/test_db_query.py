import pytest

from database.interface import HarvesterDBInterface
from database.models import HarvestSource


def test_facet_builder_empty():
    assert HarvesterDBInterface.query_filter_builder(HarvestSource, "") == []


def test_facet_builder_single():
    assert len(HarvesterDBInterface.query_filter_builder(HarvestSource, "id eq 1")) == 1


def test_facet_builder_notequal():
    assert (
        len(
            HarvesterDBInterface.query_filter_builder(
                HarvestSource, "url startswith_op http:"
            )
        )
        == 1
    )


def test_facet_builder_multiple():
    assert (
        len(
            HarvesterDBInterface.query_filter_builder(
                HarvestSource, "id eq 1,organization_id eq 2"
            )
        )
        == 2
    )


def test_facet_builder_exception():
    with pytest.raises(AttributeError):
        HarvesterDBInterface.query_filter_builder(HarvestSource, "nonexistent eq 1")
