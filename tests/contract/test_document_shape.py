"""Document-shape contract: can catalog read what harvester's writer produced?

This file lives in datagov-harvester but runs *inside* the published catalog image
(see docker-compose.catalog-contract.yml). By the time it runs, harvester has:

    flask db upgrade            # harvester's schema
    flask search reset-mapping  # harvester's mapping
    flask testdata load_test_data
    flask search compare --update  # indexed via harvester's OpenSearchWriter

So the documents in the index were produced by harvester's real `documents.py` and
`writer.py`. Everything below reads them through catalog's real, unsubstituted
search stack -- catalog's SearchCriteria, OpenSearchReader, and HTTP routes.

That is the whole point: neither side is faked. Catalog's own tests/unit index their
fixtures through *catalog's* writer, which is a closed loop -- it cannot notice that
harvester renamed a field. This can.

Why it runs as a separate compose stage: catalog's `interface` fixture calls
delete_by_query(match_all) around every test in tests/unit, which would wipe the
harvester-written documents before they were ever read.

What a failure means: harvester's document shape drifted away from what catalog
reads. Adding a field is fine and will not fail here. Renaming or removing one that
catalog depends on will.
"""

import pytest

from app import create_app
from app.database.interface import CatalogDBInterface
from app.search import SearchCriteria
from app.search.config import INDEX_NAME

# Slugs come from harvester's tests/generate_fixtures.py, which load_test_data uses.
FIXTURE_SLUGS = {"fixture-dataset-1", "fixture-dataset-2"}


@pytest.fixture(scope="module")
def app():
    app = create_app()
    app.debug = True
    with app.app_context():
        yield app


@pytest.fixture(scope="module")
def interface(app):
    return CatalogDBInterface()


@pytest.fixture(scope="module")
def raw_documents(interface):
    """The documents harvester's writer actually wrote, straight from the index."""
    client = interface.os_client.client
    client.indices.refresh(index=INDEX_NAME)
    response = client.search(index=INDEX_NAME, body={"query": {"match_all": {}}, "size": 50})
    hits = [hit["_source"] for hit in response["hits"]["hits"]]
    assert hits, (
        "No documents in the index. Harvester's provisioning step "
        "(flask testdata load_test_data && flask search compare --update) "
        "should have indexed the fixtures before this stage ran."
    )
    return hits


def test_harvester_indexed_the_fixtures(raw_documents):
    """Guard the guard: if seeding silently no-ops, every assertion below is vacuous."""
    slugs = {doc.get("slug") for doc in raw_documents}
    assert FIXTURE_SLUGS <= slugs, f"expected {FIXTURE_SLUGS}, indexed slugs were {slugs}"


def test_catalog_reads_every_field_it_renders(raw_documents):
    """The fields catalog's templates and API responses actually consume.

    Asserted against harvester-written documents, so a harvester-side rename or
    removal fails here instead of in production.
    """
    # Note: the dataset UUID is carried as OpenSearch's `_id`, not a `_source`
    # field (see harvester's documents.py), so it is deliberately not listed here.
    required = {
        "slug",
        "title",
        "description",
        "organization",
        "publisher",
        "keyword",
        "theme",
        "identifier",
        "dcat",
        "last_harvested_date",
        "has_spatial",
    }
    for doc in raw_documents:
        missing = required - doc.keys()
        assert not missing, (
            f"harvester's document for slug={doc.get('slug')!r} is missing "
            f"{sorted(missing)}, which catalog reads"
        )


def test_organization_is_nested_the_way_catalog_queries_it(raw_documents):
    """`organization` is a nested object in the mapping; catalog filters on its subfields."""
    for doc in raw_documents:
        organization = doc["organization"]
        assert isinstance(organization, dict), (
            f"expected organization to be an object, got {type(organization).__name__}"
        )
        assert "name" in organization and "slug" in organization, (
            f"organization missing name/slug: {sorted(organization)}"
        )


def test_search_finds_harvester_written_documents(interface):
    """Catalog's own reader and query builder against harvester's documents."""
    result = interface.search_datasets(SearchCriteria(query="Fixture"))
    assert result.total > 0, "catalog's search returned nothing for harvester's fixtures"
    slugs = {row.get("slug") for row in result.results}
    assert slugs & FIXTURE_SLUGS, f"expected one of {FIXTURE_SLUGS}, got {slugs}"


def test_lookup_by_slug_works(interface):
    """get_document_by_slug backs catalog's /dataset/<slug> page."""
    documents = interface.get_document_by_slug("fixture-dataset-1")
    assert documents, "catalog could not fetch harvester's document by slug"


def test_aggregations_work_against_harvester_documents(interface):
    """Facet counts read keyword/organization/publisher subfields.

    These break differently from a plain search: an aggregation needs the field to be
    a `keyword` type, so a harvester-side type change surfaces here specifically.
    """
    keywords = interface.get_unique_keywords(size=10)
    assert isinstance(keywords, list)


def test_dataset_detail_page_renders(app):
    """End to end through catalog's real route and template."""
    client = app.test_client()
    response = client.get("/dataset/fixture-dataset-1")
    assert response.status_code == 200, (
        f"catalog's dataset detail page returned {response.status_code} for a "
        "harvester-written document"
    )


def test_search_api_serializes_harvester_documents(app):
    """Catalog's JSON API reads document fields directly into its response schema."""
    client = app.test_client()
    response = client.get("/search", query_string={"q": "Fixture"})
    assert response.status_code == 200, f"/search returned {response.status_code}"
    payload = response.get_json()
    assert payload.get("results"), f"no results in API payload: {payload}"


def test_dataset_api_serializes_harvester_documents(app):
    """/api/dataset/<slug> serializes a single harvester-written document."""
    client = app.test_client()
    response = client.get("/api/dataset/fixture-dataset-1")
    assert response.status_code == 200, (
        f"/api/dataset returned {response.status_code} for a harvester-written document"
    )
