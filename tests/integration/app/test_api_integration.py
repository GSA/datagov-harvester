def test_harvest_jobs_collection_response_structure(client, interface):
    """Test harvest jobs endpoint returns structured response."""
    org = interface.add_organization(
        {
            "name": "Test Org",
            "organization_type": "Federal Government",
            "slug": "test-org",
        }
    )
    source = interface.add_harvest_source(
        {
            "name": "Test Source",
            "organization_id": org.id,
            "notification_emails": [],
            "frequency": "manual",
            "url": "http://example.com",
            "schema_type": "dcatus1.1: federal",
            "source_type": "document",
        }
    )
    interface.add_harvest_job({"harvest_source_id": source.id, "status": "complete"})
    interface.add_harvest_job({"harvest_source_id": source.id, "status": "complete"})
    interface.session.commit()

    response = client.get("/api/v1/harvest_jobs/")

    assert response.status_code == 200
    data = response.get_json()

    assert "data" in data
    assert "meta" in data
    assert "links" in data
    assert isinstance(data["data"], list)
    assert len(data["data"]) == 2


def test_harvest_jobs_collection_meta_structure(client, interface):
    """Test meta object contains all required fields."""
    org = interface.add_organization(
        {
            "name": "Test Org",
            "organization_type": "Federal Government",
            "slug": "test-org",
        }
    )
    source = interface.add_harvest_source(
        {
            "name": "Test Source",
            "organization_id": org.id,
            "notification_emails": [],
            "frequency": "manual",
            "url": "http://example.com",
            "schema_type": "dcatus1.1: federal",
            "source_type": "document",
        }
    )
    for i in range(15):
        interface.add_harvest_job(
            {"harvest_source_id": source.id, "status": "complete"}
        )
    interface.session.commit()

    response = client.get("/api/v1/harvest_jobs/?page=0&per_page=10")

    assert response.status_code == 200
    data = response.get_json()

    assert "page" in data["meta"]
    assert "per_page" in data["meta"]
    assert "total" in data["meta"]
    assert "page_count" in data["meta"]
    assert "order_by" in data["meta"]
    assert "api_version" in data["meta"]
    assert "application_version" in data["meta"]

    assert data["meta"]["page"] == 0
    assert data["meta"]["per_page"] == 10
    assert data["meta"]["total"] == 15
    assert data["meta"]["page_count"] == 2


def test_harvest_jobs_collection_links_structure(client, interface):
    """Test links object contains all required fields."""
    org = interface.add_organization(
        {
            "name": "Test Org",
            "organization_type": "Federal Government",
            "slug": "test-org",
        }
    )
    source = interface.add_harvest_source(
        {
            "name": "Test Source",
            "organization_id": org.id,
            "notification_emails": [],
            "frequency": "manual",
            "url": "http://example.com",
            "schema_type": "dcatus1.1: federal",
            "source_type": "document",
        }
    )
    for i in range(25):
        interface.add_harvest_job(
            {"harvest_source_id": source.id, "status": "complete"}
        )
    interface.session.commit()

    response = client.get("/api/v1/harvest_jobs/?page=1&per_page=10")

    assert response.status_code == 200
    data = response.get_json()

    assert "self" in data["links"]
    assert "next" in data["links"]
    assert "previous" in data["links"]
    assert "documentation" in data["links"]
    assert "changelog" in data["links"]

    assert "page=1" in data["links"]["self"]
    assert data["links"]["next"] is not None
    assert data["links"]["previous"] is not None


def test_organizations_collection_response_structure(client):
    """Test organizations endpoint returns structured response."""
    response = client.get("/api/v1/organizations/")

    assert response.status_code == 200
    data = response.get_json()

    assert "data" in data
    assert "meta" in data
    assert "links" in data


def test_harvest_sources_collection_response_structure(client):
    """Test harvest sources endpoint returns structured response."""
    response = client.get("/api/v1/harvest_sources/")

    assert response.status_code == 200
    data = response.get_json()

    assert "data" in data
    assert "meta" in data
    assert "links" in data


def test_harvest_records_collection_response_structure(client):
    """Test harvest records endpoint returns structured response."""
    response = client.get("/api/v1/harvest_records/")

    assert response.status_code == 200
    data = response.get_json()

    assert "data" in data
    assert "meta" in data
    assert "links" in data


def test_harvest_job_errors_collection_response_structure(client):
    """Test harvest job errors endpoint returns structured response."""
    response = client.get("/api/v1/harvest_job_errors/")

    assert response.status_code == 200
    data = response.get_json()

    assert "data" in data
    assert "meta" in data
    assert "links" in data


def test_harvest_record_errors_collection_response_structure(client):
    """Test harvest record errors endpoint returns structured response."""
    response = client.get("/api/v1/harvest_record_errors/")

    assert response.status_code == 200
    data = response.get_json()

    assert "data" in data
    assert "meta" in data
    assert "links" in data


def test_collection_response_with_facets(client, interface):
    """Test collection response structure preserved with facets."""
    org = interface.add_organization(
        {"name": "Test Org", "organization_type": "Federal Government"}
    )
    source = interface.add_harvest_source(
        {
            "name": "Test Source",
            "organization_id": org.id,
            "notification_emails": [],
            "frequency": "manual",
            "url": "http://example.com",
            "schema_type": "dcatus1.1: federal",
            "source_type": "document",
        }
    )
    interface.add_harvest_job({"harvest_source_id": source.id, "status": "complete"})
    interface.add_harvest_job({"harvest_source_id": source.id, "status": "error"})
    interface.session.commit()

    response = client.get("/api/v1/harvest_jobs/?facets=status eq complete")

    assert response.status_code == 200
    data = response.get_json()

    assert "data" in data
    assert len(data["data"]) == 1
    assert data["meta"]["total"] == 1
