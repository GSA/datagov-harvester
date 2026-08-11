def test_add_organization(interface, organization_data):
    org = interface.add_organization(organization_data)

    assert org is not None
    assert org.name == "Test Org"


def test_get_all_organizations(interface, organization_data):
    interface.add_organization(organization_data)

    orgs = interface.get_all_organizations()
    assert len(orgs) > 0
    assert orgs[0].name == "Test Org"


def test_get_organization_by_slug(interface, organization_data):
    interface.add_organization(organization_data)

    org = interface.get_organization_by_slug(organization_data["slug"])
    assert org is not None
    assert org.id == organization_data["id"]


def test_get_organization_by_alias(interface, organization_data):
    interface.add_organization(organization_data)

    org = interface.get_organization_by_alias(organization_data["aliases"][0])
    assert org is not None
    assert org.id == organization_data["id"]


def test_get_organization_by_alias_not_found(interface, organization_data):
    interface.add_organization(organization_data)

    org = interface.get_organization_by_alias("nonexistent-alias")
    assert org is None


def test_update_organization(interface, organization_data):
    org = interface.add_organization(organization_data)

    updates = {"name": "Updated Org"}
    updated_org = interface.update_organization(org.id, updates)
    assert updated_org.name == "Updated Org"


def test_delete_organization(interface, organization_data):
    org = interface.add_organization(organization_data)

    result = interface.delete_organization(org.id)
    # ruff: noqa: E501
    assert result == (
        "Deleted organization with ID:d925f84d-955b-4cb7-812f-dcfd6681a18f successfully",
        200,
    )
