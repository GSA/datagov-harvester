class TestForms:
    def test_add_organization_strips_string_fields(self, app, client, interface):
        app.config.update({"WTF_CSRF_ENABLED": False})
        with client.session_transaction() as sess:
            sess["user"] = "tester@gsa.gov"

        data = {
            "name": "  Test Org  ",
            "logo": "  https://example.com/logo.png  ",
            "description": "  A sample description  ",
            "slug": "  test-slug  ",
        }
        res = client.post("/organization/add", data=data)

        assert res.status_code == 302
        orgs = interface.get_all_organizations()
        assert len(orgs) == 1
        org = orgs[0]
        assert org.name == "Test Org"
        assert org.logo == "https://example.com/logo.png"
        assert org.description == "A sample description"
        assert org.slug == "test-slug"

    def test_add_organization_aliases(self, app, client, interface):
        app.config.update({"WTF_CSRF_ENABLED": False})
        with client.session_transaction() as sess:
            sess["user"] = "tester@gsa.gov"

        data = {
            "name": "Test Org",
            "logo": "https://example.com/logo.png  ",
            "description": "description  ",
            "slug": "test",
            "aliases": "first, second",
        }
        res = client.post("/organization/add", data=data)

        assert res.status_code == 302
        orgs = interface.get_all_organizations()
        org = orgs[0]
        assert len(org.aliases) == 2
        assert not org.aliases[-1].startswith(" ")

    def test_add_organization_duplicate_slug_validation(self, app, client, interface):
        app.config.update({"WTF_CSRF_ENABLED": False})
        with client.session_transaction() as sess:
            sess["user"] = "tester@gsa.gov"

        existing_org = {
            "name": "Existing Org",
            "logo": "https://example.com/existing.png",
            "description": "Existing",
            "slug": "duplicate-slug",
        }

        interface.add_organization(existing_org)

        data = {
            "name": "New Org",
            "logo": "https://example.com/logo.png",
            "description": "Another Org",
            "slug": "duplicate-slug",
            "organization_type": "",
        }

        res = client.post("/organization/add", data=data, follow_redirects=False)

        assert res.status_code == 302

        follow = client.get(res.headers["Location"], follow_redirects=True)
        assert follow.status_code == 200
        print(follow.data.decode())
        assert b"Slug must be unique." in follow.data

        orgs = interface.get_all_organizations()
        assert len(orgs) == 1
        assert orgs[0].slug == "duplicate-slug"

    def test_add_harvest_source_strips_string_fields(
        self, app, client, interface, organization_data
    ):
        app.config.update({"WTF_CSRF_ENABLED": False})
        with client.session_transaction() as sess:
            sess["user"] = "tester@gsa.gov"

        interface.add_organization(organization_data)

        form_data = {
            "organization_id": organization_data["id"],
            "name": "  Test Source  ",
            "url": "  https://example.com/datasets.json  ",
            "notification_emails": "  user@example.com  ,  other@example.com   ",
            "frequency": "daily",
            "schema_type": "dcatus1.1: federal",
            "source_type": "document",
            "notification_frequency": "always",
        }
        res = client.post("/harvest_source/add", data=form_data)

        assert res.status_code == 302
        sources = interface.get_all_harvest_sources()
        assert len(sources) == 1
        src = sources[0]
        assert src.name == "Test Source"
        assert src.url == "https://example.com/datasets.json"
        assert src.notification_emails == [
            "user@example.com",
            "other@example.com",
        ]

    def test_edit_organization_updates_all_fields(
        self, app, client, interface, organization_data
    ):
        app.config.update({"WTF_CSRF_ENABLED": False})
        with client.session_transaction() as sess:
            sess["user"] = "tester@gsa.gov"

        interface.add_organization(organization_data)

        edit_response = client.get(f"/organization/edit/{organization_data['id']}")

        assert edit_response.status_code == 200
        assert b'name="name"' in edit_response.data
        assert b'name="logo"' in edit_response.data
        assert b'name="organization_type"' in edit_response.data
        assert b'name="description"' in edit_response.data
        assert b'name="slug"' in edit_response.data

        updated_data = {
            "name": "Updated Org",
            "logo": "https://example.com/newlogo.png",
            "organization_type": "City Government",
            "description": "New description",
            "slug": "updated-slug",
        }

        post_response = client.post(
            f"/organization/edit/{organization_data['id']}",
            data=updated_data,
        )

        assert post_response.status_code == 302

        org = interface.get_organization(organization_data["id"])
        assert org.name == "Updated Org"
        assert org.logo == "https://example.com/newlogo.png"
        assert org.organization_type == "City Government"
        assert org.description == "New description"
        assert org.slug == "updated-slug"
