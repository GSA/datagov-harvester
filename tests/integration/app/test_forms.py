class TestFormStringStripping:
    def test_add_organization_strips_string_fields(self, app, client, interface):
        app.config.update({"WTF_CSRF_ENABLED": False})
        with client.session_transaction() as sess:
            sess["user"] = "tester@gsa.gov"

        data = {
            "name": "  Test Org  ",
            "logo": "  https://example.com/logo.png  ",
        }
        res = client.post("/organization/add", data=data)

        assert res.status_code == 302
        orgs = interface.get_all_organizations()
        assert len(orgs) == 1
        org = orgs[0]
        assert org.name == "Test Org"
        assert org.logo == "https://example.com/logo.png"

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

