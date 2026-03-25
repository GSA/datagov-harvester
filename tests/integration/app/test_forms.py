from bs4 import BeautifulSoup


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

    def test_edit_organization_handles_aliases(
        self, app, client, interface, organization_data
    ):
        """aliases form field is formatted correctly"""
        app.config.update({"WTF_CSRF_ENABLED": False})
        with client.session_transaction() as sess:
            sess["user"] = "tester@gsa.gov"

        interface.add_organization(organization_data)

        edit_response = client.get(f"/organization/edit/{organization_data['id']}")
        assert "testorg" in edit_response.text
        assert "['testorg']" not in edit_response.text


class TestDatasetSlugForm:
    """
    Tests for DatasetSlugForm validation and the dataset slug-edit route.
    """

    def _setup_dataset(self, interface, fixtures_json):
        """
        Insert the minimal org/source/job/record/dataset chain into the DB.
        """
        org = fixtures_json["organization"][0]
        source = fixtures_json["source"][0]
        job = fixtures_json["job"][0]
        record = fixtures_json["record"][0]
        dataset = fixtures_json["dataset"][0]

        interface.add_organization(org)
        interface.add_harvest_source(source)
        interface.add_harvest_job(job)
        interface.add_harvest_record(record)
        interface.insert_dataset(dataset)

        return dataset

    def test_get_dataset_page_renders_form(self, app, client, interface, fixtures_json):
        """
        GET /dataset/<slug> renders the page with the slug form field.
        """
        app.config.update({"WTF_CSRF_ENABLED": False})
        dataset = self._setup_dataset(interface, fixtures_json)
        with client.session_transaction() as sess:
            sess["user"] = "tester@gsa.gov"

        res = client.get(f"/dataset/{dataset['slug']}")

        assert res.status_code == 200
        soup = BeautifulSoup(res.data, "html.parser")
        # only one form
        form = soup.find("form")
        assert form is not None
        # only one input
        slug_input = form.find("input")
        assert slug_input is not None
        assert slug_input.attrs.get("name") == "slug"

    def test_edit_slug_success(self, app, client, interface, fixtures_json):
        """
        A valid, unique slug POST updates the dataset and redirects.
        """
        app.config.update({"WTF_CSRF_ENABLED": False})
        with client.session_transaction() as sess:
            sess["user"] = "tester@gsa.gov"

        dataset = self._setup_dataset(interface, fixtures_json)

        res = client.post(
            f"/dataset/{dataset['slug']}",
            data={"slug": "brand-new-slug"},
        )

        assert res.status_code == 302
        assert "brand-new-slug" in res.headers["Location"]

        updated = interface.get_dataset_by_slug("brand-new-slug")
        assert updated is not None
        assert updated.id == dataset["id"]

    def test_edit_slug_strips_whitespace(self, app, client, interface, fixtures_json):
        """
        Whitespace around the submitted slug is stripped before saving.
        """
        app.config.update({"WTF_CSRF_ENABLED": False})
        with client.session_transaction() as sess:
            sess["user"] = "tester@gsa.gov"

        dataset = self._setup_dataset(interface, fixtures_json)

        res = client.post(
            f"/dataset/{dataset['slug']}",
            data={"slug": "  stripped-slug  "},
        )

        assert res.status_code == 302
        updated = interface.get_dataset_by_slug("stripped-slug")
        assert updated is not None

    def test_edit_slug_own_slug_allowed(self, app, client, interface, fixtures_json):
        """
        POSTing the dataset's existing slug is accepted and redirects cleanly.
        """
        app.config.update({"WTF_CSRF_ENABLED": False})
        with client.session_transaction() as sess:
            sess["user"] = "tester@gsa.gov"

        dataset = self._setup_dataset(interface, fixtures_json)
        original_slug = dataset["slug"]

        res = client.post(
            f"/dataset/{original_slug}",
            data={"slug": original_slug},
        )

        assert res.status_code == 302
        assert original_slug in res.headers["Location"]

    def test_edit_slug_duplicate_returns_422(
        self, app, client, interface, fixtures_json
    ):
        """
        A slug already owned by a different dataset returns 422 with an error.
        """
        app.config.update({"WTF_CSRF_ENABLED": False})
        with client.session_transaction() as sess:
            sess["user"] = "tester@gsa.gov"

        self._setup_dataset(interface, fixtures_json)

        # Insert a second dataset with a known slug to create a collision.
        second_dataset = fixtures_json["dataset"][1]
        interface.add_harvest_record(fixtures_json["record"][1])
        interface.insert_dataset(second_dataset)

        res = client.post(
            f"/dataset/{fixtures_json['dataset'][0]['slug']}",
            data={"slug": second_dataset["slug"]},
        )

        assert res.status_code == 422
        assert b"already in use" in res.data

    def test_edit_slug_invalid_chars_returns_422(
        self, app, client, interface, fixtures_json
    ):
        """
        A slug containing invalid characters returns 422 with a validation error.
        """
        app.config.update({"WTF_CSRF_ENABLED": False})
        with client.session_transaction() as sess:
            sess["user"] = "tester@gsa.gov"

        dataset = self._setup_dataset(interface, fixtures_json)

        res = client.post(
            f"/dataset/{dataset['slug']}",
            data={"slug": "Invalid_Slug!"},
        )

        assert res.status_code == 422
        assert b"lowercase" in res.data
