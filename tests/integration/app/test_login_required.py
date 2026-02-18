from tests.utils.test_decorators import force_login


class TestLogin:
    # Logged in user can see protected page
    @force_login(email="test@data.gov")
    def test_harvest_edit__logged_in(
        self, client, interface_no_jobs, source_data_dcatus
    ):
        res = client.get(f"/harvest_source/edit/{source_data_dcatus['id']}")
        assert res.status_code == 200

    # Logged out user cannot see protected page
    def test_harvest_edit__logged_out(
        self, client, interface_no_jobs, source_data_dcatus
    ):
        res = client.get(f"/harvest_source/edit/{source_data_dcatus['id']}")
        # ruff: noqa: E501
        redirect_str = 'You should be redirected automatically to the target URL: <a href="/login">/login</a>'
        assert res.status_code == 302
        assert res.text.find(redirect_str) != -1

    @force_login(email="test@data.gov")
    def test_harvest_edit_bad_source_url(self, client, interface_no_jobs):
        """Logged in user gets a not found error from bad url."""
        res = client.get("/harvest_source/edit/1234")
        # ruff: noqa: E501
        assert res.status_code == 404
        assert "Not Found" in res.text

    # Logged in user can see the organization action buttons
    @force_login(email="test@data.gov")
    def test_org_edit_buttons__logged_in(
        self,
        client,
        interface_no_jobs,
        organization_data,
    ):
        res = client.get(f"/organization/{organization_data['id']}")
        # ruff: noqa: E501
        button_string_text = '<div class="config-actions organization-config-actions">'
        org_edit_text = '<input class="usa-button" id="edit" name="edit" type="submit" value="Edit">'
        org_delete_text = '<input class="usa-button usa-button--secondary confirm-submit" data-action="delete" id="delete" name="delete" type="submit" value="Delete">'
        assert res.status_code == 200
        assert res.text.find(button_string_text) != -1
        assert res.text.find(org_edit_text) != -1
        assert res.text.find(org_delete_text) != -1

    # Logged out user cannot see the organization action buttons
    def test_org_edit_buttons__logged_out(
        self, client, interface_no_jobs, organization_data
    ):
        res = client.get(f"/organization/{organization_data['id']}")
        button_string_text = '<div class="config-actions">'
        org_edit_text = '<input class="usa-button" id="edit" name="edit" type="submit" value="Edit">'
        org_delete_text = '<input class="usa-button usa-button--secondary confirm-submit" data-action="delete" id="delete" name="delete" type="submit" value="Delete">'
        assert res.status_code == 200
        assert res.text.find(button_string_text) == -1
        assert res.text.find(org_edit_text) == -1
        assert res.text.find(org_delete_text) == -1

    # Logged in user can see the harvest source action buttons
    @force_login(email="test@data.gov")
    def test_harvest_data_edit_buttons__logged_in(
        self, client, interface_no_jobs, source_data_dcatus
    ):
        res = client.get(f"/harvest_source/{source_data_dcatus['id']}")
        button_string_text = (
            '<div class="config-actions harvest-source-config-actions">'
        )
        source_edit_text = '<input class="usa-button" id="edit" name="edit" type="submit" value="Edit">'
        source_harvest_text = '<input class="usa-button usa-button--base" id="harvest" name="harvest" type="submit" value="Harvest">'
        source_clear_text = ' <input class="usa-button usa-button--accent-cool confirm-submit" data-action="cleart" id="clear" name="clear" type="submit" value="Clear">'
        source_delete_text = '<input class="usa-button usa-button--secondary confirm-submit" data-action="delete" id="delete" name="delete" type="submit" value="Delete">'
        assert res.status_code == 200
        assert res.text.find(button_string_text) != -1
        assert res.text.find(source_edit_text) != -1
        assert res.text.find(source_harvest_text) != -1
        assert res.text.find(source_clear_text) != -1
        assert res.text.find(source_delete_text) != -1

    # Logged out user cannot see the harvest source action buttons
    def test_harvest_data_edit_buttons__logged_out(
        self, client, interface_no_jobs, source_data_dcatus
    ):
        res = client.get(f"/harvest_source/{source_data_dcatus['id']}")
        button_string_text = (
            '<div class="config-actions harvest-source-config-actions">'
        )
        source_edit_text = '<input class="usa-button" id="edit" name="edit" type="submit" value="Edit">'
        source_harvest_text = '<input class="usa-button usa-button--base" id="harvest" name="harvest" type="submit" value="Harvest">'
        source_clear_text = ' <input class="usa-button usa-button--accent-cool confirm-submit" data-action="clear" id="clear" name="clear" type="submit" value="Clear">'
        source_delete_text = '<input class="usa-button usa-button--secondary confirm-submit" data-action="delete" id="delete" name="delete" type="submit" value="Delete">'
        assert res.status_code == 200
        assert res.text.find(button_string_text) == -1
        assert res.text.find(source_edit_text) == -1
        assert res.text.find(source_harvest_text) == -1
        assert res.text.find(source_clear_text) == -1
        assert res.text.find(source_delete_text) == -1
