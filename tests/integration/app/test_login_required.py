from tests.utils.test_decorators import force_login


class TestLogin:
    # Logged in user can see protected page
    @force_login(email="test@data.gov")
    def test_harvest_edit__logged_in(
        self, client, interface_no_jobs, source_data_dcatus
    ):
        res = client.get(f"/harvest_source/config/edit/{source_data_dcatus['id']}")
        assert res.status_code == 200

    # Logged out user cannot see protected page
    def test_harvest_edit__logged_out(
        self, client, interface_no_jobs, source_data_dcatus
    ):
        res = client.get(f"/harvest_source/config/edit/{source_data_dcatus['id']}")
        redirect_str = 'You should be redirected automatically to the target URL: <a href="/login">/login</a>'
        assert res.status_code == 302
        assert res.text.find(redirect_str) != -1

    # Logged in user is redirected away from bad url
    @force_login(email="test@data.gov")
    def test_harvest_edit_bad_source_url(self, client, interface_no_jobs):
        res = client.get("/harvest_source/config/edit/1234")
        redirect_str = 'You should be redirected automatically to the target URL: <a href="/harvest_sources/">/harvest_sources/</a>'
        assert res.status_code == 302
        assert res.text.find(redirect_str) != -1

    # Logged in user can see the organization action buttons
    @force_login(email="test@data.gov")
    def test_org_edit_buttons__logged_in(
        self,
        client,
        interface_with_multiple_jobs,
        source_data_dcatus,
        organization_data,
    ):
        res = client.get(f"/organization/{organization_data['id']}")
        button_string_text = '<div class="config-actions organization-config-actions">'
        org_edit_text = f'<a href="/organization/config/edit/{organization_data["id"]}"'
        org_delete_text = f"onclick=\"confirmDelete('/organization/config/delete/{organization_data['id']}')"
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
        org_edit_text = f'<a href="/organization/config/edit/{organization_data["id"]}"'
        org_delete_text = f"onclick=\"confirmDelete('/organization/config/delete/{organization_data['id']}')"
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
        source_edit_text = (
            f'<a href="/harvest_source/config/edit/{source_data_dcatus["id"]}"'
        )
        source_clear_text = (
            f'<a href="/harvest_source/harvest/{source_data_dcatus["id"]}/clear"'
        )
        source_delete_text = f"onclick=\"confirmAction('delete', '/harvest_source/config/delete/{source_data_dcatus['id']}')"
        assert res.status_code == 200
        assert res.text.find(button_string_text) != -1
        assert res.text.find(source_edit_text) != -1
        assert res.text.find(source_clear_text) != -1
        assert res.text.find(source_delete_text) != -1

    # Logged out user cannot see the harvest source action buttons
    def test_harvest_data_edit_buttons__logged_out(
        self, client, interface_no_jobs, source_data_dcatus
    ):
        res = client.get(f"/harvest_source/{source_data_dcatus['id']}")
        button_string_text = '<div class="config-actions">'
        source_edit_text = (
            f'<a href="/harvest_source/config/edit/{source_data_dcatus["id"]}"'
        )
        source_clear_text = f"onclick=\"confirmAction('clear', '/harvest_source/config/clear/{source_data_dcatus['id']}')"
        source_delete_text = f"onclick=\"confirmAction('delete', '/harvest_source/config/delete/{source_data_dcatus['id']}')"
        assert res.status_code == 200
        assert res.text.find(button_string_text) == -1
        assert res.text.find(source_edit_text) == -1
        assert res.text.find(source_clear_text) == -1
        assert res.text.find(source_delete_text) == -1
