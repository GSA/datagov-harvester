import os
import re


class TestDynamicRouteTable:
    """
    NOTE: Making these explicit opt-in/whitelisted so they
    fail automatically on the addition of any new routes
    """

    def test_all_routes(
        self,
        client,
        interface_with_fixture_json,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        record_data_dcatus,
        record_error_data,
    ):
        # dont test flask internal or auth routes
        whitelisted_routes = [
            "static",
            "bootstrap.static",
            "main.login",
            "main.logout",
            "main.callback",
        ]
        # provide a simple map for MOST other routes
        simple_assertion_map = {"200": None, "302": "/login"}
        # provide a special assertions map for routes which do something special
        special_assertion_map = {
            "main.index": {"status_code": 302, "location": "/organizations/"},
        }
        for route in client.application.url_map.iter_rules():
            if route.endpoint not in whitelisted_routes:
                # replace arg values with real data
                replacements = [
                    ("<org_id>", organization_data["id"]),
                    ("<source_id>", source_data_dcatus["id"]),
                    ("<job_id>", job_data_dcatus["id"]),
                    ("<error_type>", "record"),
                    ("<record_id>", record_data_dcatus[0]["id"]),
                    ("<error_id>", record_error_data[0]["id"]),
                ]
                cleaned_route_rule = route.rule
                for old, new in replacements:
                    cleaned_route_rule = re.sub(old, new, cleaned_route_rule)

                res = client.get(cleaned_route_rule)

                # test special assertions first
                if route.endpoint in special_assertion_map:
                    for key, val in special_assertion_map[route.endpoint].items():
                        assert getattr(res, key) == val
                else:
                    try:
                        # test ALL other non-whitelisted routes
                        assert (
                            simple_assertion_map[str(getattr(res, "status_code"))]
                            == res.location
                        )
                    except Exception as e:
                        raise Exception(
                            f"{route.endpoint} fails to get {cleaned_route_rule} for {repr(e)} reason"
                        )

    def test_bad_id_get_responses(self, client):
        # ignore routes which aren't public GETS and don't accept args
        whitelisted_routes = [
            "static",
            "bootstrap.static",
            "main.login",
            "main.logout",
            "main.callback",
            "main.index",
            "main.add_organization",
            "main.edit_organization",
            "main.view_organizations",
            "main.add_harvest_source",
            "main.edit_harvest_source",
            "main.view_harvest_sources",
            "main.add_harvest_job",
            "main.update_harvest_job",
            "main.delete_harvest_job",
            "main.cancel_harvest_job",
            "main.add_harvest_record",
            "main.get_harvest_records",
            "main.view_metrics",
            "api.delete_organization",
            "api.delete_harvest_source",
            "api.trigger_harvest_source",
        ]

        # some endpoints respond with JSON
        json_responses_map = {
            "main.get_harvest_record": "Not Found",
            "main.get_harvest_record_raw": '{"error":"Not Found"}\n',
            "main.get_all_harvest_record_errors": "Please provide a valid record_id",
            "main.get_harvest_error": "Not Found",
        }
        # some respond with a template
        templated_responses_map = {
            "main.view_organization": "Looks like you navigated to an organization that doesn't exist",
            "main.view_harvest_source": "Looks like you navigated to a source that doesn't exist",
            "main.view_harvest_job": "Looks like you navigated to a job that doesn't exist",
            "main.download_harvest_errors_by_job": "Please provide correct job_id",
        }
        for route in client.application.url_map.iter_rules():
            if route.endpoint not in whitelisted_routes:
                cleaned_route_rule = re.sub("(\<.*?\>)", "1234", route.rule)

                res = client.get(cleaned_route_rule)
                if route.endpoint in json_responses_map:
                    try:
                        # assert against a decoded byte-string
                        assert res.data.decode() == json_responses_map[route.endpoint]
                    except Exception as e:
                        raise Exception(
                            f"{route.endpoint} fails to map {res.data.decode()} to json_responses_map"
                        )
                else:
                    try:
                        # assert against a substring in the byte-string response
                        assert (
                            templated_responses_map[route.endpoint].encode() in res.data
                        )
                    except Exception as e:
                        raise Exception(
                            f"{route.endpoint} fails to map {templated_responses_map[route.endpoint]} substring to response"
                        )


class TestLoginAuthHeaders:
    def test_login_required_no_token(self, client):
        headers = {"Content-Type": "application/json"}
        data = {"name": "Test Org", "logo": "test_logo.png"}
        response = client.get("/organization/add", json=data, headers=headers)
        assert response.status_code == 401
        assert response.data.decode() == "error: Authorization header missing"

    def test_login_required_valid_token(self, client):
        api_token = os.getenv("FLASK_APP_SECRET_KEY")
        headers = {
            "Authorization": api_token,
            "Content-Type": "application/json",
        }
        data = {"name": "Test Org", "logo": "test_logo.png"}
        response = client.get("/organization/add", json=data, headers=headers)
        assert response.status_code == 200

    def test_login_required_invalid_token(self, client):
        headers = {
            "Authorization": "invalid_token",
            "Content-Type": "application/json",
        }
        data = {"name": "Test Org", "logo": "test_logo.png"}
        response = client.get("/organization/add", json=data, headers=headers)
        assert response.status_code == 401
        assert response.data.decode() == "error: Unauthorized"
