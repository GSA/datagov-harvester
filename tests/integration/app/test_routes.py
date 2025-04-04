import os
import re

# ruff: noqa: F401

LOCATION_ENUMS = {
    "LOGIN": "/login",
    "NONE": None,
}


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
        # provide a special assertions regex map for routes which do something special
        special_assertion_map = {
            "(main\.index)": {
                "(GET|HEAD)": {
                    "status_code": 302,
                    "location": "/organizations/",
                },
            },
            "((main|api)\.(add|edit|cancel|update|delete|trigger)_(organization|harvest_source|harvest_job|harvest_record))": {
                "(POST|HEAD|PUT|DELETE)": {
                    "status_code": 302,
                    "location": LOCATION_ENUMS["LOGIN"],
                }
            },
        }
        # provide a simple map for MOST other routes
        default_assertion_map = {
            "GET": {"200": LOCATION_ENUMS["NONE"], "302": LOCATION_ENUMS["LOGIN"]},
            "POST": {"302": "<cleaned_route_rule>"},
            "OPTIONS": {"200": LOCATION_ENUMS["NONE"]},
            "HEAD": {"200": LOCATION_ENUMS["NONE"]},
        }
        for route in client.application.url_map.iter_rules():
            if route.endpoint in whitelisted_routes:
                continue

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

            for method in route.methods:
                client_method = getattr(client, method.lower(), None)
                res = client_method(cleaned_route_rule)

                # check if route.endpoint matches any regex in special_assertion_map
                re_match_route_list = list(
                    filter(lambda x: re.match(x, route.endpoint), special_assertion_map)
                )

                # throw exception if route endpoint matches more than one regex in special_assertion_map
                if len(re_match_route_list) > 1:
                    raise Exception(
                        f"Regex error: more than one match for {route.endpoint} :: {re_match_route_list}"
                    )

                # check our method regex next if we have a match to the route endpoint
                if len(re_match_route_list):
                    re_match_method_list = list(
                        filter(
                            lambda x: re.match(x, method),
                            special_assertion_map[re_match_route_list[0]],
                        )
                    )

                # if route matches a regex in special_assertion_map
                if len(re_match_route_list) and len(re_match_method_list):
                    for key, val in special_assertion_map[re_match_route_list[0]][
                        re_match_method_list[0]
                    ].items():
                        try:
                            assert getattr(res, key) == val
                        except Exception as e:
                            raise Exception(
                                f"{re_match_route_list[0]} fails {re_match_method_list[0]} on {cleaned_route_rule} \
                                        for {repr(e)} reason"
                            )

                # test other non-whitelisted routes
                else:
                    if not default_assertion_map.get(method):
                        continue  # throw away METHODS we don't explicitly test for
                    try:
                        # replace res assertion values with real data
                        replacements = [
                            ("<cleaned_route_rule>", cleaned_route_rule),
                        ]

                        expected_location = default_assertion_map[method][
                            str(getattr(res, "status_code"))
                        ]

                        if expected_location is None:
                            assert expected_location == res.location
                            continue

                        for old, new in replacements:
                            expected_location = re.sub(old, new, expected_location)

                        assert expected_location == res.location
                    except Exception as e:
                        raise Exception(
                            f"{route.endpoint} fails {method} on {cleaned_route_rule} \
                                for {repr(e)} reason"
                        )

    def test_client_response_on_error(self, client):
        # ignore routes which aren't public GETS and don't accept args
        whitelisted_route_regex = "((main|api|bootstrap)?(?:\.)?(add|edit|cancel|update|delete|trigger|view)?(?:_)?(static|index|callback|get_harvest_records|view_metrics|log(in|out)|organization(?:s)?|harvest_source|harvest_job|harvest_record))"

        # some endpoints respond with JSON
        json_responses_map = {
            "main.get_harvest_record": "Not Found",
            "main.get_harvest_record_raw": '{"error":"Not Found"}\n',
            "main.get_all_harvest_record_errors": "Not Found",
            "main.get_harvest_error": "Not Found",
        }
        # some respond with a template
        # ruff: noqa: E501
        templated_responses_map = {
            "main.view_organization": {
                "GET": "Looks like you navigated to an organization that doesn't exist",
                "POST": 'You should be redirected automatically to the target URL: <a href="/organization/1234">/organization/1234</a>',
            },
            "main.view_harvest_source": {
                "GET": "Looks like you navigated to a harvest source that doesn't exist",
                "POST": 'You should be redirected automatically to the target URL: <a href="/harvest_source/1234">/harvest_source/1234</a>',
            },
            "main.view_harvest_job": {
                "GET": "Looks like you navigated to a harvest job that doesn't exist"
            },
            "main.download_harvest_errors_by_job": {
                "GET": "Please provide correct job_id"
            },
        }
        for route in client.application.url_map.iter_rules():
            if re.match(whitelisted_route_regex, route.endpoint):
                continue

            cleaned_route_rule = re.sub("(\<.*?\>)", "1234", route.rule)

            for method in route.methods:
                if method in ["HEAD", "OPTIONS"]:
                    continue  # we get no response body from these methods, so skip them
                client_method = getattr(client, method.lower(), None)
                res = client_method(cleaned_route_rule)

                if route.endpoint in json_responses_map:
                    try:
                        # assert against a decoded byte-string
                        assert res.data.decode() == json_responses_map[route.endpoint]
                    except Exception:
                        raise Exception(
                            f"{route.endpoint} fails to map {res.data.decode()} to json_responses_map"
                        )
                else:
                    try:
                        # assert against a substring in the byte-string response
                        assert (
                            templated_responses_map[route.endpoint][method].encode()
                            in res.data
                        )
                    except Exception:
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
