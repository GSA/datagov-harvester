import json
import os
import re
from unittest.mock import patch

import pytest
from flask import Response

from app.routes import UnsafeTemplateEnvError, render_block
from harvester.harvest import HarvestSource

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
            r"(main\.index)": {
                "(GET|HEAD)": {
                    "status_code": 302,
                    "location": "/organization_list/",
                },
            },
            r"((main|api)\.(add|edit|cancel|update|delete|trigger)_(organization|harvest_source|harvest_job|harvest_record))": {
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

                        expected_location = default_assertion_map[method].get(
                            str(getattr(res, "status_code"))
                        )

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
        whitelisted_route_regex = r"((main|api|bootstrap)?(?:\.)?(add|edit|cancel|update|delete|trigger|view)?(?:_)?(static|index|callback|json_builder_query|view_metrics|log(in|out)|organization(?:s)?|harvest_source|harvest_job|harvest_record))"

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
                "GET": "Invalid error type. Must be 'job' or 'record'"
            },
        }
        for route in client.application.url_map.iter_rules():
            if re.match(whitelisted_route_regex, route.endpoint):
                continue

            cleaned_route_rule = re.sub(r"(\<.*?\>)", "1234", route.rule)

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


class TestJSONResponses:
    def test_get_organization_json(
        self,
        client,
        interface_with_multiple_jobs,
        organization_data,
    ):
        res = client.get(
            f"/organization/{organization_data['id']}",
            headers={"Content-type": "application/json"},
        )
        assert res.status_code == 200
        assert res.is_json

    def test_get_missing_organization(
        self,
        client,
        interface_with_multiple_jobs,
        organization_data,
    ):
        res = client.get(
            f"/organization/{organization_data['id']}-missing",
            headers={"Content-type": "application/json"},
        )
        assert res.status_code == 404
        assert res.is_json

    @pytest.mark.parametrize(
        "route,status_code,response",
        [
            (
                "/harvest_records/?harvest_source_id=2f2652de-91df-4c63-8b53-bfced20b276b",
                200,
                10,
            ),
            (
                "/harvest_records/?harvest_source_id=2f2652de-91df-4c63-8b53-bfced20b276b&facets=status='success'",
                200,
                2,
            ),
            (
                "/harvest_records/?harvest_source_id=2f2652de-91df-4c63-8b53-bfced20b276b&facets=ckan_id='1234'",
                200,
                1,
            ),
            (
                "/harvest_records/?harvest_source_id=2f2652de-91df-4c63-8b53-bfced20b276b&facets=status='success'&count=True",
                200,
                2,
            ),
            (
                "/harvest_records/?harvest_source_id=2f2652de-91df-4c63-8b53-bfced20b276b&facets=status='not_status'",
                400,
                "Error with query",
            ),
            (
                "/organizations/",
                200,
                1,
            ),
            (
                "/harvest_sources/",
                200,
                1,
            ),
            (
                "/harvest_sources/?facets=schema_type='dcatus1.1: non-federal'",
                404,
                "No harvest_sources found for this query",
            ),
        ],
    )
    def test_json_builder_query(
        self, client, interface_with_fixture_json, route, status_code, response
    ):
        """Tests against seeded content in `interface_with_fixture_json`"""
        res = client.get(route)
        assert res.status_code == status_code
        try:
            json_res = json.loads(res.data)
            if "count" in json_res:
                assert json_res["count"] == response
            else:
                assert len(json_res) == response
        except Exception:
            assert res.data.decode() == response


class TestHarvestRecordRawAPI:
    """Test the HarvestRecord API Raw endpoint."""

    def test_xml_harvest_record_raw(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
        job_data_waf_iso19115_2,
        client,
    ):
        """
        Test the Raw endpoint for a harvest record with XML source.
        The expectaion is that the XML data is returned as it appears in the
        source_raw of the record.
        """
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)
        harvest_job = interface.add_harvest_job(job_data_waf_iso19115_2)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_internal_data()
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        iso_records = list(external_records_to_process)
        # Filter for the record with 'valid_iso2' in the identifier
        test_iso_2_record = next(
            (
                record
                for record in iso_records
                if "http://localhost:80/iso_2_waf/valid_iso2.xml" == record.identifier
            ),
            None,
        )

        if test_iso_2_record is None:
            raise ValueError(
                "Could not find record with 'valid_iso2.xml' in identifier"
            )

        test_iso_2_record.compare()

        response = client.get(f"/harvest_record/{test_iso_2_record.id}/raw")

        assert response.status_code == 200
        assert response.text == test_iso_2_record.source_raw

    def test_json_harvest_record_raw(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        client,
    ):
        """
        Test the Raw endpoint for a harvest record with JSON source.
        Expected result should display the JSON data as it appears in the
        source_raw of the record.
        """
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        harvest_job = interface.add_harvest_job(job_data_dcatus)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.acquire_minimum_internal_data()
        harvest_source.acquire_minimum_external_data()
        external_records_to_process = harvest_source.external_records_to_process()

        # first one is always "cftc-dc1"
        test_record = next(external_records_to_process)
        test_record.compare()

        response = client.get(f"/harvest_record/{test_record.id}/raw")

        assert response.status_code == 200
        assert response.json == json.loads(test_record.source_raw)

    @patch("harvester.lib.load_manager.LoadManager")
    def test_cancel_in_progress_job(
        self,
        LMMock,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        client,
    ):
        """
        Tests whether a redirect to /harvest_job/cancel/<job_id> is given on job
        cancellation with a valid job id
        """
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)

        # doesn't affect anything but adding anyways.
        job_data_dcatus["status"] = "in_progress"
        job = interface.add_harvest_job(job_data_dcatus)

        # the value itself doesn't matter. we just want to mock stop_job so it completes.
        LMMock.stop_job.return_value = "a test value"

        headers = {"Authorization": os.getenv("FLASK_APP_SECRET_KEY")}
        response = client.get(f"/harvest_job/cancel/{job.id}", headers=headers)
        assert response.status_code == 302
        assert response.location == f"/harvest_job/{job.id}"


class TestRenderBlock:
    """Test cases for render_block function."""

    def test_autoescape_enabled_allows_rendering(self, app_with_temp_template):
        """
        Test that render_block escapes the content correctly now that it uses the
        flask app's Jinja environment with autoescape enabled.
        """
        with app_with_temp_template.app_context():
            response = render_block(
                "test_template.html",
                "test_block",
                name="World",
                user_input='<script>alert("XSS")</script>',
            )

            assert isinstance(response, Response)
            assert response.status_code == 200
            assert response.mimetype == "text/html"

            html_content = response.get_data(as_text=True)

            # Check that safe content is preserved
            assert "<p>Hello World!</p>" in html_content

            # Check that dangerous content is escaped
            assert '<script>alert("XSS")</script>' not in html_content
            assert "&lt;script&gt;alert(&#34;XSS&#34;)&lt;/script&gt;" in html_content

    def test_autoescape_disabled_raises_exception(
        self, app_with_temp_template, monkeypatch
    ):
        """
        Test that render_block raises UnsafeTemplateEnvError when autoescape is disabled.
        """
        with app_with_temp_template.app_context():
            # Temporarily disable autoescape on the Jinja environment
            monkeypatch.setattr(app_with_temp_template.jinja_env, "autoescape", False)

            with pytest.raises(UnsafeTemplateEnvError) as exc_info:
                render_block(
                    "test_template.html",
                    "test_block",
                    name="World",
                    user_input='<script>alert("XSS")</script>',
                )

            assert "Jinja autoescape is disabled" in str(exc_info.value)
