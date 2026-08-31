import pytest

from harvester.utils.codejson_mapper import (
    codejson_release_to_dcat,
    format_email,
    get_bureau_code_for_agency,
)


class TestCodejsonReleaseToDcat:
    """Test transformation from code.json release to DCAT-US 3.0"""

    def test_basic_mapping_with_all_required_fields(self):
        """Test transformation with all required fields populated"""
        release = {
            "name": "test-project",
            "repositoryURL": "https://github.com/agency/test-project",
            "description": "Test project description",
            "permissions": {
                "licenses": [
                    {"name": "MIT", "URL": "https://opensource.org/licenses/MIT"}
                ],
                "usageType": "openSource",
            },
            "tags": ["python", "testing"],
            "contact": {"email": "dev@agency.gov", "URL": "https://github.com/agency"},
        }

        dcat = codejson_release_to_dcat(release, "TESTAG", "org-123")

        # Basic fields
        assert dcat["@type"] == "dcat:Dataset"
        assert dcat["title"] == "test-project"
        assert dcat["identifier"] == "https://github.com/agency/test-project"
        assert dcat["description"] == "Test project description"
        assert dcat["accessLevel"] == "public"

        # Keywords
        assert "python" in dcat["keyword"]
        assert "testing" in dcat["keyword"]

        # Contact
        assert dcat["contactPoint"]["@type"] == "vcard:Contact"
        assert dcat["contactPoint"]["hasEmail"] == "mailto:dev@agency.gov"
        assert dcat["contactPoint"]["fn"] == "https://github.com/agency"

        # Publisher
        assert dcat["publisher"]["@type"] == "org:Organization"
        assert dcat["publisher"]["name"] == "TESTAG"

        # License
        assert dcat["license"] == "https://opensource.org/licenses/MIT"

        # Landing page
        assert dcat["landingPage"] == "https://github.com/agency/test-project"

        # Distribution
        assert len(dcat["distribution"]) == 1
        assert (
            dcat["distribution"][0]["accessURL"]
            == "https://github.com/agency/test-project"
        )
        assert dcat["distribution"][0]["title"] == "test-project Repository"

    def test_missing_optional_fields_uses_defaults(self):
        """Test that mapper handles missing optional fields gracefully"""
        release = {
            "name": "minimal-project",
            "repositoryURL": "https://github.com/agency/minimal",
            "description": "Minimal project",
            "permissions": {"usageType": "openSource"},
        }

        dcat = codejson_release_to_dcat(release, "TESTAG", "org-123")

        assert dcat["title"] == "minimal-project"
        assert dcat["keyword"] == []
        assert dcat["theme"] == []
        assert "distribution" in dcat
        assert len(dcat["distribution"]) == 1

    def test_email_formatting_adds_mailto_prefix(self):
        """Test that email addresses get mailto: prefix"""
        release = {
            "name": "test",
            "repositoryURL": "https://github.com/agency/test",
            "description": "Test",
            "permissions": {"usageType": "openSource"},
            "contact": {"email": "test@agency.gov"},
        }

        dcat = codejson_release_to_dcat(release, "TESTAG", "org-123")

        assert dcat["contactPoint"]["hasEmail"] == "mailto:test@agency.gov"

    def test_email_with_existing_mailto_not_duplicated(self):
        """Test that mailto: prefix is not duplicated"""
        release = {
            "name": "test",
            "repositoryURL": "https://github.com/agency/test",
            "description": "Test",
            "permissions": {"usageType": "openSource"},
            "contact": {"email": "mailto:test@agency.gov"},
        }

        dcat = codejson_release_to_dcat(release, "TESTAG", "org-123")

        assert dcat["contactPoint"]["hasEmail"] == "mailto:test@agency.gov"
        assert dcat["contactPoint"]["hasEmail"].count("mailto:") == 1

    def test_languages_mapped_to_theme(self):
        """Test that programming languages are mapped to theme field"""
        release = {
            "name": "test",
            "repositoryURL": "https://github.com/agency/test",
            "description": "Test",
            "permissions": {"usageType": "openSource"},
            "languages": ["Python", "JavaScript", "Shell"],
        }

        dcat = codejson_release_to_dcat(release, "TESTAG", "org-123")

        assert dcat["theme"] == ["Python", "JavaScript", "Shell"]

    def test_homepage_url_preferred_over_repository_url(self):
        """Test that homepageURL is used as landing page when available"""
        release = {
            "name": "test",
            "repositoryURL": "https://github.com/agency/test.git",
            "homepageURL": "https://agency.gov/projects/test",
            "description": "Test",
            "permissions": {"usageType": "openSource"},
        }

        dcat = codejson_release_to_dcat(release, "TESTAG", "org-123")

        assert dcat["landingPage"] == "https://agency.gov/projects/test"

    def test_download_url_added_to_distribution(self):
        """Test that downloadURL is included in distribution when present"""
        release = {
            "name": "test",
            "repositoryURL": "https://github.com/agency/test",
            "downloadURL": "https://github.com/agency/test/archive/main.zip",
            "description": "Test",
            "permissions": {"usageType": "openSource"},
        }

        dcat = codejson_release_to_dcat(release, "TESTAG", "org-123")

        assert (
            dcat["distribution"][0]["downloadURL"]
            == "https://github.com/agency/test/archive/main.zip"
        )

    def test_dates_mapped_correctly(self):
        """Test that created and modified dates are mapped"""
        release = {
            "name": "test",
            "repositoryURL": "https://github.com/agency/test",
            "description": "Test",
            "permissions": {"usageType": "openSource"},
            "date": {"created": "2020-01-15", "lastModified": "2026-08-15"},
        }

        dcat = codejson_release_to_dcat(release, "TESTAG", "org-123")

        assert dcat["issued"] == "2020-01-15"
        assert dcat["modified"] == "2026-08-15"

    def test_organization_used_as_publisher_when_present(self):
        """Test that organization field becomes publisher name"""
        release = {
            "name": "test",
            "repositoryURL": "https://github.com/agency/test",
            "description": "Test",
            "permissions": {"usageType": "openSource"},
            "organization": "Sub-Agency Division",
        }

        dcat = codejson_release_to_dcat(release, "DHS", "org-123")

        assert dcat["publisher"]["name"] == "Sub-Agency Division"
        assert dcat["publisher"]["subOrganizationOf"]["name"] == "DHS"

    def test_codejson_specific_fields_preserved(self):
        """Test that code.json-specific fields are preserved in metadata"""
        release = {
            "name": "test",
            "repositoryURL": "https://github.com/agency/test",
            "description": "Test",
            "permissions": {"usageType": "openSource"},
            "status": "Production",
            "laborHours": 2008.5,
            "vcs": "git",
        }

        dcat = codejson_release_to_dcat(release, "TESTAG", "org-123")

        assert dcat["codejson"]["status"] == "Production"
        assert dcat["codejson"]["laborHours"] == 2008.5
        assert dcat["codejson"]["vcs"] == "git"
        assert dcat["codejson"]["usageType"] == "openSource"

    def test_license_name_used_when_no_url(self):
        """Test that license name is used when URL is not provided"""
        release = {
            "name": "test",
            "repositoryURL": "https://github.com/agency/test",
            "description": "Test",
            "permissions": {"licenses": [{"name": "MIT"}], "usageType": "openSource"},
        }

        dcat = codejson_release_to_dcat(release, "TESTAG", "org-123")

        assert dcat["license"] == "MIT"

    def test_missing_contact_uses_default(self):
        """Test that missing contact info uses default values"""
        release = {
            "name": "test",
            "repositoryURL": "https://github.com/agency/test",
            "description": "Test",
            "permissions": {"usageType": "openSource"},
        }

        dcat = codejson_release_to_dcat(release, "TESTAG", "org-123")

        assert dcat["contactPoint"]["fn"] == "Unknown"
        assert dcat["contactPoint"]["hasEmail"] is None

    def test_vcs_used_as_distribution_format(self):
        """Test that version control system is used as distribution format"""
        release = {
            "name": "test",
            "repositoryURL": "https://github.com/agency/test",
            "description": "Test",
            "permissions": {"usageType": "openSource"},
            "vcs": "svn",
        }

        dcat = codejson_release_to_dcat(release, "TESTAG", "org-123")

        assert dcat["distribution"][0]["format"] == "svn"

    def test_bureau_code_added_for_known_agency(self):
        """Test that bureau code is added for known federal agencies"""
        release = {
            "name": "test",
            "repositoryURL": "https://github.com/dhs/test",
            "description": "Test",
            "permissions": {"usageType": "openSource"},
        }

        dcat = codejson_release_to_dcat(release, "DHS", "org-123")

        assert "bureauCode" in dcat
        assert dcat["bureauCode"] == ["070"]

    def test_bureau_code_omitted_for_unknown_agency(self):
        """Test that bureau code is omitted when agency not in mapping"""
        release = {
            "name": "test",
            "repositoryURL": "https://github.com/unknown/test",
            "description": "Test",
            "permissions": {"usageType": "openSource"},
        }

        dcat = codejson_release_to_dcat(release, "UNKNOWNAGENCY", "org-123")

        assert "bureauCode" not in dcat


class TestFormatEmail:
    """Test email formatting helper"""

    def test_adds_mailto_prefix(self):
        assert format_email("test@example.com") == "mailto:test@example.com"

    def test_preserves_existing_mailto(self):
        assert format_email("mailto:test@example.com") == "mailto:test@example.com"

    def test_returns_none_for_empty_string(self):
        assert format_email("") is None

    def test_returns_none_for_none(self):
        assert format_email(None) is None


class TestGetBureauCodeForAgency:
    """Test bureau code mapping"""

    def test_returns_none_for_unknown_agency(self):
        assert get_bureau_code_for_agency("UNKNOWNAGENCY") is None

    def test_returns_bureau_code_for_dhs(self):
        assert get_bureau_code_for_agency("DHS") == "070"

    def test_returns_bureau_code_for_nasa(self):
        assert get_bureau_code_for_agency("NASA") == "026"

    def test_returns_bureau_code_for_gsa(self):
        assert get_bureau_code_for_agency("GSA") == "023"

    def test_returns_bureau_code_for_epa(self):
        assert get_bureau_code_for_agency("EPA") == "020"

    def test_case_insensitive_lookup(self):
        # Should work with lowercase
        assert get_bureau_code_for_agency("dhs") == "070"
        assert get_bureau_code_for_agency("Nasa") == "026"
