from apiflask import APIBlueprint, APIFlask
from bs4 import BeautifulSoup
from flask import Blueprint

from app import routes as routes_module


def test_api_alias_redirects_to_last_registered_version(monkeypatch):
    """`/api` (unversioned) must redirect to whichever version is LAST in
    API_VERSIONS, so shipping a new version is just appending to that list
    -- no separate step to move the alias. See GSA/data.gov#6236."""
    v1 = APIBlueprint("test_api_v1", __name__)

    @v1.get("/ping")
    def ping_v1():
        return "v1"

    v2 = APIBlueprint("test_api_v2", __name__)

    @v2.get("/ping")
    def ping_v2():
        return "v2"

    monkeypatch.setattr(routes_module, "main", Blueprint("main", __name__))
    monkeypatch.setattr(routes_module, "API_VERSIONS", [("v1", v1), ("v2", v2)])

    app = APIFlask(__name__)
    routes_module.register_routes(app)

    with app.test_client() as client:
        redirect = client.get("/api/ping")
        assert redirect.status_code == 308
        assert redirect.location == "/api/v2/ping"

        assert client.get("/api/v1/ping").get_data(as_text=True) == "v1"
        assert client.get("/api/v2/ping").get_data(as_text=True) == "v2"


def test_login_button_shows_admin_login_label(client):
    """Test that login button displays 'Admin Login' for clarity."""
    response = client.get("/organization_list", follow_redirects=True)
    html = response.data.decode()
    soup = BeautifulSoup(html, "html.parser")
    login_link = soup.find("a", {"class": "harvester-nav__utility-link"})
    assert login_link is not None
    assert login_link.text.strip() == "Admin Login"


def test_mobile_menu_shows_admin_login_label(client):
    """Test that mobile navigation also shows 'Admin Login' label."""
    response = client.get("/organization_list", follow_redirects=True)
    soup = BeautifulSoup(response.data.decode(), "html.parser")
    # Both desktop and mobile nav should have the same link
    login_links = soup.find_all("a", {"class": "harvester-nav__utility-link"})
    # Filter to login links (exclude logout)
    login_links = [link for link in login_links if "login" in link.get("href", "")]
    assert len(login_links) > 0
    for link in login_links:
        assert link.text.strip() == "Admin Login"


def test_logged_in_user_sees_username_not_login(client):
    """Test that logged-in users see their username, not the login button."""
    # Mock a logged-in session
    with client.session_transaction() as sess:
        sess["user"] = "test.user@gsa.gov"

    response = client.get("/organization_list", follow_redirects=True)
    html = response.data.decode()

    # Should show username
    assert "test.user@gsa.gov" in html
    # Should NOT show login button
    soup = BeautifulSoup(html, "html.parser")
    login_link = soup.find(
        "a", {"class": "harvester-nav__utility-link"}, string="Admin Login"
    )
    assert login_link is None  # Login button should not appear when logged in
