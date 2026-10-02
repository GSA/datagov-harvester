from app.constants import MAX_UPLOAD_BYTES, MAX_UPLOAD_MB


class TestValidatorPage:
    """
    The /validate/ page only ever renders the form; submission is a
    client-side fetch() straight to /api/v1/validate (see
    app/static/js/view_validators.js), proxied by nginx directly to
    datagov-validator. This app has no server-side code path that calls out
    to the validator, so a slow or malicious submission can't tie up its
    gunicorn workers (GSA/data.gov#6293).
    """

    def test_get_renders_empty_form_with_upload_limits(self, client):
        """
        Jinja renders an undefined variable as "", silently breaking the
        client-side guard's JS. Pin both uses of the limit.
        """
        res = client.get("/validate/")

        assert res.status_code == 200
        assert f"window.MAX_UPLOAD_BYTES = {MAX_UPLOAD_BYTES};" in res.text
        assert f"Maximum size: {MAX_UPLOAD_MB} MB." in res.text

    def test_post_is_not_allowed(self, client):
        """
        A direct POST (bypassing the browser/JS entirely) must not be able to
        trigger any server-side validation work - that's the actual fix for
        GSA/data.gov#6293, not just a nicer UI for honest users.
        """
        res = client.post("/validate/", data={})

        assert res.status_code == 405
