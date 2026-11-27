import hashlib
import json
import logging
from unittest.mock import patch

import pytest

from app import create_app
from app.api_credentials import (
    APIIdentity,
    authenticate_api_key,
    parse_api_clients,
)
from app.startup_validation import StartupValidationError

# Synthetic keys only.
CLIENT_A_KEY = "synthetic-client-a-key-0123456789abcdef"
CLIENT_B_KEY = "synthetic-client-b-key-fedcba9876543210"
LEGACY_KEY = "synthetic-legacy-shared-token"


def digest_of(key: str) -> str:
    return "sha256:" + hashlib.sha256(key.encode()).hexdigest()


def clients_json(**clients) -> str:
    return json.dumps({k.replace("_", "-"): v for k, v in clients.items()})


class TestParseAPIClients:
    @pytest.mark.parametrize("raw", [None, "", "   ", "null", "None", "undefined"])
    def test_unset_or_placeholder_means_no_clients(self, raw):
        assert dict(parse_api_clients(raw)) == {}

    def test_parses_single_and_listed_digests(self):
        clients = parse_api_clients(
            clients_json(
                client_a=digest_of(CLIENT_A_KEY),
                client_b=[digest_of(CLIENT_B_KEY), digest_of("synthetic-old-b-key")],
            )
        )
        assert sorted(clients.values()) == ["client-a", "client-b", "client-b"]

    def test_result_is_read_only(self):
        clients = parse_api_clients(clients_json(client_a=digest_of(CLIENT_A_KEY)))
        with pytest.raises(TypeError):
            clients[b"x"] = "attacker"

    @pytest.mark.parametrize(
        "raw",
        [
            "{not json",
            '["client-a"]',
            '"sha256:abc"',
            "42",
            "[" * 100_000,  # deep nesting
        ],
    )
    def test_rejects_non_object_config(self, raw):
        with pytest.raises(StartupValidationError):
            parse_api_clients(raw)

    def test_rejects_duplicate_client_ids(self):
        raw = (
            f'{{"client-a": "{digest_of(CLIENT_A_KEY)}", '
            f'"client-a": "{digest_of(CLIENT_B_KEY)}"}}'
        )
        with pytest.raises(StartupValidationError, match="more than once"):
            parse_api_clients(raw)

    def test_rejects_same_key_for_two_clients(self):
        raw = clients_json(
            client_a=digest_of(CLIENT_A_KEY), client_b=digest_of(CLIENT_A_KEY)
        )
        with pytest.raises(StartupValidationError, match="same key"):
            parse_api_clients(raw)

    def test_rejects_same_key_listed_twice_for_one_client(self):
        raw = clients_json(client_a=[digest_of(CLIENT_A_KEY), digest_of(CLIENT_A_KEY)])
        with pytest.raises(StartupValidationError, match="same key"):
            parse_api_clients(raw)

    def test_rejects_reuse_of_legacy_shared_token(self):
        raw = clients_json(client_a=digest_of(LEGACY_KEY))
        with pytest.raises(StartupValidationError, match="HARVEST_API_TOKEN"):
            parse_api_clients(raw, legacy_token=LEGACY_KEY)

    @pytest.mark.parametrize(
        "client_id",
        [
            "",
            "Client-A",  # uppercase would allow case-only collisions
            "client a",
            "client\nuser=admin",  # log injection
            "client-a\n",  # trailing newline
            "client=a",
            "<api_token>",  # would impersonate the legacy actor
            "someone@gsa.gov",  # would look like a Login.gov user
            "-leading-dash",
            "trailing-dash-",
            "a" * 65,
            "clienté",
        ],
    )
    def test_rejects_unsafe_client_ids(self, client_id):
        raw = json.dumps({client_id: digest_of(CLIENT_A_KEY)})
        with pytest.raises(StartupValidationError, match="invalid client ID"):
            parse_api_clients(raw)

    @pytest.mark.parametrize(
        "digest",
        [
            [],
            None,
            123,
            {"sha256": "x"},
            CLIENT_A_KEY,  # a raw key instead of a digest
            "sha256:" + "a" * 63,
            "sha256:" + "A" * 64,
            "sha512:" + "a" * 64,
            " sha256:" + "a" * 64,
            "sha256:" + "a" * 64 + "\n",
            ["sha256:" + "a" * 64, 7],
        ],
    )
    def test_rejects_malformed_digests(self, digest):
        with pytest.raises(StartupValidationError):
            parse_api_clients(json.dumps({"client-a": digest}))

    def test_error_messages_never_contain_keys_or_digests(self):
        digest = digest_of(CLIENT_A_KEY)
        bad_configs = [
            (clients_json(client_a=digest, client_b=digest), None),
            (clients_json(client_a=digest_of(LEGACY_KEY)), LEGACY_KEY),
            (json.dumps({"client-a": CLIENT_A_KEY}), None),
            (json.dumps({"BAD": digest}), None),
            ("{" + digest, None),
        ]
        for raw, legacy in bad_configs:
            with pytest.raises(StartupValidationError) as exc_info:
                parse_api_clients(raw, legacy_token=legacy)
            message = str(exc_info.value)
            for secret in (CLIENT_A_KEY, LEGACY_KEY, digest, digest[7:]):
                assert secret not in message
            assert digest_of(LEGACY_KEY)[7:] not in message


class TestAuthenticateAPIKey:
    @pytest.fixture
    def clients(self):
        return parse_api_clients(
            clients_json(
                client_a=digest_of(CLIENT_A_KEY), client_b=digest_of(CLIENT_B_KEY)
            ),
            legacy_token=LEGACY_KEY,
        )

    def test_distinct_clients_get_distinct_identities(self, clients):
        a = authenticate_api_key(CLIENT_A_KEY, clients, LEGACY_KEY)
        b = authenticate_api_key(CLIENT_B_KEY, clients, LEGACY_KEY)
        assert a == APIIdentity("api_client:client-a", "api_client", "client-a")
        assert b == APIIdentity("api_client:client-b", "api_client", "client-b")

    def test_legacy_token_keeps_legacy_identity(self, clients):
        identity = authenticate_api_key(LEGACY_KEY, clients, LEGACY_KEY)
        assert identity == APIIdentity("<api_token>", "api_token", None)

    @pytest.mark.parametrize(
        "key",
        [
            "",
            "wrong",
            CLIENT_A_KEY + " ",
            CLIENT_A_KEY.upper(),
            digest_of(CLIENT_A_KEY),  # presenting the digest is not the key
            digest_of(CLIENT_A_KEY)[7:],
        ],
    )
    def test_rejects_invalid_keys(self, clients, key):
        assert authenticate_api_key(key, clients, LEGACY_KEY) is None

    def test_no_legacy_token_configured_rejects_everything_else(self, clients):
        assert authenticate_api_key(LEGACY_KEY, clients, None) is None
        assert authenticate_api_key("", clients, "") is None

    @pytest.mark.parametrize(
        ("key", "expected_actor"),
        [
            (CLIENT_A_KEY, "api_client:client-a"),
            (CLIENT_B_KEY, "api_client:client-b"),
            (LEGACY_KEY, "<api_token>"),
            ("wrong", None),
        ],
        ids=["first-client", "last-client", "legacy", "no-match"],
    )
    def test_every_digest_is_compared_whichever_entry_matches(
        self, clients, key, expected_actor
    ):
        import hmac

        real_compare = hmac.compare_digest
        compared = []

        def recording_compare(a, b):
            compared.append(b)
            return real_compare(a, b)

        with patch(
            "app.api_credentials.hmac.compare_digest", side_effect=recording_compare
        ):
            identity = authenticate_api_key(key, clients, LEGACY_KEY)

        assert (identity.actor if identity else None) == expected_actor
        legacy_digest = hashlib.sha256(LEGACY_KEY.encode()).digest()
        # Same comparisons, in the same order, for success and failure paths.
        assert compared == [*clients.keys(), legacy_digest]

    def test_without_legacy_token_only_client_digests_are_compared(self, clients):
        with patch(
            "app.api_credentials.hmac.compare_digest", return_value=False
        ) as compare:
            assert authenticate_api_key(CLIENT_A_KEY, clients, None) is None
        assert compare.call_count == len(clients)


class TestCreateAppConfig:
    BASE_ENV = {
        "FLASK_APP_SECRET_KEY": "validated-secret-key",
        "HARVEST_API_TOKEN": LEGACY_KEY,
    }

    def _create(self, extra):
        with (
            patch.dict("os.environ", {**self.BASE_ENV, **extra}),
            patch("app.deps.load_manager.start", lambda: True),
        ):
            return create_app()

    def test_unset_keeps_existing_startup_behavior(self):
        with patch.dict("os.environ", {}, clear=False) as env:
            env.pop("HARVEST_API_CLIENTS", None)
            app = self._create({})
        assert dict(app.config["API_CLIENTS"]) == {}
        assert app.config["API_TOKEN"] == LEGACY_KEY

    def test_valid_config_is_loaded(self):
        app = self._create(
            {"HARVEST_API_CLIENTS": clients_json(client_a=digest_of(CLIENT_A_KEY))}
        )
        assert list(app.config["API_CLIENTS"].values()) == ["client-a"]

    def test_invalid_config_fails_startup_without_leaking(self, caplog):
        caplog.set_level(logging.DEBUG)
        with pytest.raises(StartupValidationError) as exc_info:
            self._create(
                {"HARVEST_API_CLIENTS": clients_json(client_a=digest_of(LEGACY_KEY))}
            )
        assert LEGACY_KEY not in str(exc_info.value)
        assert LEGACY_KEY not in caplog.text
        assert digest_of(LEGACY_KEY)[7:] not in caplog.text
