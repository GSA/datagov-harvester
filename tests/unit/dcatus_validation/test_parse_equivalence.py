"""orjson against the stdlib parser (GSA/data.gov#6460): every document
parses to the same value, or fails with the same error, on both paths. Where
orjson differs from json.loads, parse_json_document re-parses with the stdlib
path, so each difference below is pinned with a test that it ends the same.

The one deliberate change: a document nested 101 to 1024 levels deep now gets
past the parser, since orjson never recurses, and the post-parse depth check
in validate.py rejects it with the same CatalogTooDeeplyNested. The API's
response is unchanged (tests/integration/validator_api).
"""

import codecs
import json
import os
from unittest import mock

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from dcatus_validation import fetch
from dcatus_validation.errors import CatalogTooDeeplyNested
from dcatus_validation.fetch import (
    INVALID_JSON_ENCODING_MESSAGE,
    INVALID_JSON_NUMBER_MESSAGE,
)
from dcatus_validation.limits import MAX_DOCUMENT_NESTING_DEPTH
from dcatus_validation.validate import validate_records
from tests.unit.dcatus_validation.equivalence_helpers import fast_path

EXAMPLE_COUNT = int(os.getenv("DCATUS_EQUIVALENCE_EXAMPLES", "300"))
HYPOTHESIS_SETTINGS = settings(
    max_examples=EXAMPLE_COUNT,
    derandomize=True,
    deadline=None,
    suppress_health_check=list(HealthCheck),
)


def parse_outcome(document):
    """What a caller can observe: the value (repr keeps 1, 1.0 and True, and
    key order, apart) or the error and everything it carries."""
    # Through the module: tests/unit/dcatus_validation/test_fetch.py reloads
    # it, which replaces InvalidCatalogSource and parse_json_document.
    try:
        return ("value", repr(fetch.parse_json_document(document)))
    except json.JSONDecodeError as error:
        return ("JSONDecodeError", error.msg, error.lineno, error.colno, error.pos)
    except fetch.InvalidCatalogSource as error:
        return ("InvalidCatalogSource", error.public_message)
    except Exception as error:
        return (type(error).__name__, str(error))


def both_parsers(document):
    with fast_path(False):
        old = parse_outcome(document)
    with fast_path(True):
        new = parse_outcome(document)
    return old, new


def orjson_accepts(document) -> bool:
    return fetch._parse_json_document_orjson(document) is not fetch._ORJSON_REFUSED


def nested(depth, encoding=None):
    document = "[" * depth + "0" + "]" * depth
    return document.encode(encoding) if encoding else document


# (document, whether orjson accepts it): each difference between orjson and
# json.loads, and the cases around it.
CASES = {
    # NaN and Infinity: orjson refuses them as syntax; the stdlib path refuses
    # them as unsupported numbers.
    "NaN": (b'{"v": NaN}', False),
    "Infinity": (b'{"v": Infinity}', False),
    "-Infinity": (b'{"v": -Infinity}', False),
    "overflowing float": (b'{"v": 1e9999}', False),
    "overflowing negative float": (b'{"v": -1e9999}', False),
    "underflowing float": (b'{"v": 1e-400}', True),
    "smallest subnormal": (b'{"v": 5e-324}', True),
    # Any float of 2**63 or more is re-parsed: it could have been an integer.
    "largest float": (b'{"v": 1.7976931348623157e308}', False),
    "1e19": (b'{"v": 1e19}', False),
    "negative zero": (b'{"v": -0.0}', True),
    "integer negative zero": (b'{"v": -0}', True),
    # Integers: exact to 64 bits in orjson; beyond that it makes floats, which
    # the stdlib re-parses to exact ints.
    "u64 max": (b'{"v": 18446744073709551615}', True),
    "u64 max + 1": (b'{"v": 18446744073709551616}', False),
    "i64 min": (b'{"v": -9223372036854775808}', True),
    "i64 min - 1": (b'{"v": -9223372036854775809}', False),
    "100 digits": (b'{"v": ' + b"9" * 100 + b"}", False),
    "4300 digits": (b'{"v": ' + b"9" * 4300 + b"}", False),
    "4301 digits": (b'{"v": ' + b"9" * 4301 + b"}", False),
    "big int in a list": (b"[1, 2, " + b"7" * 30 + b"]", False),
    "float at 2**63": (b'{"v": 9223372036854775808.0}', False),
    "float just under 2**63": (b'{"v": 9223372036854774784.0}', True),
    # Duplicate keys: the last value wins, in the first key's position.
    "duplicate keys": (b'{"a": 1, "b": 2, "a": 3}', True),
    "duplicate nested keys": (b'{"a": {"x": 1, "x": [2]}}', True),
    # Byte order marks and encodings: orjson takes UTF-8 alone.
    "UTF-8 BOM bytes": (codecs.BOM_UTF8 + b'{"a": 1}', False),
    "UTF-8 BOM str": ('﻿{"a": 1}', False),
    "UTF-16 BOM": ('{"a": "é"}'.encode("utf-16"), False),
    "UTF-16-LE": ('{"a": 1}'.encode("utf-16-le"), False),
    "UTF-16-BE": ('{"a": 1}'.encode("utf-16-be"), False),
    "UTF-32 BOM": ('{"a": 1}'.encode("utf-32"), False),
    "UTF-32-LE": ('{"a": 1}'.encode("utf-32-le"), False),
    # Invalid UTF-8 and surrogates.
    "invalid UTF-8": (b'{"a": "\xff"}', False),
    "overlong UTF-8": (b'{"a": "\xc0\xaf"}', False),
    "UTF-8 encoded surrogate": (b'{"a": "\xed\xa0\x80"}', False),
    "truncated UTF-8": (b'{"a": "\xe2\x82"}', False),
    "escaped lone high surrogate": (b'{"a": "\\ud800"}', False),
    "escaped lone low surrogate": (b'{"a": "\\udc00"}', False),
    "escaped surrogate pair": (b'{"a": "\\ud83d\\ude00"}', True),
    "lone surrogate in pasted str": ('{"a": "\ud800"}', False),
    "non-ASCII": ('{"a": "é中\U0001f600"}'.encode(), True),
    "non-ASCII str": ('{"a": "é中\U0001f600"}', True),
    "escaped NUL": (b'{"a": "\\u0000"}', True),
    # Syntax errors: both refuse, and the stdlib's position is reported.
    "raw control character": (b'{"a": "\x01"}', False),
    "raw tab": (b'{"a": "\t"}', False),
    "raw NUL": (b'{"a": "\x00"}', False),
    "DEL": (b'{"a": "\x7f"}', True),
    "trailing comma": (b'{"a": 1,}', False),
    "comment": (b'{"a": 1 /* x */}', False),
    "single quotes": (b"{'a': 1}", False),
    "leading zero": (b'{"a": 01}', False),
    "bare decimal point": (b'{"a": .5}', False),
    "trailing decimal point": (b'{"a": 1.}', False),
    "plus sign": (b'{"a": +1}', False),
    "hex": (b'{"a": 0x10}', False),
    "bad escape": (b'{"a": "\\x41"}', False),
    "unterminated string": (b'{"a": "x', False),
    "missing value": (b'{"a": }', False),
    "extra data": (b"{} x", False),
    "NUL after document": (b"{}\x00", False),
    "vertical tab as whitespace": (b'{\x0b"a": 1}', False),
    "no-break space as whitespace": (b'{\xc2\xa0"a": 1}', False),
    "empty": (b"", False),
    "empty str": ("", False),
    "whitespace only": (b"  \n ", False),
    "scalar": (b"1", True),
    "string scalar": (b'"x"', True),
    "null": (b"null", True),
    "error on line 3": (b'{\n  "a": 1,\n  "b": ]\n}', False),
    "secret in a broken document": (b'{"internal_secret": "hunter2",, "b": 2}', False),
    # Nesting: orjson refuses past 1024 levels; the stdlib's scan refuses past
    # 100. Up to 100 both parse.
    "depth 100": (nested(MAX_DOCUMENT_NESTING_DEPTH, "utf-8"), True),
    "depth 1025": (nested(1025, "utf-8"), False),
    "depth 100,000": (nested(100_000, "utf-8"), False),
    "depth 100,000 str": (nested(100_000), False),
    "depth 100,000 objects": (b'{"a":' * 100_000 + b"1" + b"}" * 100_000, False),
    "deep but unbalanced": (b"[" * 100_000, False),
    "brackets inside strings": (('{"v": "' + "[" * 2000 + '"}').encode(), True),
}


@pytest.mark.parametrize(
    "document, accepted_by_orjson", CASES.values(), ids=list(CASES)
)
def test_every_difference_ends_the_same(document, accepted_by_orjson):
    assert orjson_accepts(document) is accepted_by_orjson
    old, new = both_parsers(document)

    assert new == old


@pytest.mark.parametrize(
    "document, expected",
    [
        (b'{"v": NaN}', ("InvalidCatalogSource", INVALID_JSON_NUMBER_MESSAGE)),
        (b'{"v": 1e9999}', ("InvalidCatalogSource", INVALID_JSON_NUMBER_MESSAGE)),
        (
            b'{"v": ' + b"9" * 5000 + b"}",
            ("InvalidCatalogSource", INVALID_JSON_NUMBER_MESSAGE),
        ),
        (b'{"a": "\xff"}', ("InvalidCatalogSource", INVALID_JSON_ENCODING_MESSAGE)),
        (b'{"v": 18446744073709551616}', ("value", "{'v': 18446744073709551616}")),
        (b'{"a": 1, "b": 2, "a": 3}', ("value", "{'a': 3, 'b': 2}")),
        (codecs.BOM_UTF8 + b'{"a": 1}', ("value", "{'a': 1}")),
        (
            '﻿{"a": 1}',
            (
                "JSONDecodeError",
                "Unexpected UTF-8 BOM (decode using utf-8-sig)",
                1,
                1,
                0,
            ),
        ),
        (b'{"a": "\\ud800"}', ("value", "{'a': '\\ud800'}")),
        (
            b'{"a": 1,}',
            ("JSONDecodeError", "Illegal trailing comma before end of object", 1, 8, 7),
        ),
        (
            nested(100_000, "utf-8"),
            ("CatalogTooDeeplyNested", fetch.NESTING_TOO_DEEP_MESSAGE),
        ),
    ],
    ids=[
        "NaN",
        "infinite float",
        "5000 digits",
        "invalid UTF-8",
        "64-bit overflow stays an int",
        "duplicate keys",
        "BOM bytes",
        "BOM str",
        "lone surrogate",
        "trailing comma",
        "100,000 deep",
    ],
)
def test_the_outcomes_are_the_old_ones(document, expected):
    """Pinned rather than only compared, so both paths can't drift together."""
    with fast_path(True):
        assert parse_outcome(document) == expected


@pytest.mark.parametrize("depth", [101, 500, 1024])
@pytest.mark.parametrize("encoding", [None, "utf-8"])
def test_nesting_between_101_and_1024_is_rejected_after_parsing(depth, encoding):
    """The one change: orjson parses these (without recursing) and the
    post-parse walk rejects them, with the same error the old pre-parse
    scan raised."""
    document = nested(depth, encoding)
    with fast_path(False):
        with pytest.raises(CatalogTooDeeplyNested):
            fetch.parse_json_document(document)
    with fast_path(True):
        parsed = fetch.parse_json_document(document)
        with pytest.raises(CatalogTooDeeplyNested):
            validate_records(parsed, "dcatus1.1: federal dataset")


def test_a_100000_deep_document_never_recurses(monkeypatch):
    """orjson stops at 1024 levels; the stdlib path's character scan then
    rejects it before json.loads. Nothing recurses 100,000 deep."""
    monkeypatch.setattr("sys.getrecursionlimit", lambda: 1000)
    with (
        fast_path(True),
        mock.patch("json.loads", side_effect=AssertionError("json.loads used")),
    ):
        with pytest.raises(CatalogTooDeeplyNested):
            fetch.parse_json_document(nested(100_000, "utf-8"))


def test_orjson_parses_what_it_accepts_without_the_character_scan():
    with (
        fast_path(True),
        mock.patch.object(
            fetch, "_check_json_text_nesting", side_effect=AssertionError("scan")
        ),
    ):
        assert fetch.parse_json_document(b'{"dataset": [{"title": "x"}]}') == {
            "dataset": [{"title": "x"}]
        }


@pytest.mark.parametrize(
    "value, beyond",
    [
        (1.0, False),
        (9223372036854774784.0, False),
        (-9223372036854774784.0, False),
        (9223372036854775808.0, True),
        (-9223372036854775808.0, True),
        (1e300, True),
        ({"a": [{"b": 1e19}]}, True),
        ([[[[-1e19]]]], True),
        (2**80, False),  # an int, which orjson never produces this large
        ({"a": "1e300"}, False),
        ([], False),
    ],
)
def test_floats_beyond_exact_integers(value, beyond):
    assert fetch._has_float_beyond_exact_integers(value) is beyond


# --- random documents (Hypothesis) -------------------------------------------

JSON_VALUES = st.recursive(
    st.none()
    | st.booleans()
    | st.integers()
    | st.integers(min_value=2**62, max_value=2**66)
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.text(),
    lambda children: st.lists(children, max_size=4)
    | st.dictionaries(st.text(max_size=6), children, max_size=4),
    max_leaves=12,
)


def _serialize(value, ensure_ascii, indent):
    return json.dumps(value, ensure_ascii=ensure_ascii, indent=indent)


@HYPOTHESIS_SETTINGS
@given(
    JSON_VALUES,
    st.booleans(),
    st.sampled_from([None, 2]),
    st.sampled_from(["str", "utf-8", "utf-8-sig", "utf-16", "utf-32"]),
)
def test_random_documents_parse_the_same(value, ensure_ascii, indent, encoding):
    text = _serialize(value, ensure_ascii, indent)
    document = text if encoding == "str" else text.encode(encoding, "surrogatepass")
    old, new = both_parsers(document)

    assert new == old


@HYPOTHESIS_SETTINGS
@given(
    JSON_VALUES,
    st.lists(
        st.tuples(
            st.sampled_from(["insert", "delete", "replace"]),
            st.integers(min_value=0),
            st.binary(min_size=1, max_size=3)
            | st.sampled_from(
                [
                    b"[",
                    b"]",
                    b"{",
                    b"}",
                    b'"',
                    b"\\",
                    b",",
                    b":",
                    b"\xff",
                    b"\x00",
                    b"NaN",
                    b"1e999",
                    b"\xef\xbb\xbf",
                ]
            ),
        ),
        min_size=1,
        max_size=4,
    ),
)
def test_corrupted_documents_fail_the_same(value, edits):
    """Random byte damage to a valid document: the same value, or the same
    error at the same line and column."""
    document = bytearray(
        _serialize(value, False, None).encode("utf-8", "surrogatepass")
    )
    for action, position, data in edits:
        position %= len(document) + 1
        if action == "insert":
            document[position:position] = data
        elif action == "delete":
            del document[position : position + len(data)]
        else:
            document[position : position + len(data)] = data
    old, new = both_parsers(bytes(document))

    assert new == old


@HYPOTHESIS_SETTINGS
@given(st.text(max_size=60) | st.binary(max_size=60))
def test_arbitrary_input_fails_the_same(document):
    old, new = both_parsers(document)

    assert new == old


def pasted_outcome(text):
    """parse_outcome for the validator's paste path, which parses the UTF-8
    encoding of the pasted text (parse_pasted_json_document)."""
    try:
        return ("value", repr(fetch.parse_pasted_json_document(text.encode("utf-8"))))
    except json.JSONDecodeError as error:
        return ("JSONDecodeError", error.msg, error.lineno, error.colno, error.pos)
    except fetch.InvalidCatalogSource as error:
        return ("InvalidCatalogSource", error.public_message)
    except Exception as error:
        return (type(error).__name__, str(error))


PASTED_TEXTS = {
    "BOM": '﻿{"dataset": []}',
    "leading NUL (bytes would guess UTF-16)": '\x00{"dataset": []}',
    "NUL pairs (bytes would guess UTF-32)": '\x00\x00\x00{"dataset": []}',
    "integer beyond 64 bits": '{"dataset": [{"title": 18446744073709551616}]}',
    "NaN": '{"dataset": [NaN]}',
    "syntax error on line 3": '{\n  "dataset": [\n    {"title": "x",}\n  ]\n}',
    "non-ASCII": '{"dataset": [{"title": "Données ’ \U0001f600"}]}',
    "nested 101 deep": "[" * 101 + "]" * 101,
    "nested 1025 deep": "[" * 1025 + "]" * 1025,
    "empty": "",
    "escaped lone surrogate": '{"dataset": [{"title": "\\ud800"}]}',
}


@pytest.mark.parametrize("enabled", [True, False], ids=["fast path", "kill switch"])
@pytest.mark.parametrize("text", PASTED_TEXTS.values(), ids=list(PASTED_TEXTS))
def test_pasted_bytes_parse_like_the_pasted_text(text, enabled):
    """The paste route parses the text's UTF-8 bytes so it can drop the text
    first (GSA/data.gov#6460). That must end exactly as parsing the text did,
    including the refusals bytes would otherwise change: a leading BOM, and
    NULs that bytes parsing reads as UTF-16 or UTF-32."""
    with fast_path(enabled):
        assert pasted_outcome(text) == parse_outcome(text)


@HYPOTHESIS_SETTINGS
@given(
    JSON_VALUES.map(lambda value: _serialize(value, False, None))
    | st.text(max_size=60),
    st.booleans(),
)
def test_random_pasted_texts_parse_like_the_text(text, enabled):
    with fast_path(enabled):
        assert pasted_outcome(text) == parse_outcome(text)
