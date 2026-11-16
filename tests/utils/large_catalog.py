"""DCAT-US 1.1 catalogs of an exact byte size, generated at test time for the
large-upload tests (GSA/data.gov#6460). Write them under pytest's tmp_path and
delete them afterwards; never commit one."""

import json
from pathlib import Path

FEDERAL = "dcatus1.1: federal dataset"
INVALID_ACCESS_LEVEL = "not-an-access-level"

_EXAMPLE = (
    Path(__file__).resolve().parents[2] / "example_data" / "dcatus" / "dcatus.json"
)
_MARK = "@@N@@"
_HEADER = (
    "{\n"
    '  "@type": "dcat:Catalog",\n'
    '  "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",\n'
    '  "dataset": [\n'
)
_SEPARATOR = ",\n"
_TAIL = "\n  ]\n}\n"


def _record_templates() -> list[str]:
    """The valid federal records in example_data, pretty-printed like a real
    catalog, with a marker for a per-record identifier and title."""
    templates = []
    for record in json.loads(_EXAMPLE.read_text())["dataset"]:
        record = dict(
            record,
            identifier=f"{record['identifier']}-{_MARK}",
            title=f"{record['title']} {_MARK}",
        )
        templates.append(json.dumps(record, indent=2))
    return templates


def write_catalog(path: Path, size: int, *, invalid_last: str | None = None) -> int:
    """Write a DCAT-US 1.1 federal catalog of exactly `size` bytes (ASCII, so
    bytes and characters agree) to `path`, and return how many records it has.

    Every record is valid, unless `invalid_last` is given: then the last
    record has that identifier and an accessLevel the schema refuses, so a
    response that reports it proves the whole document was validated.
    Whitespace before the closing brackets makes up the exact size.
    """
    templates = _record_templates()
    last = None
    if invalid_last is not None:
        last = json.dumps(
            dict(
                json.loads(templates[0].replace(_MARK, "last")),
                identifier=invalid_last,
                accessLevel=INVALID_ACCESS_LEVEL,
            ),
            indent=2,
        )
    reserved = len(_TAIL) + (len(_SEPARATOR) + len(last) if last else 0)

    count = 0
    written = len(_HEADER)
    with open(path, "w", encoding="ascii", buffering=1024 * 1024) as fp:
        fp.write(_HEADER)
        while True:
            record = templates[count % len(templates)].replace(_MARK, str(count))
            chunk = (_SEPARATOR if count else "") + record
            if written + len(chunk) + reserved > size:
                break
            fp.write(chunk)
            written += len(chunk)
            count += 1
        if last:
            fp.write((_SEPARATOR if count else "") + last)
            written += len(_SEPARATOR if count else "") + len(last)
            count += 1
        padding = size - written - len(_TAIL)
        if padding < 0:
            raise ValueError(f"{size} bytes is too small for a catalog")
        fp.write(" " * padding + _TAIL)
    assert path.stat().st_size == size
    return count
