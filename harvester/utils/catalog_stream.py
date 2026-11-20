"""Stream a DCAT-US JSON catalog from disk instead of holding it in memory.

A catalog is read twice. CatalogFile.index() returns the catalog with every
harvested object replaced by a small stub that keeps only the fields the
identifier filters, duplicate checks, deletion diff, and DCAT-US 3.0 split
read, so that existing logic runs on stubs unchanged. CatalogFile.records()
streams the file again and rebuilds the full record for each surviving stub.
"""

import codecs
import os
import tempfile

import ijson
import requests
from ijson.common import ObjectBuilder
from requests.utils import guess_json_utf

from dcatus_validation.fetch import USER_AGENT

LOC_KEY = "_catalog_loc"

DOWNLOAD_CHUNK_SIZE = 1024 * 1024

DCATUS3_HARVESTED_FIELDS = ("dataset", "service", "record", "datasetSeries")

# Fields on a parent object that embed full Dataset objects.
NESTED_DATASET_FIELDS = {
    "service": ("servesDataset",),
    "datasetSeries": ("seriesMember", "first", "last"),
}

STUB_FIELDS = {
    "dataset": ("identifier", "title", "parent_identifier"),
    "service": ("identifier", "@id", "title"),
    "datasetSeries": ("identifier", "@id", "title"),
    "record": ("@id", "title", "primaryTopic", "modified"),
}

# Stub keys that are bookkeeping, not source fields to copy back.
_NOT_OVERLAID = {LOC_KEY} | {
    field for fields in NESTED_DATASET_FIELDS.values() for field in fields
}


def download_json_to_file(url: str) -> tuple[str, str]:
    """Download url to a temp file and return (path, encoding). The encoding
    is picked like requests' Response.json(): the charset the server sent,
    else a guess from the first bytes."""
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, stream=True)
    try:
        if not 200 <= resp.status_code < 300:
            raise Exception(f"unexpected status code {resp.status_code}")
        fd, path = tempfile.mkstemp(suffix=".json", prefix="catalog-")
        try:
            with os.fdopen(fd, "wb") as f:
                for chunk in resp.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                    f.write(chunk)
            with open(path, "rb") as f:
                head = f.read(4)
        except BaseException:
            os.unlink(path)
            raise
    finally:
        resp.close()

    encoding = resp.encoding or guess_json_utf(head) or "utf-8"
    return path, encoding


def _usable_encoding(encoding: str) -> str:
    # requests falls back to UTF-8 when the server names an unknown charset
    try:
        name = codecs.lookup(encoding).name
    except LookupError:
        return "utf-8"
    return "utf-8-sig" if name == "utf-8" else name


class _Utf8Reader:
    """Decode the file like requests' Response.text (undecodable bytes become
    U+FFFD) and hand ijson UTF-8 bytes."""

    def __init__(self, path: str, encoding: str):
        self._f = open(path, "r", encoding=_usable_encoding(encoding), errors="replace")

    def read(self, size=-1) -> bytes:
        return self._f.read(size).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._f.close()


# The C backend rejects integers wider than 64 bits and some escapes that
# json.loads accepts; the pure-Python backend handles them, more slowly.
_FAST_BACKEND = ijson.get_backend("yajl2_c")
_SAFE_BACKEND = ijson.get_backend("python")


def _walk(path, encoding, nested, backend, skeleton=None, stub_for=None):
    """Yield (ordinal, field, obj) for each harvested object, numbered in
    document order. When skeleton is given, every other parse event is fed to
    it and stub_for(ordinal, field, obj) takes each object's place.

    nested=True follows DCAT-US 3.0 sub-catalogs under "catalog" and harvests
    every DCATUS3_HARVESTED_FIELDS array; otherwise only the root "dataset"
    array is harvested.
    """
    harvested_fields = DCATUS3_HARVESTED_FIELDS if nested else ("dataset",)
    # one entry per open container: "catalog" (a map whose harvested fields
    # we collect), a harvested field name (an array of harvested objects),
    # "catalogs" (an array of sub-catalog maps), or None
    stack = []
    keys = []
    ordinal = 0
    builder = None
    depth = 0
    field = None

    with _Utf8Reader(path, encoding) as f:
        for event, value in backend.basic_parse(f, use_float=True):
            if builder is not None:
                builder.event(event, value)
                if event in ("start_map", "start_array"):
                    depth += 1
                elif event in ("end_map", "end_array"):
                    depth -= 1
                    if depth == 0:
                        obj = builder.value
                        builder = None
                        if skeleton is not None:
                            skeleton.event("string", stub_for(ordinal, field, obj))
                        yield ordinal, field, obj
                        ordinal += 1
                continue

            parent = stack[-1] if stack else None
            if event == "start_map" and parent in harvested_fields:
                builder = ObjectBuilder()
                builder.event(event, value)
                depth = 1
                field = parent
                continue

            if skeleton is not None:
                skeleton.event(event, value)

            if event == "map_key":
                keys[-1] = value
            elif event == "start_map":
                is_catalog = not stack or parent == "catalogs"
                stack.append("catalog" if is_catalog else None)
                keys.append(None)
            elif event == "start_array":
                kind = None
                if parent == "catalog":
                    key = keys[-1]
                    if key in harvested_fields:
                        kind = key
                    elif nested and key == "catalog":
                        kind = "catalogs"
                stack.append(kind)
                keys.append(None)
            elif event in ("end_map", "end_array"):
                stack.pop()
                keys.pop()


def _stub(obj, field, loc):
    if not isinstance(obj, dict):
        return obj
    stub = {key: obj[key] for key in STUB_FIELDS[field] if key in obj}
    if not obj.get("title") and "description" in obj:
        stub["description"] = obj["description"]
    stub[LOC_KEY] = loc
    return stub


def _stub_with_nested(ordinal, field, obj):
    stub = _stub(obj, field, (ordinal, None, None))
    if not isinstance(obj, dict):
        return stub
    for nested_field in NESTED_DATASET_FIELDS.get(field, ()):
        value = obj.get(nested_field)
        if isinstance(value, list):
            stub[nested_field] = [
                _stub(item, "dataset", (ordinal, nested_field, i))
                for i, item in enumerate(value)
            ]
        elif value is not None:
            stub[nested_field] = _stub(value, "dataset", (ordinal, nested_field, None))
    return stub


def _source_object(obj, loc):
    _, nested_field, index = loc
    if nested_field is None:
        return obj
    value = obj[nested_field]
    return value if index is None else value[index]


def materialize(stub: dict, source: dict) -> dict:
    """Rebuild the full record for stub: the source object plus whatever the
    filters and split added to the stub (resolved or backfilled identifiers,
    parent_identifier)."""
    record = dict(source)
    for key, value in stub.items():
        if key not in _NOT_OVERLAID:
            record[key] = value
    return record


class CatalogFile:
    """A downloaded catalog on disk. nested=True for DCAT-US 3.0, which
    harvests several record types and follows sub-catalogs."""

    def __init__(self, path: str, encoding: str, nested: bool):
        self.path = path
        self.encoding = encoding
        self.nested = nested
        self._backend = _FAST_BACKEND

    def _walk(self, **kwargs):
        return _walk(self.path, self.encoding, self.nested, self._backend, **kwargs)

    def index(self) -> dict:
        """Parse the catalog and return it with each harvested object
        replaced by a stub."""
        try:
            return self._index()
        except (ijson.JSONError, UnicodeDecodeError):
            if self._backend is _SAFE_BACKEND:
                raise
            # later reads must use the same backend so ordinals line up
            self._backend = _SAFE_BACKEND
            return self._index()

    def _index(self) -> dict:
        skeleton = ObjectBuilder()
        for _ in self._walk(skeleton=skeleton, stub_for=_stub_with_nested):
            pass
        return skeleton.value

    def records(self, stubs: list):
        """Yield the full record for each stub, in document order. stubs is
        emptied up front so each stub is released once its record is built."""
        wanted = {}
        for stub in stubs:
            wanted.setdefault(stub[LOC_KEY][0], []).append(stub)
        stubs.clear()

        for ordinal, _, obj in self._walk():
            if not wanted:
                return
            for stub in wanted.pop(ordinal, ()):
                yield materialize(stub, _source_object(obj, stub[LOC_KEY]))
