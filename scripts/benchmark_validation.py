"""
Benchmark DCAT-US validation: the old pure-Python path against the fast path
(orjson + jsonschema-rs + the pre-crawled 1.1 registry), GSA/data.gov#6460.

Not a test. It generates large DCAT-US 1.1 and 3.0 catalogs (valid, and
invalid with a few bad records spread through them) in a temporary directory
under /tmp, validates each one the way the validator API does, once per path,
in a fresh process each time, and prints a table of time and peak memory. It
also checks that both paths return exactly the same errors, and times the
harvest runner's per-record validation with and without the pre-crawled
registry. The catalogs are deleted afterwards unless --keep is given.

Usage:
    poetry run python scripts/benchmark_validation.py [--size-mb 150]
        [--only 1.1-valid,3.0-invalid] [--keep] [--skip-harvest]

Each measurement runs as a subprocess:
    python scripts/benchmark_validation.py measure FILE SCHEMA old|new
"""

import argparse
import copy
import hashlib
import json
import os
import resource
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

EXAMPLES = REPO_ROOT / "example_data" / "dcatus"
DCATUS3_EXAMPLES = REPO_ROOT / "_external" / "dcat-us" / "jsonschema" / "examples"
FEDERAL = "dcatus1.1: federal dataset"
DCATUS3 = "dcatus3.0 catalog"

# One bad record in this many, in the "invalid" catalogs: few enough to stay
# under the 1,000-error cap, so both paths have to validate everything.
INVALID_EVERY = 1000


def _load(path):
    with open(path) as fp:
        return json.load(fp)


def _with_query(url, i):
    if not isinstance(url, str) or not url.startswith("http"):
        return url
    return f"{url}{'&' if '?' in url else '?'}bench={i}"


def _date(i, with_time):
    year, month, day = 2000 + i % 25, 1 + i % 12, 1 + i % 28
    if not with_time:
        return f"{year}-{month:02}-{day:02}"
    return f"{year}-{month:02}-{day:02}T{i % 24:02}:{i % 60:02}:{(i * 7) % 60:02}Z"


# --- DCAT-US 1.1 --------------------------------------------------------------


def _dcatus1_1_pool():
    """Real records from example_data, repaired to be valid federal datasets:
    the Philadelphia records lack bureau/program codes and a modified date."""
    pool = []
    for name in ("dcatus.json", "dcatus_2.json", "dcatus_with_none.json"):
        for record in _load(EXAMPLES / name)["dataset"]:
            record.setdefault("bureauCode", ["015:11"])
            record.setdefault("programCode", ["015:001"])
            if not record.get("modified"):
                record["modified"] = "2024-01-15"
            pool.append(record)
    return pool


def _unique_dcatus1_1(record, i):
    """A copy of `record` with the fields that differ between real records
    (identifier, title, URLs, dates, keywords) made unique."""
    record = copy.deepcopy(record)
    record["identifier"] = f"{record['identifier']}-{i}"
    record["title"] = f"{record['title']} ({i})"
    record["modified"] = _date(i, with_time=i % 2 == 0)
    record["landingPage"] = _with_query(record.get("landingPage"), i)
    record["keyword"] = [*record.get("keyword", []), f"topic-{i % 997}"]
    for distribution in record.get("distribution") or []:
        for key in ("downloadURL", "accessURL"):
            if key in distribution:
                distribution[key] = _with_query(distribution[key], i)
    return record


def _break_dcatus1_1(record, i):
    mutation = (i // INVALID_EVERY) % 8
    if mutation == 0:
        record["accessLevel"] = "secret"
    elif mutation == 1:
        del record["title"]
    elif mutation == 2:
        record["license"] = "not a uri"
    elif mutation == 3:
        record["modified"] = "last tuesday"
    elif mutation == 4:
        record["bureauCode"] = ["15:11"]
    elif mutation == 5:
        record["contactPoint"]["hasEmail"] = "nobody@example.gov"
    elif mutation == 6:
        record["keyword"] = []
    else:
        record["distribution"] = [{"mediaType": "csv", "downloadURL": "x y"}]
    return record


# --- DCAT-US 3.0 --------------------------------------------------------------


def _dcatus3_pool():
    pool = [
        _load(path)
        for path in sorted((DCATUS3_EXAMPLES / "Dataset" / "good").glob("*.json"))
    ]
    pool += _load(EXAMPLES / "dcatus3_0.json")["dataset"]
    return pool


def _unique_dcatus3(dataset, i):
    dataset = copy.deepcopy(dataset)
    if isinstance(dataset.get("identifier"), str):
        dataset["identifier"] = f"{dataset['identifier']}-{i}"
    dataset["title"] = f"{dataset['title']} ({i})"
    dataset["modified"] = _date(i, with_time=i % 2 == 0)
    if isinstance(dataset.get("keyword"), list):
        dataset["keyword"] = [*dataset["keyword"], f"topic-{i % 997}"]
    for distribution in dataset.get("distribution") or []:
        for key in ("downloadURL", "accessURL"):
            if key in distribution:
                distribution[key] = _with_query(distribution[key], i)
    return dataset


def _break_dcatus3(dataset, i):
    mutation = (i // INVALID_EVERY) % 6
    if mutation == 0:
        del dataset["title"]
    elif mutation == 1:
        dataset["modified"] = "last tuesday"
    elif mutation == 2:
        dataset["contactPoint"] = "nobody"
    elif mutation == 3:
        dataset["accrualPeriodicity"] = "sometimes"
    elif mutation == 4:
        dataset["keyword"] = [""]
    else:
        dataset["distribution"] = [{"downloadURL": "not an iri"}]
    return dataset


# --- generation ---------------------------------------------------------------


def generate(path, version, target_bytes, invalid):
    """Write a catalog of about target_bytes to path, one record at a time."""
    if version == "1.1":
        pool, unique, broken = _dcatus1_1_pool(), _unique_dcatus1_1, _break_dcatus1_1
        header = {
            "@type": "dcat:Catalog",
            "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
        }
    else:
        pool, unique, broken = _dcatus3_pool(), _unique_dcatus3, _break_dcatus3
        header = {
            "@type": "Catalog",
            "title": "Benchmark catalog",
            "description": "Generated by scripts/benchmark_validation.py",
        }

    prefix = json.dumps(header)[:-1] + ', "dataset": ['
    count = 0
    with open(path, "w") as fp:
        fp.write(prefix)
        written = len(prefix)
        while written < target_bytes:
            record = unique(pool[count % len(pool)], count)
            if invalid and count % INVALID_EVERY == INVALID_EVERY // 2:
                record = broken(record, count)
            chunk = ("," if count else "") + json.dumps(record)
            fp.write(chunk)
            written += len(chunk.encode())
            count += 1
        fp.write("]}")
    return count


# --- measurement (runs in a subprocess) --------------------------------------


def _peak_rss_mb():
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # bytes on macOS, KiB on Linux
    return peak / (1024 * 1024) if sys.platform == "darwin" else peak / 1024


def measure(path, schema, mode):
    os.environ["DCATUS_VALIDATION_FAST_PATH"] = "true" if mode == "new" else "false"
    from dcatus_validation.fetch import parse_json_document
    from dcatus_validation.limits import MAX_VALIDATION_ERRORS
    from dcatus_validation.validate import (
        load_schema_validators,
        validate_records_limited,
    )

    load_schema_validators()
    with open(path, "rb") as fp:
        content = fp.read()
    input_rss = _peak_rss_mb()

    start = time.perf_counter()
    document = parse_json_document(content)
    parsed = time.perf_counter()
    errors, incomplete = validate_records_limited(
        document, schema, max_errors=MAX_VALIDATION_ERRORS
    )
    done = time.perf_counter()

    digest = hashlib.sha256(json.dumps([errors, incomplete]).encode()).hexdigest()
    return {
        "parse_s": parsed - start,
        "validate_s": done - parsed,
        "total_s": done - start,
        "peak_rss_mb": _peak_rss_mb(),
        "input_rss_mb": input_rss,
        "errors": len(errors),
        "incomplete": incomplete,
        "errors_sha256": digest,
    }


def measure_harvest(path, schema, mode):
    """The harvest runner's per-record validation (harvester/harvest.py): the
    1.1 validator with the old registry or the pre-crawled one."""
    import orjson

    from dcatus_validation.messages import (
        assemble_validation_errors,
        build_dcatus1_1_validator,
        build_dcatus3_validator,
        open_json,
    )
    from dcatus_validation.schema_paths import (
        DCATUS1_1_DIR,
        DCATUS3_DEFINITIONS_DIR,
    )

    if schema == DCATUS3:
        validator = build_dcatus3_validator(
            DCATUS3_DEFINITIONS_DIR,
            root_ref="https://resources.data.gov/dcat-us/3.0.0/definitions/dataset",
        )
    else:
        validator = build_dcatus1_1_validator(
            open_json(DCATUS1_1_DIR / "federal_dataset.json"),
            precrawl=mode == "new",
        )
    with open(path, "rb") as fp:
        records = orjson.loads(fp.read())["dataset"]

    start = time.perf_counter()
    messages = [
        [
            error.message
            for error in assemble_validation_errors(validator.iter_errors(r))
        ]
        for r in records
    ]
    elapsed = time.perf_counter() - start
    digest = hashlib.sha256(json.dumps(messages).encode()).hexdigest()
    return {
        "total_s": elapsed,
        "records": len(records),
        "errors": sum(map(len, messages)),
        "errors_sha256": digest,
        "peak_rss_mb": _peak_rss_mb(),
    }


def _run(command, *args):
    result = subprocess.run(
        [sys.executable, __file__, command, *map(str, args)],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    if result.returncode != 0:
        raise SystemExit(result.stderr)
    return json.loads(result.stdout.strip().splitlines()[-1])


VARIANTS = {
    "1.1-valid": ("1.1", False, FEDERAL),
    "1.1-invalid": ("1.1", True, FEDERAL),
    "3.0-valid": ("3.0", False, DCATUS3),
    "3.0-invalid": ("3.0", True, DCATUS3),
}


def main():
    if len(sys.argv) > 1 and sys.argv[1] in ("measure", "measure-harvest"):
        command, path, schema, mode = sys.argv[1:5]
        function = measure if command == "measure" else measure_harvest
        print(json.dumps(function(path, schema, mode)))
        return

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--size-mb", type=float, default=150)
    parser.add_argument("--only", default=",".join(VARIANTS))
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--skip-harvest", action="store_true")
    args = parser.parse_args()

    workdir = Path(tempfile.mkdtemp(prefix="dcatus-benchmark-", dir="/tmp"))
    print(f"catalogs in {workdir}", file=sys.stderr)
    rows, harvest_rows = [], []
    try:
        for name in args.only.split(","):
            version, invalid, schema = VARIANTS[name]
            path = workdir / f"{name}.json"
            records = generate(path, version, int(args.size_mb * 1024 * 1024), invalid)
            size_mb = path.stat().st_size / (1024 * 1024)
            print(f"{name}: {size_mb:.0f}MB, {records} records", file=sys.stderr)
            old = _run("measure", path, schema, "old")
            new = _run("measure", path, schema, "new")
            rows.append((name, size_mb, records, old, new))
            if not args.skip_harvest and not invalid:
                harvest_rows.append(
                    (
                        name,
                        _run("measure-harvest", path, schema, "old"),
                        _run("measure-harvest", path, schema, "new"),
                    )
                )
            path.unlink()
    finally:
        if args.keep:
            print(f"kept {workdir}", file=sys.stderr)
        else:
            shutil.rmtree(workdir, ignore_errors=True)

    print(
        "| catalog | size | records | errors | old: parse + validate "
        "| new: parse + validate | speed-up | old peak RSS | new peak RSS "
        "| same result |"
    )
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for name, size_mb, records, old, new in rows:
        print(
            f"| {name} | {size_mb:.0f}MB | {records:,} | {old['errors']}"
            f"{' (capped)' if old['incomplete'] else ''} "
            f"| {old['total_s']:.1f}s ({old['parse_s']:.1f} + {old['validate_s']:.1f}) "
            f"| {new['total_s']:.2f}s ({new['parse_s']:.2f} + {new['validate_s']:.2f}) "
            f"| {old['total_s'] / new['total_s']:.0f}x "
            f"| {old['peak_rss_mb']:.0f}MB | {new['peak_rss_mb']:.0f}MB "
            f"| {'yes' if old['errors_sha256'] == new['errors_sha256'] else 'NO'} |"
        )
    if harvest_rows:
        print()
        print(
            "| harvest runner, per record | records | old registry "
            "| pre-crawled registry | speed-up | same result |"
        )
        print("| --- | --- | --- | --- | --- | --- |")
        for name, old, new in harvest_rows:
            print(
                f"| {name} | {old['records']:,} | {old['total_s']:.1f}s "
                f"| {new['total_s']:.1f}s | {old['total_s'] / new['total_s']:.1f}x "
                f"| {'yes' if old['errors_sha256'] == new['errors_sha256'] else 'NO'} |"
            )
    if any(o["errors_sha256"] != n["errors_sha256"] for *_, o, n in rows):
        raise SystemExit("old and new paths returned different errors")


if __name__ == "__main__":
    main()
