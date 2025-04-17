"""
Datasets-count Comparison Script for catalog and catalog-next 
-------------------------------------------------------------

This script fetches organization data from two apps (catalog and catalog-next),
saves the results as JSON, compares the dataset counts for each organization
between the two catalogs, and outputs a comparison CSV report.

Parameters (can be passed via command line):
- --catalog-url: Base URL for the current catalog
- --catalog-next-url: Base URL for the next catalog
- --output-csv: CSV file path to save the comparison report

Usage:
    python compare_datasets_count.py [--catalog-url <URL>] 
                                     [--catalog-next-url <URL>] 
                                     [--output-csv <FILE>]

Examples:
    # Run with default values
    python compare_datasets_count.py

    # Run with custom parameters
    python compare_datasets_count.py --catalog-url https://catalog.data.gov \
        --catalog-next-url https://catalog-next-dev-datagov.app.cloud.gov \
        --output-csv catalog_orgs_comparison.csv
"""

import requests
import json
import csv
import argparse
import sys

# === Default Values ===
DEFAULT_CATALOG_URL = "https://catalog.data.gov"
DEFAULT_CATALOG_NEXT_URL = "https://catalog-next-dev-datagov.app.cloud.gov"
DEFAULT_OUTPUT_JSON_CURRENT = "catalog_orgs.json"
DEFAULT_OUTPUT_JSON_NEXT = "catalog_next_orgs.json"
DEFAULT_OUTPUT_CSV = "catalog_orgs_comparison.csv"

def fetch_all_organizations(base_url):
    """
    Fetch all organizations and their dataset counts from a catalog.

    Args:
        base_url (str): Base URL of the catalog.

    Returns:
        list: A list of organization dictionaries with dataset counts.
    """
    organizations = []
    limit = 25
    offset = 0

    while True:
        url = f"{base_url}/api/3/action/organization_list"
        params = {
            'include_dataset_count': 'true',
            'all_fields': 'true',
            'limit': limit,
            'offset': offset
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        result = response.json()

        if result.get('success'):
            batch = result['result']
            if not batch:
                break
            organizations.extend(batch)
            offset += limit
        else:
            raise Exception(f"Failed to fetch organizations from {base_url}")

    return organizations

def save_orgs_to_json(orgs, filename):
    """
    Save a list of organizations with datasets count to a JSON file.

    Args:
        orgs (list): List of organization dictionaries.
        filename (str): Output filename for the JSON file.

    Returns:
        None
    """
    with open(filename, "w", encoding="utf-8") as jsonfile:
        json.dump(orgs, jsonfile, indent=2)

def load_orgs(filename):
    """
    Load organization data from a JSON file.

    Args:
        filename (str): Path to the JSON file.

    Returns:
        list: A list of organization data loaded from the file.
    """
    with open(filename, "r", encoding="utf-8") as file:
        return json.load(file)

def compare_orgs(catalog_orgs, catalog_next_orgs, output_csv_path):
    """
    Compare dataset counts between two catalogs and write a CSV report.

    Args:
        catalog_orgs (list): Current catalog organization data.
        catalog_next_orgs (list): Next catalog organization data.
        output_csv_path (str): Path to the CSV file to be written.

    Returns:
        None
    """
    next_orgs_dict = {
        org['name']: org['package_count'] for org in catalog_next_orgs
    }

    rows = []
    for org in catalog_orgs:
        org_name = org['name']
        org_title = org.get('title', org_name)
        count_current = org['package_count']
        count_next = next_orgs_dict.get(org_name, 0)
        rows.append([org_name, org_title, count_current, count_next])

    rows.sort(key=lambda x: x[2], reverse=True)

    with open(output_csv_path, "w", newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            'Name',
            'Organization',
            'Catalog Dataset Count',
            'Catalog-Next Dataset Count'
        ])
        writer.writerows(rows)

    print(
        f"\n Comparison report saved to: {output_csv_path} "
        f"(Total: {len(rows)} rows)"
    )

def main():
    parser = argparse.ArgumentParser(
        description="Compare dataset counts from two catalogs."
    )
    parser.add_argument(
        "--catalog-url",
        default=DEFAULT_CATALOG_URL,
        help="URL of the current catalog"
    )
    parser.add_argument(
        "--catalog-next-url",
        default=DEFAULT_CATALOG_NEXT_URL,
        help="URL of the next catalog"
    )
    parser.add_argument(
        "--output-csv",
        default=DEFAULT_OUTPUT_CSV,
        help="Output CSV file for comparison"
    )

    args = parser.parse_args()

    if len(sys.argv) == 1:
        print(
            "\n  Using default values "
            "(may be overridden using command-line arguments):"
        )
        print(f"   Current catalog URL: {DEFAULT_CATALOG_URL}")
        print(f"   Next catalog URL:    {DEFAULT_CATALOG_NEXT_URL}")
        print(f"   Output CSV:          {DEFAULT_OUTPUT_CSV}\n")

    output_json_current = DEFAULT_OUTPUT_JSON_CURRENT
    output_json_next = DEFAULT_OUTPUT_JSON_NEXT

    print(
        f"\n Fetching organizations with datasets count from current "
        f"catalog: {args.catalog_url} ..."
    )
    catalog_orgs = fetch_all_organizations(args.catalog_url)
    print(f"    Total organizations fetched from current catalog: "
          f"{len(catalog_orgs)}")
    save_orgs_to_json(catalog_orgs, output_json_current)

    print(
        f"\n Fetching organizations with datasets count from next "
        f"catalog: {args.catalog_next_url} ..."
    )
    catalog_next_orgs = fetch_all_organizations(args.catalog_next_url)
    print(f"    Total organizations fetched from next catalog: "
          f"{len(catalog_next_orgs)}")
    save_orgs_to_json(catalog_next_orgs, output_json_next)

    print("\n Generating datasets count comparison report ...")
    compare_orgs(catalog_orgs, catalog_next_orgs, args.output_csv)

if __name__ == "__main__":
    main()
