
import csv
import json

from pathlib import Path


CATALOG_PROD_BASE_URL = "https://catalog.data.gov"
CATALOG_NEXT_BASE_URL = "https://catalog-next-dev-datagov.app.cloud.gov"


class OutputBase:

    """Output methods and directory data."""

    def __init__(self, output_dir=Path()):
        self.output_dir = Path(output_dir)


    def write_to_csv(self, file_path: str, data: list) -> str:
        with open(self.output_dir / file_path, "w") as file:
            writer = csv.writer(file)
            writer.writerows(data)

        return file_path


    def write_to_json(self, file_path: str, data: dict) -> str:
        with open(self.output_dir / file_path, "w") as file:
            json.dump(data, file)
        return file_path


    def write_to_file(self, file_path: str, contents: str) -> str:
        with open(self.output_dir / file_path, "w") as file:
            file.write(contents)
        return file_path
