import csv
import json
import shutil
from pathlib import Path

CATALOG_PROD_BASE_URL = "https://catalog.data.gov"
CATALOG_NEXT_BASE_URL = "https://catalog-next-dev-datagov.app.cloud.gov"


class OutputBase:
    """Output methods and directory data."""

    def __init__(self, output_dir=None, clear_on_start=True):
        if output_dir is None:
            self.output_dir = Path()
            # no clean up if no output_dir is specified
            return

        self.output_dir = Path(output_dir)
        if clear_on_start and self.output_dir.exists():
            shutil.rmtree(self.output_dir)

        self.output_dir.mkdir(parents=True, exist_ok=True)

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
