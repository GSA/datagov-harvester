# TODO: maybe turn off this ruff ignore?
# ruff: noqa: F405, F403

__all__ = [
    "compare",
    "extract",
    "traverse_waf",
    "download_waf",
    "load",
    "create_ckan_package",
    "update_ckan_package",
    "patch_ckan_package",
    "purge_ckan_package",
    "dcatus_to_ckan",
    "transform",
    "validate",
    "utils",
]

from dotenv import load_dotenv

# TODO these imports will need to be updated to ensure a consistent api
from .compare import compare
from .extract import download_waf, extract, traverse_waf
from .load import (create_ckan_package, dcatus_to_ckan, load,
                   patch_ckan_package, purge_ckan_package, update_ckan_package)
from .transform import transform
from .utils import *
from .validate import *

load_dotenv()

# configuration settings
bucket_name = "test-bucket"
content_types = {
    "json": "application/json",
}
extract_feat_name = "extract"
