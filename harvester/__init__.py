# configuration settings
bucket_name = "test-bucket"
content_types = {
    "json": "application/json",
}
extract_feat_name = "extract"

from . import compare, extract, load, transform
from .validate import dcat_us
from .utils import json
