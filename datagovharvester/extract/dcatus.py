import json

from datagovharvester import content_types, extract_feat_name
from datagovharvester.utils.s3_utilities import create_s3_upload_data

# ruff: noqa: F841


def parse_catalog(catalog, job_info):
    """parse the catalog and yield each record as an S3 data upload dict
    catalog (dict)  :   dcatus catalog json
    job_info (dict) :   info on the job ( e.g. source_id, job_id, url )
    """
    for idx, record in enumerate(catalog["dataset"]):
        try:
            record = json.dumps(record)
            key_name = f"{extract_feat_name}/{job_info['source_id']}/{job_info['job_id']}/{idx}.json"  # noqa: E501
            yield create_s3_upload_data(record, key_name, content_types["json"])
        except Exception as e:
            pass
