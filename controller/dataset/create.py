import uuid

from controller.dataset import bp, db

test_dataset = {
    "accessLevel": "public",
    "landingPage": "https://data.ny.gov/d/22ew-dxez",
    "issued": "2016-07-21",
    "@type": "dcat:Dataset",
    "modified": "2019-06-10",
    "publisher": {"@type": "org:Organization", "name": "State of New York"},
    "identifier": "https://data.ny.gov/api/views/22ew-dxez",
    "description": "Financial and Geographic Information on SONYMA...",
    "title": "State of New York Mortgage Agency (SONYMA) Loans",
}


# just for testing purpose
@bp.route("/create/", methods=["GET"])
def create():
    id = str(uuid.uuid4())
    db[id] = test_dataset
    return {"dataset": db[id], "dataset_id": id}
