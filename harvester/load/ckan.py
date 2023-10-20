import ckanapi
import re


def create_ckan_extra_base(*args):
    keys = ["publisher_hierarchy", "resource-type", "publisher"]
    data = zip(keys, args)
    return [{"key": d[0], "value": d[1]} for d in data]


def create_ckan_extras_additions(dcatus_catalog, additions):
    extras = [
        "accessLevel",
        "bureauCode",
        "identifier",
        "modified",
        "programCode",
        "publisher",
    ]

    output = []

    for extra in extras:
        data = {"key": extra, "value": None}
        if extra == "publisher":
            data["value"] = dcatus_catalog[extra]["name"]
        else:
            data["value"] = dcatus_catalog[extra]
        output.append(data)

    return output + additions


def create_ckan_tags(keywords):
    output = []

    for keyword in keywords:
        keyword = "-".join(keyword.split())
        output.append({"name": keyword})

    return output


def create_ckan_publisher_hierarchy(pub_dict, data=[]):
    for k, v in pub_dict.items():
        if k == "name":
            data.append(v)
        if isinstance(v, dict):
            create_ckan_publisher_hierarchy(v, data)

    return " > ".join(data[::-1])


def get_email_from_str(in_str):
    res = re.search(r"[\w.+-]+@[\w-]+\.[\w.-]+", in_str)
    if res is not None:
        return res.group(0)


def create_ckan_resources(dists):
    output = []

    for dist in dists:
        url_key = "downloadURL" if "downloadURL" in dist else "accessURL"
        resource = {"url": dist[url_key], "mimetype": dist["mediaType"]}
        output.append(resource)

    return output


def simple_transform(dcatus_catalog):
    output = {
        "name": "-".join(dcatus_catalog["title"].lower().split()),
        "owner_org": "test",
    }

    mapping = {
        "contactPoint": {"fn": "maintainer", "hasEmail": "maintainer_email"},
        "description": "notes",
        "title": "title",
    }

    for k, v in dcatus_catalog.items():
        if k not in mapping:
            continue
        if isinstance(mapping[k], dict):
            temp = {}
            for k2, v2 in v.items():
                if k2 == "hasEmail":
                    v2 = get_email_from_str(v2)
                temp[mapping[k][k2]] = v2
            output = {**output, **temp}
        else:
            output[mapping[k]] = v

    return output


def create_defaults():
    return {
        "author": None,
        "author_email": None,
    }


def dcatus_to_ckan(dcatus_catalog):
    """
    example:
    - from this:
        - https://catalog.data.gov/harvest/object/cb22fea9-0c90-43e9-94bf-903eacd37c92
    - to this:
        - https://catalog.data.gov/api/action/package_show?id=fdic-failed-bank-list

    """

    output = simple_transform(dcatus_catalog)

    resources = create_ckan_resources(dcatus_catalog["distribution"])
    tags = create_ckan_tags(dcatus_catalog["keyword"])
    pubisher_hierarchy = create_ckan_publisher_hierarchy(dcatus_catalog["publisher"])

    extras_base = create_ckan_extra_base(
        pubisher_hierarchy, "Dataset", dcatus_catalog["publisher"]["name"]
    )
    extras = create_ckan_extras_additions(dcatus_catalog, extras_base)

    defaults = create_defaults()

    output["resources"] = resources
    output["tags"] = tags
    output["extras"] = extras_base
    output["extras"] += extras

    return {**output, **defaults}


def create_ckan_entrypoint(url, api_key):
    return ckanapi.RemoteCKAN(url, apikey=api_key)


def create_ckan_package(ckan, package_data):
    return ckan.action.package_create(**package_data)


def patch_ckan_package(ckan, patch_data):
    # partially updates the package
    return ckan.action.package_patch(**patch_data)


def update_ckan_package(ckan, update_data):
    # fully replaces the package
    return ckan.action.package_update(**update_data)


def purge_ckan_package(ckan, package_data):
    return ckan.action.dataset_purge(**package_data)
