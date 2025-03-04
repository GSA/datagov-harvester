import re
import uuid

from harvester.harvest import HarvestSource

# all of these are copy/pasted from ckan core
# https://github.com/ckan/ckan/blob/master/ckan/lib/munge.py

PACKAGE_NAME_MAX_LENGTH = 90
PACKAGE_NAME_MIN_LENGTH = 2

MAX_TAG_LENGTH = 100
MIN_TAG_LENGTH = 2


def _munge_to_length(string: str, min_length: int, max_length: int) -> str:
    """Pad/truncates a string"""
    if len(string) < min_length:
        string += "_" * (min_length - len(string))
    if len(string) > max_length:
        string = string[:max_length]
    return string


def substitute_ascii_equivalents(text_unicode: str) -> str:
    # Method taken from: http://code.activestate.com/recipes/251871/
    """
    This takes a UNICODE string and replaces Latin-1 characters with something
    equivalent in 7-bit ASCII. It returns a plain ASCII string. This function
    makes a best effort to convert Latin-1 characters into ASCII equivalents.
    It does not just strip out the Latin-1 characters. All characters in the
    standard 7-bit ASCII range are preserved. In the 8th bit range all the
    Latin-1 accented letters are converted to unaccented equivalents. Most
    symbol characters are converted to something meaningful. Anything not
    converted is deleted.
    """
    char_mapping = {
        0xC0: "A",
        0xC1: "A",
        0xC2: "A",
        0xC3: "A",
        0xC4: "A",
        0xC5: "A",
        0xC6: "Ae",
        0xC7: "C",
        0xC8: "E",
        0xC9: "E",
        0xCA: "E",
        0xCB: "E",
        0xCC: "I",
        0xCD: "I",
        0xCE: "I",
        0xCF: "I",
        0xD0: "Th",
        0xD1: "N",
        0xD2: "O",
        0xD3: "O",
        0xD4: "O",
        0xD5: "O",
        0xD6: "O",
        0xD8: "O",
        0xD9: "U",
        0xDA: "U",
        0xDB: "U",
        0xDC: "U",
        0xDD: "Y",
        0xDE: "th",
        0xDF: "ss",
        0xE0: "a",
        0xE1: "a",
        0xE2: "a",
        0xE3: "a",
        0xE4: "a",
        0xE5: "a",
        0xE6: "ae",
        0xE7: "c",
        0xE8: "e",
        0xE9: "e",
        0xEA: "e",
        0xEB: "e",
        0xEC: "i",
        0xED: "i",
        0xEE: "i",
        0xEF: "i",
        0xF0: "th",
        0xF1: "n",
        0xF2: "o",
        0xF3: "o",
        0xF4: "o",
        0xF5: "o",
        0xF6: "o",
        0xF8: "o",
        0xF9: "u",
        0xFA: "u",
        0xFB: "u",
        0xFC: "u",
        0xFD: "y",
        0xFE: "th",
        0xFF: "y",
        # 0xa1: '!', 0xa2: '{cent}', 0xa3: '{pound}', 0xa4: '{currency}',
        # 0xa5: '{yen}', 0xa6: '|', 0xa7: '{section}', 0xa8: '{umlaut}',
        # 0xa9: '{C}', 0xaa: '{^a}', 0xab: '<<', 0xac: '{not}',
        # 0xad: '-', 0xae: '{R}', 0xaf: '_', 0xb0: '{degrees}',
        # 0xb1: '{+/-}', 0xb2: '{^2}', 0xb3: '{^3}', 0xb4:"'",
        # 0xb5: '{micro}', 0xb6: '{paragraph}', 0xb7: '*', 0xb8: '{cedilla}',
        # 0xb9: '{^1}', 0xba: '{^o}', 0xbb: '>>',
        # 0xbc: '{1/4}', 0xbd: '{1/2}', 0xbe: '{3/4}', 0xbf: '?',
        # 0xd7: '*', 0xf7: '/'
    }

    r = ""
    for char in text_unicode:
        if ord(char) in char_mapping:
            r += char_mapping[ord(char)]
        elif ord(char) >= 0x80:
            pass
        else:
            r += str(char)
    return r


def munge_title_to_name(name: str) -> str:
    """Munge a package title into a package name."""
    name = substitute_ascii_equivalents(name)
    # convert spaces and separators
    name = re.sub("[ .:/]", "-", name)
    # take out not-allowed characters
    name = re.sub("[^a-zA-Z0-9-_]", "", name).lower()
    # remove doubles
    name = re.sub("-+", "-", name)
    # remove leading or trailing hyphens
    name = name.strip("-")
    # if longer than max_length, keep last word if a year
    max_length = PACKAGE_NAME_MAX_LENGTH
    # (make length less than max, in case we need a few for '_' chars
    # to de-clash names.)
    if len(name) > max_length:
        year_match = re.match(r".*?[_-]((?:\d{2,4}[-/])?\d{2,4})$", name)
        if year_match:
            year = year_match.groups()[0]
            name = "%s-%s" % (name[: (max_length - len(year) - 1)], year)
        else:
            name = name[:max_length]
    name = _munge_to_length(name, PACKAGE_NAME_MIN_LENGTH, PACKAGE_NAME_MAX_LENGTH)
    return name


def munge_tag(tag: str) -> str:
    tag = substitute_ascii_equivalents(tag)
    tag = tag.lower().strip()
    tag = re.sub(r"[^a-zA-Z0-9\- ]", "", tag).replace(" ", "-")
    tag = _munge_to_length(tag, MIN_TAG_LENGTH, MAX_TAG_LENGTH)
    return tag


def create_ckan_extras(
    metadata: dict, harvest_source: HarvestSource, record_id: str
) -> list[dict]:
    extras = [
        "accessLevel",
        "bureauCode",
        "identifier",
        "modified",
        "programCode",
        "publisher",
    ]

    output = [
        {"key": "resource-type", "value": "Dataset"},
        {"key": "harvest_object_id", "value": record_id},
        {
            "key": "source_datajson_identifier",  # dataset is datajson format or not
            "value": True,
        },
        {
            "key": "harvest_source_id",
            "value": harvest_source.id,
        },
        {
            "key": "harvest_source_title",
            "value": harvest_source.name,
        },
    ]

    for extra in extras:
        if extra not in metadata:
            continue
        data = {"key": extra, "value": None}
        val = metadata[extra]
        if extra == "publisher":
            data["value"] = val["name"]

            output.append(
                {
                    "key": "publisher_hierarchy",
                    "value": create_ckan_publisher_hierarchy(val, []),
                }
            )

        else:
            if isinstance(val, list):  # TODO: confirm this is what we want.
                val = val[0]
            data["value"] = val
        output.append(data)

    # TODO: update this
    # output.append(
    #     {
    #         "key": "dcat_metadata",
    #         "value": str(sort_dataset(self.metadata)),
    #     }
    # )

    # output.append(
    #     {
    #         "key": self.harvest_source.extra_source_name,
    #         "value": self.harvest_source.title,
    #     }
    # )

    output.append({"key": "identifier", "value": metadata["identifier"]})

    return output


def create_ckan_tags(keywords: list[str]) -> list:
    output = []

    for keyword in keywords:
        output.append({"name": munge_tag(keyword)})

    return output


def create_ckan_publisher_hierarchy(pub_dict: dict, data: list = []) -> str:
    for k, v in pub_dict.items():
        if k == "name":
            data.append(v)
        if isinstance(v, dict):
            create_ckan_publisher_hierarchy(v, data)

    return " > ".join(data[::-1])


def get_email_from_str(in_str: str) -> str:
    res = re.search(r"[\w.+-]+@[\w-]+\.[\w.-]+", in_str)
    if res is not None:
        return res.group(0)


def create_ckan_resources(metadata: dict) -> list[dict]:
    output = []

    if "distribution" not in metadata or metadata["distribution"] is None:
        return output

    for dist in metadata["distribution"]:
        url_keys = ["downloadURL", "accessURL"]
        for url_key in url_keys:
            if dist.get(url_key, None) is None:
                continue
            resource = {"url": dist[url_key]}
            if "mimetype" in dist:
                resource["mimetype"] = dist["mediaType"]

        output.append(resource)

    return output


def simple_transform(metadata: dict, owner_org: str) -> dict:
    output = {
        "name": munge_title_to_name(metadata["title"]),
        "owner_org": owner_org,
        "identifier": metadata["identifier"],
        "author": None,  # TODO: CHANGE THIS!
        "author_email": None,  # TODO: CHANGE THIS!
    }

    mapping = {
        "contactPoint": {"fn": "maintainer", "hasEmail": "maintainer_email"},
        "description": "notes",
        "title": "title",
    }

    for k, v in metadata.items():
        if k not in mapping:
            continue
        if isinstance(mapping[k], dict):
            temp = {}
            to_skip = ["@type"]
            for k2, v2 in v.items():
                if k2 == "hasEmail":
                    v2 = get_email_from_str(v2)
                if k2 in to_skip:
                    continue
                temp[mapping[k][k2]] = v2
            output = {**output, **temp}
        else:
            output[mapping[k]] = v

    return output


def ckanify_dcatus(
    metadata: dict, harvest_source: HarvestSource, record_id: str
) -> dict:
    ckanified_metadata = simple_transform(metadata, harvest_source.organization_id)

    ckanified_metadata["resources"] = create_ckan_resources(metadata)
    ckanified_metadata["tags"] = (
        create_ckan_tags(metadata["keyword"]) if "keyword" in metadata else []
    )
    ckanified_metadata["extras"] = create_ckan_extras(
        metadata, harvest_source, record_id
    )

    return ckanified_metadata


def add_uuid_to_package_name(name: str) -> str:
    return name + "-" + str(uuid.uuid4())[:5]
