from datagovharvester.validate.dcat_us import validate_json_schema


def test_numerical_title(open_catalog_schema, open_numerical_title_json):
    dataset_schema = open_catalog_schema
    json_data = open_numerical_title_json
    valid_flag = False

    assert validate_json_schema(json_data, dataset_schema, valid_flag)


def test_collection_1_parent_2_children(
    open_catalog_schema, open_collection_1_parent_2_children_json
):
    dataset_schema = open_catalog_schema
    json_data = open_collection_1_parent_2_children_json
    valid_flag = True

    assert validate_json_schema(json_data, dataset_schema, valid_flag)


def test_missing_catalog(open_catalog_schema, open_missing_catalog_json):
    dataset_schema = open_catalog_schema
    json_data = open_missing_catalog_json
    valid_flag = False

    assert validate_json_schema(json_data, dataset_schema, valid_flag)


def test_ny(open_catalog_schema, open_ny_json):
    dataset_schema = open_catalog_schema
    json_data = open_ny_json
    valid_flag = False

    assert validate_json_schema(json_data, dataset_schema, valid_flag)


def test_missing_identifier_title(
    open_catalog_schema, open_missing_identifier_title_json
):
    dataset_schema = open_catalog_schema
    json_data = open_missing_identifier_title_json
    valid_flag = False

    assert validate_json_schema(json_data, dataset_schema, valid_flag)


def test_usda_gov(open_catalog_schema, open_usda_gov_json):
    dataset_schema = open_catalog_schema
    json_data = open_usda_gov_json
    valid_flag = True

    assert validate_json_schema(json_data, dataset_schema, valid_flag)


def test_arm(open_catalog_schema, open_arm_json):
    dataset_schema = open_catalog_schema
    json_data = open_arm_json
    valid_flag = True

    assert validate_json_schema(json_data, dataset_schema, valid_flag)


def test_large_spatial(open_catalog_schema, open_large_spatial_json):
    dataset_schema = open_catalog_schema
    json_data = open_large_spatial_json
    valid_flag = True

    assert validate_json_schema(json_data, dataset_schema, valid_flag)


def test_reserved_title(open_catalog_schema, open_reserved_title_json):
    dataset_schema = open_catalog_schema
    json_data = open_reserved_title_json
    valid_flag = True

    assert validate_json_schema(json_data, dataset_schema, valid_flag)


def test_collection_2_parent_4_children(
    open_catalog_schema, open_collection_2_parent_4_children_json
):
    dataset_schema = open_catalog_schema
    json_data = open_collection_2_parent_4_children_json
    valid_flag = True

    assert validate_json_schema(json_data, dataset_schema, valid_flag)


def test_geospatial(open_catalog_schema, open_geospatial_json):
    dataset_schema = open_catalog_schema
    json_data = open_geospatial_json
    valid_flag = True

    assert validate_json_schema(json_data, dataset_schema, valid_flag)


def test_null_spatial(open_catalog_schema, open_null_spatial_json):
    dataset_schema = open_catalog_schema
    json_data = open_null_spatial_json
    valid_flag = True

    assert validate_json_schema(json_data, dataset_schema, valid_flag)
