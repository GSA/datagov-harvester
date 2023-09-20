import ckanapi


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
