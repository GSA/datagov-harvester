# datagov-harvester

This repository holds the source code the Data.gov Harvester 2.0, which includes three applications applications:

- datagov-harvest-runner: This is a python application, chiefly composed of files in the `harvester` directory.

- datagov-harvest-admin: This is a Flask app which manages the configuration of harvest sources, organizations, and the creation of harvest jobs.

- datagov-harvest-proxy: This is an nginx app which owns the public route and proxies traffic to the internal Flask app route.

There is further documentation in the [developer](/docs/developer.md) quickstart.


## Documentation

 - [Developer setup](docs/developer.md)
 - [Harvester wiki](https://github.com/GSA/data.gov/wiki/harvest.data.gov)
 - The latest sequence diagrams are available in the [/docs/diagrams/mermaid/dest](docs/diagrams/mermaid/dest) folder. (Click "Raw" for a full-page view of any diagram.)
 - The [docs](docs) folder in general.
 - Application specific documentation [below](#applications).

Additional background for team members on Google Drive (not publicly accessible):
 - [Harvester 2.0 folder](https://drive.google.com/drive/folders/11mhCBb9vWxrTHV_s_S4pZhhI2fGHmxrq)
 - Historic documentation [Archive folder](https://drive.google.com/drive/folders/1DG1oxSCoeru2-bbmSfcBnIiCZUg-N1Az), along with [historic diagrams](https://drive.google.com/drive/folders/1GJZYVVMubGG54d5yCZY9zoea3xjZ9HNI)


## Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for additional information.

## Public domain

This project is in the worldwide [public domain](LICENSE.md). As stated in [CONTRIBUTING](CONTRIBUTING.md):

> This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).
>
> All contributions to this project will be released under the CC0 dedication. By submitting a pull request, you are agreeing to comply with this waiver of copyright interest.
