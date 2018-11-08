#!/usr/bin/env python
import sys

from common import (
    create_bq_client, create_bq_dataset, load_gcp_credentials_and_project_from_file,
)


def main(gcp_credentials_filepath: str):
    gcp_credentials, project_id = load_gcp_credentials_and_project_from_file(
        gcp_credentials_filepath)
    bq_client = create_bq_client(gcp_credentials, project_id)

    dataset = create_bq_dataset(
        bq_client, 'dataset_x_1', dataset_description='some description')

    print("dataset fully-qualified ID:", dataset.full_dataset_id)
    print("dataset GRN:", dataset.path)
    print("dataset description:", dataset.description)
    print("dataset API representation:", dataset.to_api_repr())


if __name__ == '__main__':
    main(sys.argv[1])
