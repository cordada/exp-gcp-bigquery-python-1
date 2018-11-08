#!/usr/bin/env python
import sys

from common import (
    create_bq_client, get_bq_dataset, get_bq_table, load_gcp_credentials_and_project_from_file,
)


def main(gcp_credentials_filepath: str):
    gcp_credentials, project_id = load_gcp_credentials_and_project_from_file(
        gcp_credentials_filepath)
    bq_client = create_bq_client(gcp_credentials, project_id)

    dataset = get_bq_dataset(
        bq_client, dataset_id='samples', project_id='bigquery-public-data')
    table = get_bq_table(
        bq_client, dataset_id='samples', table_id='shakespeare', project_id='bigquery-public-data')

    print("dataset fully-qualified ID:", dataset.full_dataset_id)
    print("dataset GRN:", dataset.path)
    print("dataset description:", dataset.description)
    # print("dataset API representation:", dataset.to_api_repr())

    print("table fully-qualified ID:", table.full_table_id)
    print("table GRN:", table.path)
    print("table description:", table.description)
    print("table type:", table.table_type)
    print("table size [MiB]:", table.num_bytes / 1014)
    print("table size [rows]:", table.num_rows)
    print("table schema fields:")
    for _field in table.schema:
        print("\t", _field)
    # print("table API representation:", table.to_api_repr())


if __name__ == '__main__':
    main(sys.argv[1])
