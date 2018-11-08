#!/usr/bin/env python
import sys

from google.cloud.bigquery import SchemaField

from common import (
    create_bq_client, create_bq_table, get_bq_dataset, load_gcp_credentials_and_project_from_file,
)


def main(gcp_credentials_filepath: str):
    gcp_credentials, project_id = load_gcp_credentials_and_project_from_file(
        gcp_credentials_filepath)
    bq_client = create_bq_client(gcp_credentials, project_id)

    dataset = get_bq_dataset(bq_client, 'dataset_x_1')

    # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
    table_schema = [
        SchemaField('field_INT64', 'INT64', 'NULLABLE'),
        SchemaField('field_NUMERIC', 'NUMERIC', 'NULLABLE'),
        SchemaField('field_FLOAT64', 'FLOAT64', 'NULLABLE'),
        SchemaField('field_BOOL', 'BOOL', 'NULLABLE'),
        SchemaField('field_STRING', 'STRING', 'NULLABLE'),
        SchemaField('field_BYTES', 'BYTES', 'NULLABLE'),
        SchemaField('field_DATE', 'DATE', 'NULLABLE'),
        SchemaField('field_DATETIME', 'DATETIME', 'NULLABLE'),
        SchemaField('field_TIME', 'TIME', 'NULLABLE'),
        SchemaField('field_TIMESTAMP', 'TIMESTAMP', 'NULLABLE'),
    ]

    table = create_bq_table(
        bq_client, dataset, table_id='table_2', table_schema=table_schema,
        table_description='some table description')

    print("table fully-qualified ID:", table.full_table_id)
    print("table GRN:", table.path)
    print("table description:", table.description)
    print("table type:", table.table_type)
    print("table size [MiB]:", table.num_bytes / 1014)
    print("table size [rows]:", table.num_rows)
    print("table schema fields:")
    for _field in table.schema:
        print("\t", _field)
    print("table API representation:", table.to_api_repr())


if __name__ == '__main__':
    main(sys.argv[1])
