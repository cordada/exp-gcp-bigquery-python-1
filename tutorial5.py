#!/usr/bin/env python
import math
import sys
from datetime import date, datetime
from decimal import Decimal

from common import (
    create_bq_client, get_bq_table, load_gcp_credentials_and_project_from_file, execute_bq_query,
)


def main(gcp_credentials_filepath: str):
    gcp_credentials, project_id = load_gcp_credentials_and_project_from_file(
        gcp_credentials_filepath)
    bq_client = create_bq_client(gcp_credentials, project_id)

    table = get_bq_table(bq_client, 'dataset_x_1', 'table_2')

    # note: to generate a valid value for table field type 'TIME' we tried we a few functions in
    #   :mod:`time` but had no luck.
    rows_to_insert = [
        (
            24 * 38,  # SchemaField('field_INT64', 'INT64', 'NULLABLE'),
            Decimal('20.83'),  # SchemaField('field_NUMERIC', 'NUMERIC', 'NULLABLE'),
            math.e ** math.pi,  # SchemaField('field_FLOAT64', 'FLOAT64', 'NULLABLE'),
            True,  # SchemaField('field_BOOL', 'BOOL', 'NULLABLE'),
            "Jürgen loves Ω! ✔ \n\r\t 123",  # SchemaField('field_STRING', 'STRING', 'NULLABLE'),
            b'SsO8cmdlbiBsb3ZlcyDOqSEg4pyUIAoNCSAxMjM=',  # SchemaField('field_BYTES', 'BYTES', 'NULLABLE'),
            date.today(),  # SchemaField('field_DATE', 'DATE', 'NULLABLE'),
            datetime.now(),  # SchemaField('field_DATETIME', 'DATETIME', 'NULLABLE'),
            datetime.now().time(),  # SchemaField('field_TIME', 'TIME', 'NULLABLE'),
            datetime.now(),  # SchemaField('field_TIMESTAMP', 'TIMESTAMP', 'NULLABLE'),
        ),
    ]

    # warning: 'insert_rows' performs serialization to JSON-compatible native Python types,
    #   and then calls 'insert_rows_json'. Obviously this impacts performance.
    # warning: if 'insert_rows' does not raise any exceptions it **does not** mean the insertion
    #   succeeded.
    # warning: this kind of insert is actually implemented as **Streaming Data into BigQuery**
    #   https://cloud.google.com/bigquery/streaming-data-into-bigquery
    # TODO: insertion using load jobs
    #   https://cloud.google.com/bigquery/docs/loading-data-local#loading_data_from_a_local_data_source
    #   https://github.com/googleapis/google-cloud-python/blob/master/bigquery/docs/snippets.py
    #   https://googleapis.github.io/google-cloud-python/latest/bigquery/usage/tables.html#creating-a-table
    #   https://googleapis.github.io/google-cloud-python/latest/bigquery/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.load_table_from_file
    errors = bq_client.insert_rows(
        table, rows_to_insert, skip_invalid_rows=False, ignore_unknown_values=False)
    if errors:
        print(errors)
        raise Exception

    query_sql = """
        SELECT
          *
        FROM `dataset_x_1.table_2`
        WHERE true
        LIMIT 10"""

    results_iter = execute_bq_query(bq_client, query_sql)

    # Print schema **of the results**, not the source table.
    print(results_iter.schema)

    # Evaluate results.
    results = list(results_iter)  # each row is an instance of 'bigquery.Row'

    # note: if the iterator has not been evaluated then 'total_rows' will be None.
    print(results_iter.total_rows)

    for row in results:
        print(list(row.items()))


if __name__ == '__main__':
    main(sys.argv[1])
