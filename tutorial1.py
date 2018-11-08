#!/usr/bin/env python
import sys

from common import (
    create_bq_client, execute_bq_query, load_gcp_credentials_and_project_from_file
)


def main(gcp_credentials_filepath: str):
    query_sql = """
        SELECT
          id,
          CONCAT(
            'https://stackoverflow.com/questions/',
            CAST(id as STRING)) as url,
          view_count
        FROM `bigquery-public-data.stackoverflow.posts_questions`
        WHERE tags like '%google-bigquery%'
        ORDER BY view_count DESC
        LIMIT 10"""

    gcp_credentials, project_id = load_gcp_credentials_and_project_from_file(
        gcp_credentials_filepath)
    bq_client = create_bq_client(gcp_credentials, project_id)
    results_iter = execute_bq_query(bq_client, query_sql)

    # Print schema **of the results**, not the source table.
    print(results_iter.schema)

    # Evaluate results.
    results = list(results_iter)  # each row is an instance of 'bigquery.Row'

    # note: if the iterator has not been evaluated then 'total_rows' will be None.
    print(results_iter.total_rows)

    for row in results:
        print("{} - {} : {} views".format(row.id, row.url, row.view_count))


if __name__ == '__main__':
    main(sys.argv[1])
