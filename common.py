from typing import Tuple

import google.auth
from google.auth.credentials import Credentials as GcpCredentials
from google.cloud import bigquery


def load_gcp_credentials_and_project_from_file(filename: str) -> Tuple[GcpCredentials, str]:
    credentials, project_id = google.auth._default._load_credentials_from_file(filename)
    return credentials, project_id


def create_bq_client(gcp_credentials: GcpCredentials, project_id: str) -> bigquery.Client:
    return bigquery.Client(credentials=gcp_credentials, project=project_id)


def execute_bq_query(
    client: bigquery.Client,
    query_sql: str,
    query_params: list =None,
    dry_run: bool =False,
) -> bigquery.table.RowIterator:
    """
    Execute bq query and return results as a row iterator.

    """
    if query_params is not None:
        # TODO: implement. See
        #   https://cloud.google.com/bigquery/docs/parameterized-queries
        #   https://googleapis.github.io/google-cloud-python/latest/bigquery/usage/queries.html#run-a-query-using-a-named-query-parameter
        raise NotImplementedError
    if dry_run:
        # TODO: implement. See
        #   https://googleapis.github.io/google-cloud-python/latest/bigquery/usage/queries.html#run-a-dry-run-query
        raise NotImplementedError

    query_job = client.query(query_sql)  # type: bigquery.QueryJob

    # API request; waits for job to complete.
    # TODO: is all results' data retrieved here? Or will some be retrieved later, when the iterator
    #   is consumed?
    return query_job.result()  # type: bigquery.table.RowIterator

