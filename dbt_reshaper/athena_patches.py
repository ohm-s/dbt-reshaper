from dbt.adapters.athena.connections import AthenaCursor
from dbt.adapters.athena.impl import AthenaAdapter
from dbt.contracts.connection import AdapterResponse
from dbt.adapters.base import Column
from dbt.events import AdapterLogger
from dbt.exceptions import RuntimeException
from dbt.adapters.athena.config import get_boto3_config
from threading import Lock
from glueduck import aws_helper

# Add initial value to the AdapterResponse class
AdapterResponse.data_scanned_in_bytes = None

logger = AdapterLogger("Athena")

### First patch Add delete from s3 to dbt 1.3
def delete_from_s3_override(self, s3_path):
    """
    Deletes files from s3 given a s3 path in the format: s3://my_bucket/prefix
    Additionally, parses the response from the s3 delete request and raises
    a DbtRuntimeError in case it included errors.
    """
    conn = self.connections.get_thread_connection()
    client = conn.handle
    bucket_name, prefix = self._parse_s3_path(s3_path)
    if self._s3_path_exists(client, bucket_name, prefix):
        s3_resource = client.session.resource("s3", region_name=client.region_name, config=get_boto3_config())
        s3_bucket = s3_resource.Bucket(bucket_name)
        logger.debug(f"Deleting table data: path='{s3_path}', bucket='{bucket_name}', prefix='{prefix}'")
        response = s3_bucket.objects.filter(Prefix=prefix).delete()
        is_all_successful = True
        for res in response:
            if "Errors" in res:
                for err in res["Errors"]:
                    is_all_successful = False
                    logger.error(
                        "Failed to delete files: Key='{}', Code='{}', Message='{}', s3_bucket='{}'",
                        err["Key"],
                        err["Code"],
                        err["Message"],
                        bucket_name,
                    )
        if is_all_successful is False:
            raise RuntimeException("Failed to delete files from S3.")
    else:
        logger.debug("S3 path does not exist")
AthenaAdapter.delete_from_s3 = delete_from_s3_override

### 2nd Patch: Collecting all queries and their data scanned in bytes in one variables
AthenaAdapter.list_of_all_queries = {}
lock = Lock()
_original_collect_result_set = AthenaCursor._collect_result_set
def _collect_result_set(self, query_id: str):
    response = _original_collect_result_set(self, query_id)
    if response is not None:
        data_scanned = response.data_scanned_in_bytes
        with lock:
            AthenaAdapter.list_of_all_queries[query_id] = data_scanned
    return response
AthenaCursor._collect_result_set = _collect_result_set

# 3rd Patch: Getting all columns of table from Glue Catalog instead of information schema (which is very slow)
def get_columns_in_relation(self, relation):
    glue = aws_helper.get_thread_safe_client('glue')
    response = aws_helper.retry_api_call(lambda: glue.get_table(DatabaseName=relation.schema, Name=relation.name))
    columns = []
    for column in response['Table']['StorageDescriptor']['Columns']:
        column = Column.from_description(column['Name'], column['Type'])
        columns.append(column)
    for column in response['Table']['PartitionKeys']:
        column = Column.from_description(column['Name'], column['Type'])
        columns.append(column)
    return columns

AthenaAdapter.get_columns_in_relation = get_columns_in_relation
