from .modules.dbt_glueduck import _instance
from . import reshaper_logs
def configure(aws_region, output_iam_role, output_s3_bucket, output_database, duckdb_modules):
  _instance.configure(
    aws_region=aws_region,
    iam_role=output_iam_role,
    temp_results_bucket=output_s3_bucket,
    glue_temp_results_database=output_database,
    modules=duckdb_modules
  )
  _instance.log = lambda x: reshaper_logs.fire_info_event('DDB-LG', x)
  _instance.log_error = lambda x: reshaper_logs.fire_error_event('DDB-ER', x)

