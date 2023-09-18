from duckdb import connect
from . import aws_helper
import datetime
import threading
from . import glue_helper
import time

duck_instance_lock = threading.Lock()

class DuckDB:

  temp_tables_lock = threading.Lock()
  temp_tables = []
  aws_region = None
  _duckdb_instance = None
  temp_results_bucket = None
  iam_role = None
  iam_role_name = None
  glue_temp_results_database = None
  connection_type = None
  modules = []

  def __init__(self):
    pass

  def log(self, message):
    pass

  def log_error(self, err):
    pass

  def configure(self, aws_region, temp_results_bucket, glue_temp_results_database, iam_role, iam_role_name = 'glueduck', connection_type = ':memory:', modules = []):
    self.aws_region = aws_region
    self.iam_role = iam_role
    self.iam_role_name = iam_role_name
    self.connection_type = connection_type
    self.modules = modules
    self.glue_temp_results_database = glue_temp_results_database
    self.temp_results_bucket = temp_results_bucket.rstrip("/")

  def _retry_create_duckdb_instance(self):
    retry_counter = 0
    while True:
        try:
            duckdb_instance = connect(self.connection_type)
            for module in self.modules:
              duckdb_instance.query(f"INSTALL {module}")
              duckdb_instance.query(f"LOAD {module}")
            self._duckdb_instance = duckdb_instance
            return duckdb_instance
        except Exception as e:
            if retry_counter > 7:
              self.log_error(str(e))
              raise e
            retry_counter += 1
            import time, random
            timer = random.randint(23, 999)
            time.sleep(timer / 1000)


  def _get_duckdb_instance(self):
    if self._duckdb_instance is None:
      with duck_instance_lock:
        if self._duckdb_instance is None:
          self._retry_create_duckdb_instance()
    return self._duckdb_instance

  def execute_query(self, query):
    return self._get_duckdb_instance().cursor().query(query)

  def info_schema(self, table_name):
    res = self.execute_query(f"select column_name, data_type, character_maximum_length,numeric_precision,numeric_scale,datetime_precision from information_schema.columns where table_name = '{table_name}'")
    columns_info = res.fetchall()
    return columns_info

  def get_cursor_with_s3_credentials(self):
    sts = aws_helper.get_thread_safe_client('sts')
    roleCreds = aws_helper.retry_api_call(lambda: sts.assume_role(RoleArn=self.iam_role,RoleSessionName=self.iam_role_name))
    access_key = roleCreds['Credentials']['AccessKeyId']
    secret_key = roleCreds['Credentials']['SecretAccessKey']
    session_token = roleCreds['Credentials']['SessionToken']
    cursor = self._get_duckdb_instance().cursor()
    cursor.execute(f"set s3_region='{self.aws_region}'")
    cursor.execute(f"SET s3_secret_access_key='{secret_key}'")
    cursor.execute(f"SET s3_access_key_id='{access_key}'")
    cursor.execute(f"SET s3_session_token='{session_token}'")
    return cursor

  def append_temp_table(self, table_name):
    with self.temp_tables_lock:
      self.temp_tables.append(table_name)

  def drop_temp_tables(self):
    with self.temp_tables_lock:
      glue = aws_helper.get_thread_safe_client('glue')
      for table in self.temp_tables:
        def drop_table():
          return glue.delete_table(
            DatabaseName=self.glue_temp_results_database,
            Name=table
          )
        aws_helper.retry_api_call(drop_table)

      self.temp_tables = []


  def append_df_to_table(self, table_name, dataframe):
    self._get_duckdb_instance().cursor().append(table_name, dataframe)

  def query_to_glue_table(self, query):
    try:
      name, location = self.create_temp_table_name_and_location()
      converted_query = f"copy ({query} ) to '{location}/result.parquet'"
      cursor = self.get_cursor_with_s3_credentials()
      cursor.execute(converted_query)
      cursor.execute(f"create view {name} as select * from read_parquet('{self.temp_results_bucket}/{name}/result.parquet')")
      columns = self.info_schema(str(name))
      return self.create_temporary_glue_table_from_parquet(name, columns)
    except Exception as e:
      self.log_error(str(e))
      raise e

  def create_glue_table_from_parquet(self, glue_database, name, columns, location):
    glue_helper.create_glue_table(glue_database, name, columns, location)
    return f'{glue_database}.{name}'

  def create_temporary_glue_table_from_parquet(self, name, columns):
    self.log(f'Creating temporary glue table {name} in db {self.glue_temp_results_database}')
    table_name = self.create_glue_table_from_parquet(self.glue_temp_results_database, name, columns, f"{self.temp_results_bucket}/{name}")
    self.append_temp_table(name)
    return table_name

  def create_temp_table_name_and_location(self, name = ''):
    table_name = 'duck_engine_' + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + str(datetime.datetime.now().microsecond)
    if name:
      table_name = name.replace(".", "_") + '_' + table_name
    temp_s3_location = self.temp_results_bucket.strip('/') + '/' + table_name;
    return table_name, temp_s3_location

  def create_temporary_glue_table_from_redshift_classic_query(self, table_prefix, redshift_cluster_id, database, db_user, unload_role, query):
    name, s3location = self.create_temp_table_name_and_location(table_prefix)
    redshift_data = aws_helper.get_thread_safe_client('redshift-data')
    statement = redshift_data.execute_statement(
        ClusterIdentifier=redshift_cluster_id,
        Database=database,
        DbUser=db_user,
        Sql=f"unload ($$ {query} $$) to '{s3location}' iam_role '{unload_role}' format parquet   CLEANPATH",
    )
    return self._create_temporary_glue_from_redshift_statement(statement, name, s3location)

  def create_temporary_glue_table_from_redshift_serverless_query(self, table_prefix, workgroup_name, database, unload_role, query):
    name, s3location = self.create_temp_table_name_and_location(table_prefix)
    redshift_data = aws_helper.get_thread_safe_client('redshift-data')
    statement = redshift_data.execute_statement(
        WorkgroupName=workgroup_name,
        Database=database,
        Sql=f"unload ($$ {query} $$) to '{s3location}' iam_role '{unload_role}' format parquet   CLEANPATH",
    )
    return self._create_temporary_glue_from_redshift_statement(statement, name, s3location)

  def _create_temporary_glue_from_redshift_statement(self, statement, table_name, s3location):
    query_id = statement['Id']
    redshift_data = aws_helper.get_thread_safe_client('redshift-data')
    while True:
        result_status = redshift_data.describe_statement(Id=query_id)['Status']
        if result_status == 'FINISHED':
            break
        elif result_status == 'FAILED':
            raise Exception('Query failed')
        time.sleep(3)

    self.execute_query(f"create view {table_name} as select * from parquet_scan('{s3location}/*')")
    columns_info = self.info_schema(table_name)
    full_table_name = self.create_temporary_glue_table_from_parquet(table_name, columns_info)
    try:
        #delete temp csv file
        self.execute_query(f"drop view {table_name}")
    except:
        pass
    return full_table_name