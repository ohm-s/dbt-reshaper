from . import aws_helper

duckdb_to_glue_mapping = {
    'BOOLEAN': 'boolean',
    'TINYINT': 'tinyint',
    'SMALLINT': 'smallint',
    'INTEGER': 'int',
    'BIGINT': 'bigint',
    'FLOAT': 'float',
    'DOUBLE': 'double',
    'VARCHAR': 'string',
    'TIMESTAMP': 'timestamp',
    'DATE': 'date',
    'TIME': 'string',
    'BLOB': 'binary',
    'INTERVAL': 'string'
}

def convert_duckdb_to_glue_type(duckdb_type):
    if 'DECIMAL' in duckdb_type:
        return duckdb_type.lower()
    else:
        return duckdb_to_glue_mapping[duckdb_type]


def create_glue_table(database, temp_table, columns_info, directory):
    glue = aws_helper.get_thread_safe_client('glue')
    def create_glue_table():
        return glue.create_table(
            DatabaseName=database,
            TableInput={
                'Name': temp_table,
                'StorageDescriptor': {
                    'Columns': [
                        {
                            'Name': col[0],
                            'Type': convert_duckdb_to_glue_type(col[1]),
                            'Comment': col[0]
                        } for col in columns_info
                    ],
                    'Location': directory,
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'Compressed': False,
                    'NumberOfBuckets': -1,
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                        'Parameters': {
                            'serialization.format': '1'
                        }
                    },
                    'BucketColumns': [],
                    'SortColumns': [],
                    'Parameters': {
                        'classification': 'parquet',
                        'compressionType': 'none',
                    },
                    'StoredAsSubDirectories': False
                },
                'PartitionKeys': [],
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': {
                    'classification': 'parquet',
                    'compressionType': 'none',
                    'typeOfData': 'file'
                }
            }
        )

    return aws_helper.retry_api_call(create_glue_table)
