# import needed libraries
import datetime
import json
import sys

from pyspark.context import SparkContext
from pyspark.sql import DataFrame

from awsglue.context import GlueContext
from awsglue import DynamicFrame
from awsglue.utils import getResolvedOptions

# create GlueContext
glueContext = GlueContext(SparkContext.getOrCreate())

tables = ["new_table","user"]
print("Tables:", tables)

def mapping(_input: dict) -> dict:
    print(_input)
    return _input;

for _table_name in tables:
    connection_mysql8_options = {
    "url": "",
    "dbtable": _table_name,
    "user": "",
    "password": "",
    "customJdbcDriverS3Path": "s3://stefan-glue/gluelibs/mysql-connector-java-8.0.23.jar",
    "customJdbcDriverClassName": "com.mysql.cj.jdbc.Driver"}
    table: DynamicFrame = glueContext.create_dynamic_frame.from_options(connection_type="mysql",
                                                          connection_options=connection_mysql8_options)

    table.printSchema()

        
    _table: DynamicFrame = (
        table.map(lambda row: mapping(row))
    )
        
    _table.toDF().printSchema()

    # Make num_partitions based on size of table
    #_table_df: DataFrame = _table.toDF().repartition(num_partitions)
    # output_path = f"s3://{bucket_name}/core-account/subledger-control-procedure/{_table_name}/{tsnow.strftime('%Y-%m-%d')}/"
    output_path = f"s3://stefan-glue/output/v2/{_table_name}/"
    print(f"Writing {table.count()} rows to {output_path}")
    _table.toDF().write.parquet(output_path)