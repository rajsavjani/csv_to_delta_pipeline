# tests to be run by executing pytest from cc2 directory

from spark_job import build_spark_session, read_csv, filter, add_ts_col, add_uuid_col
from pyspark.sql.types import StructType, StructField, StringType

app_name = "csv to delta tests"
config_key = "spark.sql.extensions"
config_value = "io.delta.sql.DeltaSparkSessionExtension"
data_schema = StructType([
    StructField("id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True)
])
path = "/app/*.csv"
spark = build_spark_session(app_name, config_key, config_value)
df = read_csv(spark, data_schema, path)
filtered_df = filter(df)
ts_df = add_ts_col(filtered_df)
uuid_df = add_uuid_col(ts_df)
    
def test_build_spark_session():
    assert str(spark)[0:44] == "<pyspark.sql.session.SparkSession object at "

def test_read_csv():
    assert df.count() == 6

def test_filter():
    assert filtered_df.count() == 5

def test_add_ts_col():
    assert len(ts_df.columns) == 4

def test_add_uuid_col():
    assert len(uuid_df.columns) == 5
