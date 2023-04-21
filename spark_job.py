from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import current_timestamp, udf
from pyspark.sql.types import StructType, StructField, StringType
import uuid

def build_spark_session(app_name, config_key, config_value):
    """
    Instantiates Spark session with app_name and config provided
    Input args: app name, config key, config value
    Output:     Spark session
    """
    spark = SparkSession \
        .builder \
        .appName(app_name) \
        .config(config_key, config_value) \
        .getOrCreate()
    return spark

def read_csv(spark_session, schema, filepath):
    """
    Reads the CSV from the supplied path into a dataframe using the provided schema.
    Input args: Spark session, schema, full path to CSV
    Output:     dataframe
    """
    df = spark_session.read.format("csv") \
        .schema(schema) \
        .load(filepath)
    return df

def filter(df):
    """
    Takes an input dataframe and filters out header rows.
    Here we assume that the column names will always be id, first_name and last_name in all input files.
    Input args: input_dataframe
    Output:     filtered_dataframe
    """
    filter_df = df \
        .filter(
            (df["id"] != "id") &
            (df["first_name"] != "first_name") & 
            (df["last_name"] != "last_name")
        )
    return filter_df

def add_ts_col(df):
    """
    Takes an input dataframe and adds a current timestamp column to it
    Input args: input_dataframe
    Output:     transformed_dataframe
    """
    out_df = df.withColumn("ingestion_tms", current_timestamp())
    return out_df

def add_uuid_col(df):
    """
    Takes an input dataframe and adds a UUID v4 column to it
    Input args: input_dataframe
    Output:     transformed_dataframe
    """
    uuid_udf = udf(lambda : str(uuid.uuid4()), StringType())
    out_df = df.withColumn("batch_id", uuid_udf())
    return out_df

def write_df_to_delta(df):
    """
    Writes input dataframe to delta table
    Input args: dataframe
    Output:     None
    """
    df.write.format("delta").mode("append").save("test-delta-table")

if __name__ == "__main__":
    app_name = "csv to delta"
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
    write_df_to_delta(uuid_df)
