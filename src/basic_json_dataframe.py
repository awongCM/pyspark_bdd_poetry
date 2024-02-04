from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


def read_json_to_dataframe(spark: SparkSession, jsonlFile: str) -> DataFrame:
    return spark.read \
        .option("multiline", "true") \
        .json(jsonlFile)


def get_total_row_count(dataFrame: DataFrame) -> int:
    return dataFrame.count()


def convert_to_lower_case(dataFrame: DataFrame) -> DataFrame:
    df = dataFrame.withColumn("name", lower(dataFrame["name"]))
    return df
