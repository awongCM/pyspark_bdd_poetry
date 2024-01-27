from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


# xmlFile = "path/to/xml/file.xml"

def read_xml_to_dataframe(spark: SparkSession, rowTag: str, inferSchema: bool, xmlFile: str) -> DataFrame:
    return spark.read \
        .format('com.databricks.spark.xml') \
        .options(rowTag=rowTag, inferSchema=inferSchema) \
        .load(xmlFile)


def get_total_row_count(dataFrame: DataFrame):
    return dataFrame.count()