from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField,
    StringType,
    StructType,
    ArrayType,
    MapType,
)
from pyspark.sql.utils import ParseException

import re

def save_word_count_from_text_input(spark: SparkSession, text: str):
    sc = spark.sparkContext
    words = sc.parallelize(text.split(" "))
    sc.result = words.count()
    return sc


def retrieve_result(spark: SparkSession) -> int:
    sc = spark.sparkContext
    return sc.result


# private methods
def table_to_spark(spark, table):
    substitutions = [
        ("^ *", ""),  # Remove leading spaces
        (" *$", ""),  # Remove trailing spaces
        (r" *\| *", "|"),  # Remove spaces between columns
        (r"^\|", ""),  # Remove redundant leading delimiter
        (r"\|$", ""),  # Remove redundant trailing delimiter
        (r"\\", ""),  # Remove redundant trailing delimiter
    ]
    cleansed_table_str = table

    for pattern, replacement in substitutions:
        cleansed_table_str = re.sub(
            pattern, replacement, cleansed_table_str, flags=re.MULTILINE
        )

    table_lines = cleansed_table_str.split("\n")

    schema_row, *data_rows = table_lines

    schema = schema_row.replace("|", ",").replace("[", "<").replace("]", ">")

    try:
        schema_struct = spark.createDataFrame(
            spark.sparkContext.emptyRDD(), schema=schema
        ).schema

    except ParseException as e:
        raise ValueError(
            f"Unable to parse the schema provided in the scenario table: {schema}"
        ) from e

    stringified_schema = [
        StructField(field.name, StringType()) for field in schema_struct
    ]

    parsed_df = (
        spark.read.option("sep", "|")
        .option("nullValue", "null")
        .csv(
            path=spark.sparkContext.parallelize(data_rows),
            schema=StructType(stringified_schema),
        )
        .select(*[cast_str_column_to_actual_type(field) for field in schema_struct])
    )

    return parsed_df

def cast_str_column_to_actual_type(field):
    complex_types = StructType, ArrayType, MapType
    is_complex_type = isinstance(field.dataType, complex_types)

    if is_complex_type:
        return F.from_json(F.col(field.name), field.dataType).alias(field.name)
    else:
        return F.col(field.name).cast(field.dataType)


def create_source_table(spark: SparkSession, table_name: str, table: str) -> DataFrame:
    df = table_to_spark(spark, table)
    df = df.createOrReplaceTempView(table_name)

    return df


def construct_dest_table(spark:SparkSession, source_table: str, field: str, threshold: str, dest_table:str) -> DataFrame:
    df = spark.sql("select * from {0} where {1} >= {2}".format(source_table, field, threshold))
    df.createOrReplaceTempView(dest_table)

    return df

def show_destination_table_output(spark: SparkSession, table_name: str) -> DataFrame:
    return spark.sql("select * from {0}".format(table_name))

