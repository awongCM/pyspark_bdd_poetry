import pytest
from pytest_bdd import scenarios, given, when, then, parsers
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.testing.utils import assertDataFrameEqual

from src import basic_json_dataframe

scenarios('../features/basic_json_dataframe.feature')

@pytest.fixture(scope='session')
def json_source() -> str:
    # relative to calling code from root src
    return "./tests/fixtures/json_data.json"

@given("a spark session")
def given_spark_session(spark: SparkSession) -> SparkSession:
    yield spark


@when(parsers.parse("I read some JSON data coming into Spark that has multi-lines"), target_fixture="dataframe")
def complete_json_data_analysis(spark: SparkSession, json_source: str) -> DataFrame:

    return basic_json_dataframe.read_json_to_dataframe(spark, json_source)


@then(parsers.parse("I expect to see same data appearing in dataframe format it came with"))
def complete_json_data_assertion(spark:SparkSession, dataframe: DataFrame) -> None:

    data = [(1, "John"), (2,"Jane"), (3, "Jim")]
    schema = StructType([
        StructField("id", LongType(), True), # FYI -> https://stackoverflow.com/a/45147723/1065118
        StructField("name", StringType(), True)
    ])

    expected_df = spark.createDataFrame(data, schema=schema)
    actual_df = dataframe

    print("\n\n\nEXPECTED:")
    expected_df.show()
    print("ACTUAL:")
    actual_df.show()

    assertDataFrameEqual(actual_df, expected_df)

    expectedRowCount = expected_df.count()
    actualRowCount = basic_json_dataframe.get_total_row_count(actual_df)
    assert actualRowCount == expectedRowCount