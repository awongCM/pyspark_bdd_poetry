import pytest
from pytest_bdd import scenarios, given, when, then, parsers
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.testing.utils import assertDataFrameEqual

from src import basic_xml_dataframe

scenarios('../features/basic_xml_dataframe.feature')

@pytest.fixture(scope='session')
def xml_source() -> str:
    # relative to calling code from root src
    return "./tests/fixtures/xml_data.xml"

@given("a spark session")
def given_spark_session(spark: SparkSession) -> SparkSession:
    yield spark


@when(parsers.parse("I read some xml data coming into Spark that has a row tag '{rowTag}'"), target_fixture="dataframe")
def complete_xml_data_analysis(spark: SparkSession, rowTag: str, xml_source: str) -> DataFrame:
    # just convert all column fields as string for now
    inferSchema = False

    return basic_xml_dataframe.read_xml_to_dataframe(spark, rowTag, inferSchema, xml_source)


@then(parsers.parse("I expect to see same data appearing in dataframe format it came with"))
def complete_xml_data_assertion(spark:SparkSession, dataframe: DataFrame) -> None:

    data = [("1", "John"), ("2","Jane"), ("3", "Jim")]
    schema = StructType([
        StructField("id", StringType(), True),
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
    actualRowCount = basic_xml_dataframe.get_total_row_count(actual_df)
    assert actualRowCount == expectedRowCount


@then(parsers.parse("It gets transformed with its name column as lower-case characters"))
def complete_json_data_transformation_assertion(spark: SparkSession, dataframe: DataFrame) -> None:
    data = [(1, "john"), (2, "jane"), (3, "jim")]
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True)
    ])

    expected_df = spark.createDataFrame(data, schema=schema)

    actual_df = basic_xml_dataframe.convert_to_lower_case(dataframe)

    assertDataFrameEqual(actual_df, expected_df)