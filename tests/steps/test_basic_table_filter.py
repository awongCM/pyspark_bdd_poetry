import pytest
from pytest_bdd import scenarios, given, when, then, parsers
from pyspark.sql import SparkSession

from src import basic_table_filter

scenarios('../features/basic_table_filter.feature')

@pytest.fixture(scope='session')
def spark() -> SparkSession:
    return SparkSession.builder.master("local") \
            .appName("BasicTableFilter") \
            .config('spark.executor.memory', '2G') \
            .config('spark.driver.memory', '2G') \
            .config('spark.driver.maxResultSize', '10G') \
            .getOrCreate()

@given("a spark session")
def given_spark_session(spark: SparkSession):
    yield spark


@given(parsers.parse('a table called "{table_name}" containing\n{table}'))
def given_table(spark: SparkSession, table_name: str, table: str):
    return basic_table_filter.create_source_table(spark, table_name, table)

@when(parsers.parse('I select rows from "{source_table}" where "{field}" greater than "{threshold}" into table "{dest_table}"'))
def select_query_from_source_table(spark:SparkSession, source_table:str, field:str, threshold:str, dest_table:str):
    return basic_table_filter.construct_dest_table(spark, source_table, field, threshold, dest_table)

@then(parsers.parse('the table "{table_name}" contains\n{table}'))
def display_destination_table_output(spark:SparkSession, table_name:str, table:str):

    expected_df = basic_table_filter.table_to_spark(spark, table)

    actual_df = basic_table_filter.show_destination_table_output(spark, table_name)

    print("\n\n\nEXPECTED:")
    expected_df.show()
    print("ACTUAL:")
    actual_df.show()

    assert (expected_df.schema == actual_df.schema)
    assert (expected_df.subtract(actual_df).count() == 0)
    assert (actual_df.subtract(expected_df).count() == 0)


