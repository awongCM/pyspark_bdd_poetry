import pytest
from pytest_bdd import scenarios, given, when, then, parsers
from pyspark.sql import SparkSession

from src import basic_word_count

scenarios('../features/basic_word_count.feature')


@pytest.fixture(scope='session')
def spark() -> SparkSession:
    return SparkSession.builder.master("local") \
        .appName("BasicWordCount") \
        .config('spark.executor.memory', '2G') \
        .config('spark.driver.memory', '2G') \
        .config('spark.driver.maxResultSize', '10G') \
        .getOrCreate()


@given("a spark session")
def given_spark_session(spark: SparkSession):
    yield spark


@when(parsers.parse('I count the words in "{text}"'))
def complete_word_count_analysis(spark: SparkSession, text: str):
    return basic_word_count.save_word_count_from_text_input(spark, text)


@then(parsers.parse("the number of word is '{x}'"))
def complete_word_count_assertion(spark:SparkSession, x:str):
    result = basic_word_count.retrieve_result(spark)
    assert (result == int(x))
