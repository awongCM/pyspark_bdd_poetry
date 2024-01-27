import pytest
from pytest_bdd import scenarios, given, when, then, parsers
from pyspark.sql import SparkSession
from pyspark import SparkContext

from src import basic_word_count

scenarios('../features/basic_word_count.feature')


@given("a spark session")
def given_spark_session(spark: SparkSession) -> SparkSession:
    yield spark


@when(parsers.parse('I count the words in "{text}"'))
def complete_word_count_analysis(spark: SparkSession, text: str) -> SparkContext:
    return basic_word_count.save_word_count_from_text_input(spark, text)


@then(parsers.parse("the number of word is '{x}'"))
def complete_word_count_assertion(spark:SparkSession, x:str) -> None:
    result = basic_word_count.retrieve_result(spark)
    assert (result == int(x))
