import pytest
from pytest_bdd import scenarios, given, when, then, parsers
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

from src import basic_streaming

scenarios('../features/basic_streaming.feature')

@given("a spark streaming context")
def given_spark_streaming_session(spark_context: SparkContext, streaming_context: StreamingContext) -> None:
    pass


@when('I provide some a streaming list of multi-dimensional words below',
      target_fixture="input_stream")
def input_stream(spark_context: SparkContext, streaming_context: StreamingContext) -> StreamingContext:
    test_input = [
        [
            ' hello spark ',
            ' hello again spark spark'
        ],
        [
            ' hello there again spark spark'
        ],
    ]

    return basic_streaming.make_dstream_helper(spark_context, streaming_context, test_input)


@then("I expect the streaming words to display array of unique word count")
def assert_streaming_word_count(input_stream: list, streaming_context: StreamingContext) -> None:
    tally = basic_streaming.do_streaming_word_counts(input_stream)
    results = basic_streaming.collect_helper(streaming_context, tally, 2, True)

    expected_results = [
        [('hello', 2), ('again', 1), ('spark', 3)],
        [('hello', 1), ('again', 1), ('there', 1), ('spark', 2)]
    ]

    assert results == expected_results

