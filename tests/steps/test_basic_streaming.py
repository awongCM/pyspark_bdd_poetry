import pytest
from pytest_bdd import scenarios, given, when, then, parsers
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

from src import basic_streaming

scenarios('../features/basic_streaming.feature')

@pytest.fixture(scope="session")
def spark_context(request):
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object

    """
    conf = (SparkConf().setMaster("local[2]").setAppName("BasicStreaming"))
    sc = SparkContext(conf=conf)
    request.addfinalizer(lambda: sc.stop())

    return sc


@pytest.fixture(scope="session")
def streaming_context(spark_context):
    return StreamingContext(spark_context, 1)


@given("a spark streaming context")
def given_spark_streaming_session(spark_context: SparkContext, streaming_context: StreamingContext):
    pass


@when('I provide some a streaming list of multi-dimensional words below',
      target_fixture="input_stream")
def input_stream(spark_context, streaming_context):
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
def assert_streaming_word_count(input_stream, streaming_context):
    tally = basic_streaming.do_streaming_word_counts(input_stream)
    results = basic_streaming.collect_helper(streaming_context, tally, 2, True)

    expected_results = [
        [('hello', 2), ('again', 1), ('spark', 3)],
        [('hello', 1), ('again', 1), ('there', 1), ('spark', 2)]
    ]

    assert results == expected_results

