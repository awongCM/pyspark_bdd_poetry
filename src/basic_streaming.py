
from operator import add
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import rdd


def do_streaming_word_counts(lines):
    """ count of words in a dstream of lines """
    counts_stream = (lines.flatMap(lambda x: x.split())
                     .map(lambda x: (x, 1))
                     .reduceByKey(add)
                     )
    return counts_stream


def make_dstream_helper(sc: SparkContext, ssc: StreamingContext, test_input) -> StreamingContext:
    """ make dstream from input
    Args:
        test_input: list of lists of input rdd data

    Returns: a dstream
    :param test_input:
    :param ssc:

    """
    input_rdds = [sc.parallelize(d, 1) for d in test_input]
    input_stream = ssc.queueStream(input_rdds)
    return input_stream


def collect_helper(ssc: StreamingContext, dstream: rdd, expected_length: int, block=True):
    """
    Collect each RDDs into the returned list.

    This function is borrowed and modified from here:
    https://github.com/holdenk/spark-testing-base

    :return: list with the collected items.
    """
    result = []

    def get_output(_, rdd):
        if rdd and len(result) < expected_length:
            r = rdd.collect()
            if r:
                result.append(r)

    dstream.foreachRDD(get_output)

    if not block:
        return result

    ssc.start()

    timeout = 15
    start_time = time.time()
    while len(result) < expected_length and time.time() - start_time < timeout:
        time.sleep(0.01)
    if len(result) < expected_length:
        print("timeout after", timeout)

    return result
