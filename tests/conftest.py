import pytest
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession

# Thanks to this - https://stackoverflow.com/a/33908466/1065118
# NB: Helps to avoid the hassle of downloading the required jar while for local pyspark environment
import os
packages = "com.databricks:spark-xml_2.12:0.12.0"
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages {0} pyspark-shell".format(packages)
)

@pytest.fixture(scope='module')
def spark() -> SparkSession:
    return SparkSession.builder.master("local[1]") \
        .appName("PYspark BDD Poetry") \
        .config('spark.executor.memory', '2G') \
        .config('spark.driver.memory', '2G') \
        .config('spark.driver.maxResultSize', '10G') \
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.12.0") \
        .getOrCreate()


@pytest.fixture(scope="module")
def spark_context(request) -> SparkContext:
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object

    """
    conf = (SparkConf().setMaster("local[2]").setAppName("BasicStreaming"))
    sc = SparkContext.getOrCreate(conf=conf)
    request.addfinalizer(lambda: sc.stop())

    return sc


@pytest.fixture(scope="module")
def streaming_context(spark_context:SparkContext) -> StreamingContext:
    return StreamingContext(spark_context, 1)
