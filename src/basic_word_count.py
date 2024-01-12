from pyspark.sql import SparkSession


def save_word_count_from_text_input(spark: SparkSession, text: str):
    sc = spark.sparkContext
    words = sc.parallelize(text.split(" "))
    sc.result = words.count()
    return sc


def retrieve_result(spark: SparkSession) -> int:
    sc = spark.sparkContext
    return sc.result
