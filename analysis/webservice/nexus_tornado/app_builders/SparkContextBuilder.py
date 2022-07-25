
class SparkContextBuilder:
    def __init__(self):
        pass

    spark_context = None

    @classmethod
    def get_spark_context(cls):
        if cls.spark_context is None:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.appName("nexus-analysis").getOrCreate()
            cls.spark_context = spark.sparkContext

        return cls.spark_context

