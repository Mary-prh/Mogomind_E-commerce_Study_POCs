from pyspark.sql import SparkSession

def sparkInit():
    return SparkSession.builder.appName('supermarket')\
    .config("spark.jars", "e-commerce\Mogomind_E-commerce_Study_POCs\postgresql-42.3.5.jar") \
    .getOrCreate()