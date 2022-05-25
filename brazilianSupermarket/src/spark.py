

from pyspark.sql import SparkSession
from pyspark.sql import functions as func


def initSpark():
    return  SparkSession.builder.appName('spark')\
     .config("spark.jars", "e-commerce/postgresql-42.3.5.jar") \
    .getOrCreate()