from pyspark.sql import SparkSession

def branch(spark: SparkSession):
    sales = spark.read.option('header', 'true').option('inferSchema', 'true').\
    csv('e-commerce/Mogomind_E-commerce_Study_POCs/superMarket/datasets/supermarket_sales - Sheet1.csv')

    branch = sales.select('Branch').distinct()

    branch.write\
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/supermarket") \
            .option("dbtable", "branch") \
            .option("user", "postgres") \
            .option("password", "123456") \
            .option("driver", "org.postgresql.Driver") \
            .mode('append').save()

