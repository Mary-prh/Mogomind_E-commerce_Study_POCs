from pyspark.sql import SparkSession

def customerType(spark: SparkSession):
    sales = spark.read.option('header', 'true').option('inferSchema', 'true').\
    csv('e-commerce/Mogomind_E-commerce_Study_POCs/superMarket/datasets/supermarket_sales - Sheet1.csv')

    gender = sales.select('Customer type').distinct()\
        .withColumnRenamed('Customer type','customer_type_name')

    gender.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/supermarket") \
        .option("dbtable", "customer_type") \
        .option("user", "postgres") \
        .option("password", "123456") \
        .option("driver", "org.postgresql.Driver") \
        .mode('append').save()

