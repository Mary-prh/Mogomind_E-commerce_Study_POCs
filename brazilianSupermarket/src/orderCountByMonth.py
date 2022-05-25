from pyspark.sql import SparkSession
from pyspark.sql import functions as func


def insertOrders(spark: SparkSession):
    orders = spark.read.option('header', 'true').option('inferSchema', 'true') \
    .csv("e-commerce/Mogomind_E-commerce_Study_POCs/brazilianSupermarket/datasets/olist_orders_dataset.csv")
    
    orders = orders.withColumn('order_time', func.substring('order_purchase_timestamp' , pos= 0 , len=7))

    orders = orders.select(\
        orders.order_time.alias('order_time')).groupBy('order_time').count() \
        .orderBy('count',ascending=False).withColumnRenamed('count', 'order_count') \
    

    orders.write\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/E-commerce") \
        .option("dbtable", "order_per_month") \
        .option("user", "postgres") \
        .option("password", "123456") \
        .option("driver", "org.postgresql.Driver") \
        .mode('append').save()





