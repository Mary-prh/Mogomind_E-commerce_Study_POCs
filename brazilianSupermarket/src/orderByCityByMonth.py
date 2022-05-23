from pyspark.sql import SparkSession
from pyspark.sql import functions as func

def insertOrderByCityByMonth(spark : SparkSession):
    cities =spark.read\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/E-commerce") \
        .option("dbtable", "city") \
        .option("user", "postgres") \
        .option("password", "123456") \
        .option("driver", "org.postgresql.Driver").load()
    
    orders = spark.read.option('header', 'true').option('inferSchema','true'). \
        csv('Spark/e-commerce/brazilianSupermarket/datasets/olist_orders_dataset.csv')
    customerCity = spark.read.option('header', 'true').option('inferSchema','true'). \
        csv('Spark/e-commerce/brazilianSupermarket/datasets/olist_customers_dataset.csv')
    orders = orders.withColumn("ordertime", func.substring('order_purchase_timestamp', pos= 0 , len=7))
    
    orderCount = orders.join(customerCity, on= 'customer_id').select(\
        customerCity.customer_city.alias('cityname'), 'ordertime').groupBy('cityname', 'ordertime').count().withColumnRenamed('count', 'ordercount')
    
    orderByCity = orderCount.join(cities,on= 'cityname').orderBy('ordertime', 'cityname').withColumnRenamed('citiid','cityid')
    orderByCity = orderByCity.select("cityid", 'ordercount', 'ordertime')

    orderByCity.write\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/E-commerce") \
        .option("dbtable", "ordercity") \
        .option("user", "postgres") \
        .option("password", "123456") \
        .option("driver", "org.postgresql.Driver") \
        .mode('append').save()

