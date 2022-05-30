from pyspark.sql import SparkSession

def city(spark: SparkSession):
    sales = spark.read.option('header', 'true').option('inferSchema', 'true').\
    csv('e-commerce/Mogomind_E-commerce_Study_POCs/superMarket/datasets/supermarket_sales - Sheet1.csv')


    branch = spark.read\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/supermarket") \
        .option("dbtable", "branch") \
        .option("user", "postgres") \
        .option("password", "123456") \
        .option("driver", "org.postgresql.Driver").load()

    city = sales.select('City','Branch').distinct()\
        .withColumnRenamed('Branch','branch')\
        .withColumnRenamed('City','city_name')\
        .join(branch,on="branch")\
        .select('city_name','branch_id')\
            


    city.write\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/supermarket") \
        .option("dbtable", "city") \
        .option("user", "postgres") \
        .option("password", "123456") \
        .option("driver", "org.postgresql.Driver") \
        .mode('append').save()
