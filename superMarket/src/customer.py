from pyspark.sql import SparkSession

def loadCustomerGroups(spark: SparkSession):
    sales = spark.read.option('header', 'true').option('inferSchema', 'true').\
    csv('e-commerce/Mogomind_E-commerce_Study_POCs/superMarket/datasets/supermarket_sales - Sheet1.csv')

    customerType = spark.read\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/supermarket") \
        .option("dbtable", "customer_type") \
        .option("user", "postgres") \
        .option("password", "123456") \
        .option("driver", "org.postgresql.Driver").load()

    city = spark.read\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/supermarket") \
        .option("dbtable", "city") \
        .option("user", "postgres") \
        .option("password", "123456") \
        .option("driver", "org.postgresql.Driver").load()

    gender = spark.read\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/supermarket") \
        .option("dbtable", "gender") \
        .option("user", "postgres") \
        .option("password", "123456") \
        .option("driver", "org.postgresql.Driver").load()


    sales.select('Customer type', 'City', 'Gender').distinct()\
        .withColumnRenamed('Customer type','customer_type_name')\
        .withColumnRenamed('City', 'city_name')\
        .withColumnRenamed('Gender', 'gender_name')\
        .join(customerType, on= 'customer_type_name')\
        .join(city, on= 'city_name')\
        .join(gender, on='gender_name')\
        .select('city_id','customer_type_id','gender_id')\
        .write\
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/supermarket") \
            .option("dbtable", "customer_group") \
            .option("user", "postgres") \
            .option("password", "123456") \
            .option("driver", "org.postgresql.Driver") \
            .mode('append').save()
        
        
