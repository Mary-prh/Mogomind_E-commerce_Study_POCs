from pyspark.sql import SparkSession

def city(spark: SparkSession):
    sales = spark.read.option('header', 'true').option('inferSchema', 'true').\
    csv('e-commerce/Mogomind_E-commerce_Study_POCs/superMarket/datasets/supermarket_sales - Sheet1.csv')

    customerGroup = spark.read\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/supermarket") \
        .option("dbtable", "customer_group_name") \
        .option("user", "postgres") \
        .option("password", "123456") \
        .option("driver", "org.postgresql.Driver").load()

    payment = spark.read\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/supermarket") \
        .option("dbtable", "payment") \
        .option("user", "postgres") \
        .option("password", "123456") \
        .option("driver", "org.postgresql.Driver").load()

    productLine = spark.read\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/supermarket") \
        .option("dbtable", "product_line") \
        .option("user", "postgres") \
        .option("password", "123456") \
        .option("driver", "org.postgresql.Driver").load()

    
    order = sales.select('Invoice ID','Branch','City','Customer type','Gender',\
        'Product line','Unit price','Quantity','Total','Date','Time','Payment')\
        .withColumnRenamed('Invoice ID', 'invoice_id')\
        .withColumnRenamed('Branch', 'branch')\
        .withColumnRenamed('City', 'city_name')\
        .withColumnRenamed('Customer type','customer_type_name')\
        .withColumnRenamed('Gender','gender_name')\
        .withColumnRenamed('Product line', 'product_name')\
        .withColumnRenamed('Unit price', 'unit_price')\
        .withColumnRenamed('Quantity','quantity')\
        .withColumnRenamed('Total', 'total')\
        .withColumnRenamed('Date','invoce_date')\
        .withColumnRenamed('Time','invoice_time')\
        .withColumnRenamed('Payment', 'payment_name')
        

        
        
        
    

