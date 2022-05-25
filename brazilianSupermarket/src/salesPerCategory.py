from pyspark.sql import SparkSession
from pyspark.sql import functions as func

def salesPerCategory(spark: SparkSession):

    translation = spark.read\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/E-commerce") \
        .option("dbtable", "english_categories") \
        .option("user", "postgres") \
        .option("password", "123456") \
        .option("driver", "org.postgresql.Driver").load()

    sales = spark.read.option('header', 'true').option('inferSchema', 'true') \
    .csv('e-commerce/Mogomind_E-commerce_Study_POCs/brazilianSupermarket/datasets/olist_products_dataset.csv')

    sales = sales.select(\
    sales.product_category_name.alias('category_name')).groupBy('category_name').count()\
        .orderBy('count', ascending=False).withColumnRenamed('count','sales_quantity')
    
    salesPerCategory = sales.join(translation , on= 'category_name').select(\
    translation.category_id ,translation.category_name_english, sales.sales_quantity)\
    .orderBy('sales_quantity', ascending=False)

    salesPerCategory.write\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/E-commerce") \
        .option("dbtable", "sales_category") \
        .option("user", "postgres") \
        .option("password", "123456") \
        .option("driver", "org.postgresql.Driver") \
        .mode('append').save()





