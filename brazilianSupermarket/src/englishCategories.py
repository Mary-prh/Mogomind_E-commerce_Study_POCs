from pyspark.sql import SparkSession

def englishCategories(spark: SparkSession):
    
    englishCategory = spark.read.option('header', 'true').option('inferSchema', 'true')\
    .csv('e-commerce/Mogomind_E-commerce_Study_POCs/brazilianSupermarket/datasets/product_category_name_translation.csv')

    englishCategory = englishCategory.select(\
    englishCategory.product_category_name.alias('category_name'),\
    englishCategory.product_category_name_english.alias('category_name_english'))

    englishCategory.write\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/E-commerce") \
        .option("dbtable", "english_categories") \
        .option("user", "postgres") \
        .option("password", "123456") \
        .option("driver", "org.postgresql.Driver") \
        .mode('append').save()

