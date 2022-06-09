from pyspark.sql import SparkSession

def loadData(spark: SparkSession):
   return spark.read.option('header', 'true').option('inferSchema','true')\
    .csv('e-commerce/Mogomind_E-commerce_Study_POCs/insurance_Complaints/datasets/Insurance_Company_Complaints.csv')

