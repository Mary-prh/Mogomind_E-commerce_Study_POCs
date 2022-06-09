from loadData import loadData
from pyspark.sql import SparkSession
from pgOptions import pgOptions

def insertConclusion(spark: SparkSession):
     loadData(spark).\
        select('Conclusion')\
        .withColumnRenamed('Conclusion', 'conclusion_category')\
        .distinct()\
        .write\
            .format("jdbc") \
            .options(**pgOptions)\
            .option("dbtable", "conclusion") \
            .mode('append').save()

