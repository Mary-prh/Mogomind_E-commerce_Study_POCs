from pyspark.sql import DataFrame
from pgOptions import pgOptions

def inserCompany(insuranceComplaintsData: DataFrame):
    insuranceComplaintsData.select('Company')\
    .withColumnRenamed('Company','company_name')\
    .distinct()\
        .write\
        .format("jdbc") \
        .options(**pgOptions)\
        .option("dbtable", "company") \
        .mode('append').save()
