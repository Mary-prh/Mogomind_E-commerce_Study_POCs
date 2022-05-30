from pyspark.sql import DataFrame
from pgOptions import pgOptions
from pyspark.sql import SparkSession
from loadData import loadData

def insertReason(spark: SparkSession):

    ## Insert Parent Reasone
    loadData(spark)\
        .select('Reason')\
        .withColumnRenamed('Reason','reason_title')\
        .distinct()\
        .select('reason_title')\
            .write\
            .format("jdbc") \
            .options(**pgOptions)\
            .option("dbtable", "reason") \
            .mode('append').save()
    
    ## Load Parent Reason to have access to reason_id for sub Reasons
    reasons = spark\
        .read\
        .format("jdbc") \
        .options(**pgOptions)\
        .option("dbtable", "reason") \
        .load()
    
    reasonDf =  loadData(spark)\
        .select('Reason','SubReason')\
        .withColumnRenamed('Reason','reason_title')\
        .distinct()\
    
    ## Joining Subreasons with DB Reason on Reason column to find the Parent Id fro SubReasons
    reasons=  reasons\
            .join(reasonDf, on='reason_title' )\
            .select('reason_id','SubReason')\
            .withColumnRenamed('reason_id','parent_id')\
            .withColumnRenamed('SubReason','reason_title')\
        
    reasons.write\
            .format("jdbc") \
            .options(**pgOptions)\
            .option("dbtable", "reason") \
            .mode('append').save()
    
        
    
    

   