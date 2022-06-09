
from loadData import loadData
from pyspark.sql import SparkSession
from pgOptions import pgOptions

def insertCostomerDocs(spark: SparkSession):
    costomerInfo = loadData(spark)\
        .select('Company','FileNo','Opened','Closed','Coverage',\
            'SubCoverage','Reason','SubReason','Conclusion')\
                .withColumnRenamed('Company','company_name')\
                .withColumnRenamed('Conclusion','conclusion_category')\
                .withColumnRenamed('Coverage','coverage_root')\
                .withColumnRenamed('SubCoverage','coverage_leaf')\
                .withColumnRenamed('Reason','reason_root')\
                .withColumnRenamed('SubReason','reason_leaf')

    companyInfo = spark.read\
        .format("jdbc") \
        .options(**pgOptions)\
        .option("dbtable", "company") \
        .load()
    
    conclusionInfo = spark.read\
        .format("jdbc") \
        .options(**pgOptions)\
        .option("dbtable", "conclusion") \
        .load()
    
    coverageInfo = spark.read\
        .format("jdbc") \
        .options(**pgOptions)\
        .option("dbtable", "coverage_group") \
        .load()
    
    reasonInfo = spark.read\
        .format("jdbc") \
        .options(**pgOptions)\
        .option("dbtable", "reason_group") \
        .load()
             

    costomerInfo = costomerInfo.join(companyInfo, on='company_name')\
        .join(conclusionInfo, on='conclusion_category')\
        .join(coverageInfo, on= 'coverage_root' and 'coverage_leaf')\
        .join(reasonInfo, on='reason_root' and 'reason_leaf')
    
    costomerInfo = costomerInfo.select('FileNo','company_id','Opened',\
        'Closed', 'coverage_category_id', 'reason_category_id', 'conclusion_id')\
        .write\
            .format("jdbc") \
            .options(**pgOptions)\
            .option("dbtable", "customer_claim") \
            .mode('append').save()

    

    

            