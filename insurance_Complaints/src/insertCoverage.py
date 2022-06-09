from pyspark.sql import SparkSession
from loadData import loadData
from pgOptions import pgOptions

def insertCoverage(spark: SparkSession):
     
     ## Insert Parent Coverage
    #  loadData(spark).select('Coverage')\
    #      .withColumnRenamed('Coverage', 'coverage_category')\
    #      .distinct()\
    #      .write\
    #         .format("jdbc") \
    #         .options(**pgOptions)\
    #         .option("dbtable", "coverage") \
    #         .mode('append').save()
    
    ## Load Parent Coverage from DB to have access to coverage_id for sub-Coverages
     coverageFromDb = spark.read\
        .format("jdbc") \
        .options(**pgOptions)\
        .option("dbtable", "coverage") \
        .load()

     coverageFromDateset = loadData(spark).\
         select('Coverage', 'SubCoverage').\
         withColumnRenamed('Coverage', 'coverage_category').distinct()
     
    ## Joining  sub-Coverages with DB Coverage table on coverage_category column 
    ## to find the Parent Id for sub-Coverages
     coverageFromDb = coverageFromDb\
         .join(coverageFromDateset, on='coverage_category')\
         .select('coverage_id','SubCoverage')\
         .withColumnRenamed('coverage_id', 'coverage_parent_id')\
         .withColumnRenamed('SubCoverage','coverage_category')
    
     coverageFromDb.write\
         .format("jdbc") \
         .options(**pgOptions)\
         .option("dbtable", "coverage") \
         .mode('append').save()


