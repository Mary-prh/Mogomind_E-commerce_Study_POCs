
from cleanCities import cleanCities
def insertCities(spark) :
    cleanCities()
    cities = spark.read.option('header','true').option('inferSchema', 'true')\
        .csv('Spark/e-commerce/brazilianSupermarket/datasets/olist_geolocation_dataset.csv')
    cities.select(cities.geolocation_city.alias("cityName")).distinct().write\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/E-commerce") \
        .option("dbtable", "city") \
        .option("user", "postgres") \
        .option("password", "123456") \
        .option("driver", "org.postgresql.Driver") \
        .mode('append').save()



