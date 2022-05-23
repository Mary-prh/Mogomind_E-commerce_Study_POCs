from spark import initSpark
from cities import insertCities
from orderByCityByMonth import insertOrderByCityByMonth

spark = initSpark()
# insertCities(spark)

insertOrderByCityByMonth(spark)


