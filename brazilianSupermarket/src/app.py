from spark import initSpark
# from cities import insertCities
# from orderByCityByMonth import insertOrderByCityByMonth
# from orderCountByMonth import insertOrders
from salesPerCategory import salesPerCategory
from englishCategories import englishCategories
# from cleanData import cleanData

spark = initSpark()
# insertCities(spark)

# insertOrderByCityByMonth(spark)
# insertOrders(spark)

# englishCategories(spark)

# cleanData()
salesPerCategory(spark)

