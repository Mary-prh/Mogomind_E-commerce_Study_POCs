from spark import sparkInit
from loadData import loadData
from insertCompany import inserCompany
from insertReason import insertReason


spark = sparkInit()

insuranceComplaintsData = loadData(spark)

# inserCompany(insuranceComplaintsData)
insertReason(spark)




