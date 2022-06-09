from spark import sparkInit
from loadData import loadData
from insertCompany import inserCompany
from insertReason import insertReason
from insertCoverage import insertCoverage
from insertConclusion import insertConclusion
from insertCustomerDocs import insertCostomerDocs


spark = sparkInit()

# insuranceComplaintsData = loadData(spark)

# inserCompany(insuranceComplaintsData)
# insertReason(spark)
# insertCoverage(spark)
# insertConclusion(spark)
insertCostomerDocs(spark)



