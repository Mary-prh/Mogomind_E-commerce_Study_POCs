import imp
from spark import sparkInit
from city import city
from gender import gender
from customerType import customerType
from productLine import loadProductLine
from payment import loadPaymentType
from customer import loadCustomerGroups

spark = sparkInit()

# city(spark)
# gender(spark)
# customerType(spark)
# loadProductLine(spark)
# loadPaymentType(spark)
loadCustomerGroups(spark)
