# Mogomind_E-commerce_Study_POCs
## Dataset: 
- The dataset has information of 100k orders from 2016 to 2018 made at multiple marketplaces in Brazil

- Customer dataset consits the information about the customer_id and customer cities.
- The geolocation of the cities can be found in the geolocation_dataset.
- The order dataset provides information about the customers and their purchase history.

## Dataset Schema: 
- ![image](https://user-images.githubusercontent.com/73495027/169897014-ab0a7d2f-0662-48e4-9c71-227cc48ed010.png) 

## Objectives: 
- Creating a report of the monthly number of orders in each city.

## Warehousing:
- Base table: City (cityId, cityName)
- By using PySpark, the list of the existing cities extracted from geolocation_dataset.csv and loaded in "City table" of Postgresql.
- The data of orders_dataset.csv and customers_dataset.csv , and table of "City" from Postgresql were joined in Pyspark. 
- Data were grouped by "ordertime" and "cityname"
- Data were loaded in warehouse table of "cityorder" in Postgresql.
-   

 
