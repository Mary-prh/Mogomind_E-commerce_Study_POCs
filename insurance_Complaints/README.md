# Insurance Company Complaint
* consumer complaints filed against Insurance companies licensed in Connecticut. This dataset includes the Company, Line of Business, nature of complaint, outcome or resolution, and recovery.

# Dataset Description:
## Features Available in Dataset:
* Company
* File No.
* Opened
* Closed
* Coverage
* SubCoverage
* Reason
* SubReason
* Disposition
* Conclusion
* Recovery
* Status
# Objectives: 
**Create a report of the consumers and their complaints containing the following information:**
* File No.
* company 
* Date file opened
* Date file closed
* Coverage category
* Reason category 
* Conclusion

# Warehousing:
* Using **PySpark** the list of *companies, coverage, reason and conclusion* were extracted from the dataset and loaded into the corresponding tables in the Postgresql
  - For the pairs of *"Coverage and SubCoverage"* and *"Reason and SubReason"* the trees were created in **PySpark** where *Coverage* and *Reason* were the roots while *SubCoverage* and *SubReason* were the leaves
* Two tables were created in Postgresql: ***coverage_group*** and ***reason_group*** 
  - These two tables are created to have access to a unique id for each pair of *"Coverage and SubCoverage"* and *"Reason and SubReason"*

![image](https://user-images.githubusercontent.com/73495027/172914924-41c11dee-973f-422e-9115-3d67ad74492a.png)
               
               
![image](https://user-images.githubusercontent.com/73495027/172915106-dd230dee-52b0-4bda-a268-f4d9d1119ac7.png)


The final table was created by **joining the database existing tables in PySpark** and loading them into the ***customer_claim*** table 
* The customer_claim table includes the following columns:
   - FileNo
   - company_id
   - Date file opened
   - Date file closed
   - coverage_category_id
   - reason_category_id
   - conclusion_id 
