-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.ls("FileStore/tables")  

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ls /tmp/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_2021 = spark.read.csv("/FileStore/tables/clinicaltrial_2021.csv",header="true",inferSchema="true",sep="|")
-- MAGIC display(clinicaltrial_2021)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_2021.createOrReplaceTempView("clinicaltrial_2021View")

-- COMMAND ----------


select Id,Sponsor,Status,Start from clinicaltrial_2021View

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharma = spark.read.csv("/FileStore/tables/pharma.csv",header="true",inferSchema="true")
-- MAGIC display(pharma)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC pharma.createOrReplaceTempView("pharmaView")

-- COMMAND ----------


select Company,Parent_Company
from pharmaView

-- COMMAND ----------


---------------Question 1-----------------
SELECT DISTINCT Count(Status) as 2021_Status
FROM clinicaltrial_2021View

-- COMMAND ----------



---------------Question 2-----------------
SELECT Type, Count(Type) as Frequency_of_Type
FROM clinicaltrial_2021View
group by Type
order by Frequency_of_Type desc



-- COMMAND ----------


---------------Question 3-----------------
WITH selected_conditions AS (
SELECT split(Conditions, ',') AS Conditions
FROM clinicaltrial_2021View
),
exp_conditions AS (
SELECT explode(Conditions) AS Conditions
FROM selected_conditions
)
SELECT Conditions, COUNT(*) AS Frequency
FROM exp_conditions
GROUP BY Conditions
ORDER BY Frequency DESC
LIMIT 5;

-- COMMAND ----------


------------Question 4---------------

SELECT Sponsor, COUNT('Sponsor') AS Frequency FROM clinicaltrial_2021View 
LEFT JOIN pharmaView ON clinicaltrial_2021View.Sponsor = pharmaView.Parent_Company 
WHERE pharmaView.Parent_Company IS NULL 
GROUP BY Sponsor 
ORDER BY Frequency DESC limit 10;

-- COMMAND ----------


---------------Question 5-----------------
SELECT Completion AS Month, COUNT(*) AS NumberOfCompStudies
FROM clinicaltrial_2021View
WHERE Completion LIKE '%2021' AND Status = 'Completed'
GROUP BY Month
ORDER BY Month(CAST(TO_DATE(Completion, 'MMM yyyy') AS DATE));
