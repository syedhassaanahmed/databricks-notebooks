-- Databricks notebook source
-- MAGIC %md #### Credits to [Colin Cookman](https://colincookman.wordpress.com/) for creating [Pakistan Census Dataset](https://github.com/colincookman/pakistan_census)

-- COMMAND ----------

--Uncomment to redownload the dataset
--%sh curl https://raw.githubusercontent.com/colincookman/pakistan_census/master/Pakistan_2017_Census_Blocks.csv > /dbfs/Pakistan_2017_Census_Blocks.csv

-- COMMAND ----------

DROP TABLE IF EXISTS Pakistan_2017_Census_Blocks;

CREATE TEMPORARY TABLE Pakistan_2017_Census_Blocks
USING csv
OPTIONS (
  path "dbfs:/Pakistan_2017_Census_Blocks.csv", 
  header "true", 
  mode "FAILFAST")

-- COMMAND ----------

DROP TABLE IF EXISTS population_by_province;

CREATE TEMPORARY VIEW population_by_province AS
SELECT 
  province
  , sum(population) AS population
FROM Pakistan_2017_Census_Blocks
GROUP BY province

-- COMMAND ----------

-- MAGIC %md #### Population per province before KPK FATA merger

-- COMMAND ----------

SELECT *
  , (population / sum(population) OVER (ORDER BY 1)) * 100 AS percentage
FROM population_by_province
ORDER BY percentage DESC

-- COMMAND ----------

-- MAGIC %md #### Population per province after KPK FATA merger

-- COMMAND ----------

SELECT *
  , (population / sum(population) OVER (ORDER BY 1)) * 100 AS percentage
FROM 
(
  SELECT * 
  FROM population_by_province 
  WHERE province NOT IN ("KHYBER PAKHTUNKHWA", "FATA")
  UNION ALL
  SELECT 
    "KPK + FATA" AS province
    , sum(population) AS population 
  FROM population_by_province
  WHERE province IN ("KHYBER PAKHTUNKHWA", "FATA")
)
ORDER BY percentage DESC

-- COMMAND ----------


