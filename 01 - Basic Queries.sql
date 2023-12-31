-- Databricks notebook source
-- MAGIC
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Tables
-- MAGIC Run the cell below to create tables for the questions in this notebook. 

-- COMMAND ----------

-- MAGIC %run ../Utilities/01-CreateTables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 1: Modify a Table
-- MAGIC
-- MAGIC ### Summary
-- MAGIC Modify the columns in table **`discounts`** to match the provided schema.
-- MAGIC
-- MAGIC ### Steps to Complete
-- MAGIC Write a SQL query that achieves the following:
-- MAGIC * Selects columns **`discountId`**, **`code`**, and **`price`**
-- MAGIC * Converts column **`discountId`** to type **`Long`**
-- MAGIC * Converts column **`price`** to type **`Double`**, multiplies it by 100 and then converts to type **`Integer`**
-- MAGIC * Saves this to a temporary view named **`q1Results`**

-- COMMAND ----------

select * from discounts limit 5

-- COMMAND ----------

-- TODO Answer 1
create or replace temporary view q1Results as
  select 
    cast(discountId as long),
    code,
    cast(cast(price as double)*100 as int) as price
  from discounts;

-- COMMAND ----------

select * from q1Results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 2: Basic Math and Drop Columns
-- MAGIC
-- MAGIC ### Summary
-- MAGIC Modify the columns in table **`discounts2`** to match the provided schema.
-- MAGIC
-- MAGIC ### Steps to complete
-- MAGIC Write a SQL query on the table **`discounts2`** that achieves the following:
-- MAGIC * Converts column **`active`** to type **`Boolean`**
-- MAGIC * Creates the column **`price`** by converting the column **`cents`** to type **`Double`** and dividing by 100
-- MAGIC * Drops the **`cents`** column
-- MAGIC * Saves this to a temporary view named **`q2Results`**

-- COMMAND ----------

select * from discounts2

-- COMMAND ----------

-- TODO Answer 2
create or replace temporary view q2Results as
  select cast(active as Boolean), cast(cents as double)/100 as price
  from discounts2;

-- COMMAND ----------

select * from q2Results

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
