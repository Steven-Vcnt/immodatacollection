# Databricks notebook source
Testing Github actions API

# COMMAND ----------

# DBTITLE 1,Convert Pandas to spark DF & display DF
sp_MainCastorus=spark.createDataFrame(CastorusMainTable(url))
display(sp_MainCastorus)

# COMMAND ----------

# DBTITLE 1,Create SQL view from Spark DF
sp_MainCastorus.distinct().createOrReplaceTempView('main_castorus_updates')

# COMMAND ----------

# DBTITLE 1,Overwrite delta file & Create new Delta Table from the delta file
sp_MainCastorus.distinct().write.mode("append").option("overwriteschema", "true").format("delta").save("/user/main_castorus") 
spark.sql("CREATE TABLE IF NOT EXISTS main_castorus USING DELTA LOCATION '/user/main_castorus'")

# COMMAND ----------

# DBTITLE 1,Select past version of the Delta table
# MAGIC %sql
# MAGIC SELECT * FROM main_castorus VERSION AS OF 1

# COMMAND ----------

# DBTITLE 1,Restore past version of the Delta table
# MAGIC %sql
# MAGIC RESTORE TABLE main_castorus TO VERSION AS OF 2

# COMMAND ----------

# DBTITLE 1,Get History Delta Table
# MAGIC %sql
# MAGIC DESCRIBE HISTORY main_castorus

# COMMAND ----------

# DBTITLE 1,Upsert data
# MAGIC %sql
# MAGIC MERGE INTO main_castorus
# MAGIC USING main_castorus_updates
# MAGIC ON main_castorus.id=main_castorus_updates.id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
