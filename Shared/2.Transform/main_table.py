# Databricks notebook source
main_table=spark.sql(''' 
SELECT DISTINCT id, prix, m2, `prix/m2` AS prix_m2,  vue AS castorusUpdateDate, `piec.` AS nbPiece, titre AS description,  
CASE WHEN ville like '%(%' THEN SUBSTRING(REPLACE(ville, ')',''), CHARINDEX('(',ville)+1,LENGTH(ville))ELSE null END AS rue,
CASE WHEN ville like '%(%' THEN LEFT(ville, CHARINDEX('(',ville)-1) ELSE ville END AS ville,
DATE_ADD( UpdateDateMainTable, INT(- depuis)) AS CreatedDate, SourceLink, Link
 from bronze.main_castorus ''')

# COMMAND ----------

main_table.createOrReplaceTempView('main_table_updates')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.main_table
# MAGIC USING main_table_updates
# MAGIC ON silver.main_table.id=main_table_updates.id
# MAGIC WHEN MATCHED AND silver.main_table.castorusUpdateDate < main_table_updates.castorusUpdateDate THEN
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

#main_table.distinct().write.mode("Overwrite").option("overwriteschema", "true").format("delta").save("/FileStore/silver/main_table") 
#spark.sql("CREATE TABLE IF NOT EXISTS silver.main_table USING DELTA LOCATION '/FileStore/silver/main_table'")
