# Databricks notebook source
meta_table=spark.sql(''' SELECT DISTINCT mc.id, MAX(mc.UpdateDateMainTable) as UpdateDateMainTable , MAX(cc.UpdateDateChange) as UpdateDateChange, max(it.UpdateDateImage) as UpdateDateImage
FROM bronze.main_castorus mc
LEFT JOIN bronze.castorus_change cc ON mc.id=cc.id
LEFT JOIN silver.image_table it ON it.id=mc.id
group by mc.id ''')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.meta_table where UpdateDateImage is not null

# COMMAND ----------

meta_table.createOrReplaceTempView('meta_table_updates')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.meta_table
# MAGIC USING meta_table_updates
# MAGIC ON silver.meta_table.id=meta_table_updates.id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

#Return a evalue when using dbutils.notebook.run for orchestration
dbutils.notebook.exit('Success')

# COMMAND ----------

#meta_table.distinct().write.mode("Overwrite").option("overwriteschema", "true").format("delta").save("/FileStore/silver/meta_table") 
#spark.sql("CREATE TABLE IF NOT EXISTS silver.meta_table USING DELTA LOCATION '/FileStore/silver/meta_table'")
