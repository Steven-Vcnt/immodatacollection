# Databricks notebook source
image_table=spark.sql(''' 
SELECT * FROM  bronze.figaro_image 
UNION ALL
SELECT * FROM bronze.bienici_image
UNION ALL
SELECT * FROM bronze.aval_image
''').createOrReplaceTempView('image_table_updates')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.image_table
# MAGIC USING image_table_updates
# MAGIC ON silver.image_table.id=image_table_updates.id
# MAGIC and silver.image_table.imageLink = image_table_updates.imageLink
# MAGIC and silver.image_table.imagePath = image_table_updates.imagePath
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

#Return a evalue when using dbutils.notebook.run for orchestration
dbutils.notebook.exit('Success')

# COMMAND ----------

#image_table.distinct().write.mode("Overwrite").option("overwriteschema", "true").format("delta").save("/FileStore/silver/image_table") 
#spark.sql("CREATE TABLE IF NOT EXISTS silver.image_table USING DELTA LOCATION '/FileStore/silver/image_table'")
