# Databricks notebook source
image_table=spark.sql(''' 
SELECT DISTINCT * FROM  bronze.figaro_image 
UNION ALL
SELECT DISTINCT * FROM bronze.bienici_image where id !='d102114561'
UNION ALL
SELECT DISTINCT * FROM bronze.aval_image
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

#image_table.distinct().write.mode("Overwrite").option("overwriteschema", "true").format("delta").save("/FileStore/silver/image_table") 
#spark.sql("CREATE TABLE IF NOT EXISTS silver.image_table USING DELTA LOCATION '/FileStore/silver/image_table'")

# COMMAND ----------


#%sql
#SELECT id, COUNT(*) FROM image_table_updates GROUP BY id, imageLink, ImagePath ORDER BY COUNT(*) DESC

# COMMAND ----------

#%sql
#delete FROM bronze.aval_image WHERE id ='d105314719'  AND imagePath='not available' AND UpdateDateImage<'2022-07-02'

# COMMAND ----------


