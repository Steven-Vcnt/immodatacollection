# Databricks notebook source
DVF_table = spark.sql("""
SELECT DISTINCT 
  md5( CONCAT_WS('|', *)) AS DVF_ID
  ,NOW() AS CreatedDate
  ,* 
FROM 
  bronze.dvf_file 
WHERE 
  Valeur_fonciere IS NOT NULL
  AND Surface_reelle_bati IS NOT NULL 
  AND Type_local = 'Appartement' 
  AND Valeur_fonciere < 1000000 
  AND  Commune IN ('LEVALLOIS-PERRET', 'ANGERS')
 """)
DVF_table.distinct().createOrReplaceTempView('dvf_file_updates')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO 
# MAGIC   silver.dvf_table
# MAGIC USING dvf_file_updates
# MAGIC   ON silver.dvf_table.DVF_ID = dvf_file_updates.DVF_ID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT *

# COMMAND ----------

#Create dvf table
#DVF_table.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/silver/dvf_table") 
#spark.sql("CREATE TABLE IF NOT EXISTS silver.dvf_table USING DELTA LOCATION '/FileStore/silver/dvf_table'")
