# Databricks notebook source
DVF_table = spark.sql("SELECT DISTINCT md5( CONCAT_WS('|', *)) as DVF_ID, NOW() as CreatedDate, * FROM bronze.dvf_file where Valeur_fonciere is not  null and Surface_reelle_bati is not null and Type_local = 'Appartement' and Valeur_fonciere < 1000000 and  Commune = 'LEVALLOIS-PERRET'")
DVF_table.distinct().createOrReplaceTempView('dvf_file_updates')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT COUNT(*) FROM bronze.dvf_file where Valeur_fonciere is not  null and Surface_reelle_bati is not null and Type_local = 'Appartement' and Valeur_fonciere < 1000000 and  Commune = 'LEVALLOIS-PERRET' and TO_DATE(CAST(UNIX_TIMESTAMP(Date_mutation, 'dd/MM/yyyy') AS TIMESTAMP)) > '2020'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.dvf_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.dvf_file df
# MAGIC INNER JOIN silver.main_table mt ON (mt.m2<df.Surface_reelle_bati*1.10 OR mt.m2>df.Surface_reelle_bati*0.9) and (df.Valeur_fonciere*1.20 > mt.m2 OR df.Valeur_fonciere *0.80 < mt.m2) AND mt.nbPiece=df.Nombre_pieces_principales
# MAGIC where Commune = 'LEVALLOIS-PERRET' and TO_DATE(CAST(UNIX_TIMESTAMP(Date_mutation, 'dd/MM/yyyy') AS TIMESTAMP)) > '2021' and Surface_reelle_bati!=0 and mt.rue is not null and mt.m2>20 and mt.m2<40 and mt.rue='Rivay' and df.VOIE='RIVAY'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.dvf_file where Commune = 'LEVALLOIS-PERRET' and TO_DATE(CAST(UNIX_TIMESTAMP(Date_mutation, 'dd/MM/yyyy') AS TIMESTAMP)) > '2021' and Surface_reelle_bati!=0 and VOIE = 'CHAPTAL'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.image_table where id='d95317172' 

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/FileStore/images/d95317172_*.jpg

# COMMAND ----------

df.image.width = df.select("image.width").show()*2

# COMMAND ----------

from pyspark.ml.image import ImageSchema
image_df = ImageSchema.readImages(df)
display(image_df)

# COMMAND ----------

df = spark.read.format("image").option("dropInvalid", True).load("/FileStore/images/d95317172_*.jpg")
df.select("image.origin", "image.width", "image.height").show(truncate=False, )

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.main_table where CreatedDate > '2021' and rue is not null and rue like '%chaptal%'

# COMMAND ----------

#Create dvf table
#DVF_table.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/silver/dvf_table") 
#spark.sql("CREATE TABLE IF NOT EXISTS silver.dvf_table USING DELTA LOCATION '/FileStore/silver/dvf_table'")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.dvf_table
# MAGIC USING dvf_file_updates
# MAGIC ON silver.dvf_table.DVF_ID = dvf_file_updates.DVF_ID
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
