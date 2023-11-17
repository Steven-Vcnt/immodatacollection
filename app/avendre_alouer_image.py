import util.scrappers as us

# download A vendre a louer
aval_apt = spark.sql('''SELECT SourceLink, mt.id 
FROM silver.main_table mt
LEFT JOIN silver.meta_table md ON md.id=mt.id
where SourceLink LIKE '%avendrealouer%' and (UpdateDateImage is null OR UpdateDateImage < DATE_ADD(NOW(),-7)) ''').limit(1000).toPandas()
if len(aval_apt) == 0:
    dbutils.notebook.exit('Success')
else:
    full_image = us.getSource.GetImageFromHtml(aval_apt, 'img', 'SliderImages__ImgStyledSlider-sc-18z29ar-5 esrCRM adviewPhoto')
    sp_avalImage = spark.createDataFrame(full_image)
    sp_avalImage.distinct().createOrReplaceTempView('aval_image_updates')


# MAGIC %sql
# MAGIC MERGE INTO bronze.aval_image
# MAGIC USING aval_image_updates
# MAGIC ON bronze.aval_image.id=aval_image_updates.id
# MAGIC and bronze.aval_image.imageLink = aval_image_updates.imageLink
# MAGIC and bronze.aval_image.imagePath = aval_image_updates.imagePath
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *


#sp_avalImage.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/bronze/avalImage") 
#spark.sql("CREATE TABLE IF NOT EXISTS bronze.aval_image USING DELTA LOCATION '/FileStore/bronze/avalImage'")

