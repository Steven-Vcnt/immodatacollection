import util.scrappers as us

figaro_apt = spark.sql('''
SELECT
    SourceLink, mt.id
FROM
    silver.main_table mt
LEFT JOIN silver.meta_table md
    ON md.id=mt.id
WHERE 
    SourceLink LIKE '%figaro%' 
    AND (UpdateDateImage is null OR UpdateDateImage < DATE_ADD(NOW(),-7))
''').limit(100).toPandas()
if len(figaro_apt) == 0:
    dbutils.notebook.exit('Success')
else:
    full_image = us.getSource.GetImageFromHtml(figaro_apt, 'a', 'image-link default-image-background')
    sp_figaroImage = spark.createDataFrame(full_image)
    sp_figaroImage.distinct().createOrReplaceTempView('figaro_image_updates')


# MAGIC %sql
# MAGIC MERGE INTO bronze.figaro_image
# MAGIC USING figaro_image_updates
# MAGIC ON bronze.figaro_image.id=figaro_image_updates.id
# MAGIC and bronze.figaro_image.imageLink = figaro_image_updates.imageLink
# MAGIC and bronze.figaro_image.imagePath = figaro_image_updates.imagePath
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

#sp_figaroImage.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/bronze/figaroImage") 
#spark.sql("CREATE TABLE IF NOT EXISTS bronze.figaro_image USING DELTA LOCATION '/FileStore/bronze/figaroImage'")