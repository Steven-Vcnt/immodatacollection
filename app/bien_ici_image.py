import util.scrappers as us

bienici_apt = spark.sql('''SELECT DISTINCT SourceLink, mt.id
FROM silver.main_table mt
LEFT JOIN silver.meta_table md ON md.id=mt.id
where SourceLink LIKE '%bienici%' and (UpdateDateImage is null OR UpdateDateImage < DATE_ADD(NOW(),-7)) ''').limit(100).toPandas()

if len(bienici_apt) == 0:
    dbutils.notebook.exit('Success')
else:
    bienicitable = us.getSource.bieniciImage(bienici_apt, "//div[@class='w']/img", "src")
    # Create SQL view
    sp_bienicitable = spark.createDataFrame(bienicitable).distinct().createOrReplaceTempView('bienici_image_updates')

spark.sql('''MERGE INTO bronze.bienici_image
USING bienici_image_updates
ON bronze.bienici_image.id=bienici_image_updates.id
and bronze.bienici_image.imageLink=bienici_image_updates.imageLink
and  bronze.bienici_image.imagePath = bienici_image_updates.imagePath
WHEN MATCHED THEN
UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
''')
sp_bienicitable.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/bronze/bienici_image") 
spark.sql("CREATE TABLE IF NOT EXISTS bronze.bienici_image USING DELTA LOCATION '/FileStore/bronze/bienici_image'")
