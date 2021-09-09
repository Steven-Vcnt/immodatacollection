# Databricks notebook source
import pandas as pd
import requests
from selenium import webdriver
from bs4 import BeautifulSoup
from urllib.request import urlopen
from urllib.error import HTTPError
import re
from datetime import datetime
import urllib.request
import time
import lxml
import bs4.builder._lxml
import lxml.etree

# COMMAND ----------

link='https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/'
uClient= urlopen(link)
page_html= uClient.read()
uClient.close()
page_soup=BeautifulSoup(page_html, "html")
main_file = page_soup.find_all('a', {'class': 'btn-action'})
#dvf_files = main_file.find_all('li', {'class': 'btn-action'})``
dvf_files=[]
for i in range(10):
  dvf_links = re.search("(https?:\/\/[^\"]*)", str(main_file[i])).group(0)
  dvf_files.append(dvf_links)
dvf_files=list(dict.fromkeys(dvf_files))
for link in dvf_files:
  fileId=re.search("(?!(.+)?\/\/?(\?.+)?).*", link).group(0)
  fileName= "dbfs:/FileStore/DVF_file/"+ fileId + ".txt"
  try:
    urllib.request.urlretrieve(link, "/tmp/dvf_file.txt")
    dbutils.fs.mv("file:/tmp/dvf_file.txt", fileName)
  except:
    pass
  else:
    pass

# COMMAND ----------

sp_dvf_file = spark.read.format("csv").option("delimiter", "|").option("header", "true").load("/FileStore/DVF_file/*.txt")

# COMMAND ----------

  sp_dvf_file=sp_dvf_file.withColumnRenamed("Code service CH",'Code_service_CH')

# COMMAND ----------

from pyspark.sql import functions as F

renamed_sp_dvf = sp_dvf_file.select([F.col(col).alias(col.replace(' ', '_')) for col in sp_dvf_file.columns])

# COMMAND ----------

#from pyspark.sql import functions as F
#renamed_sp_dvf = sp_dvf_file.select([F.col(col).alias(col.replace(' ', '_')) for col in sp_dvf_file.columns])
#renamed_sp_dvf.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/bronze/dvf_file") 
#spark.sql("CREATE TABLE IF NOT EXISTS bronze.dvf_file USING DELTA LOCATION '/FileStore/bronze/dvf_file'")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.dvf_file where Commune = 'LEVALLOIS-PERRET'-- ORDER BY `Date mutation` DESC

# COMMAND ----------

#Create SQL view 
df.distinct().createOrReplaceTempView('DVF_updates')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze.figaro_image
# MAGIC USING figaro_image_updates
# MAGIC ON bronze.figaro_image.id=figaro_image_updates.id
# MAGIC and bronze.figaro_image.imageLink = figaro_image_updates.imageLink
# MAGIC and bronze.figaro_image.imagePath = figaro_image_updates.imagePath
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

#Return a evalue when using dbutils.notebook.run for orchestration
dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(`Valeur fonciere`) FROM DVF_updates --where Commune LIKE 'LEVALLOIS%'-- ORDER BY `Date mutation` DESC ;

# COMMAND ----------

display(df)
