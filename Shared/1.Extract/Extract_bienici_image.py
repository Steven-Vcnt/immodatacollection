# Databricks notebook source
# MAGIC %md
# MAGIC apt-get update 
# MAGIC apt install chromium-chromedriver -y

# COMMAND ----------

#before launching it got to terminal and install
#!apt-get update 
#!apt install chromium-chromedriver 
import pandas as pd
import requests
from selenium import webdriver
from bs4 import BeautifulSoup
#pd.options.display.max_colwidth=1000
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

#get all image for bien ici source
def bieniciImage(Provider, xpath, attribute):
  imageTable=pd.DataFrame()
  for link, id in zip(Provider["SourceLink"], Provider["id"]):
    imgeTable_temp=pd.DataFrame()
    bienici=jsDriver(link)
    time.sleep(3)
    src = bienici.find_elements_by_xpath(xpath)
    imgeDB=[]
    imagePath=[]
    i=0
    imageLink='not available'
    imageName='not available'
    for each in src:
      imageLink=each.get_attribute(attribute)
      imgeDB.append(imageLink)
      imageName= "dbfs:/FileStore/images/"+ id +"_"+ str(i)+".jpg"
      imagePath.append(imageName)
      try:
        urllib.request.urlretrieve(imageLink, "/tmp/imageTempbi.jpg")
        dbutils.fs.mv("file:/tmp/imageTempbi.jpg", imageName)
      except:
        pass
      else:
        pass
      i=i+1
    if imageLink=='not available':
      imgeDB.append(imageLink)
      imagePath.append(imageName)
    else:
      pass
    imgeTable_temp['imageLink']=imgeDB
    imgeTable_temp['id']=id
    imgeTable_temp['imagePath']= imagePath
    imageTable=imageTable.append(imgeTable_temp)
  imageTable['UpdateDateImage']= datetime.now()
  return imageTable
    
#Use Chrome Headless Browser
def jsDriver(url):
  user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36'
  options = webdriver.ChromeOptions()
  options.add_argument('--headless')
  options.add_argument('--no-sandbox')
  options.add_argument('--disable-dev-shm-usage')
  options.add_argument("start-maximized")
  options.add_experimental_option("excludeSwitches", ["enable-automation"])
  options.add_argument(f'user-agent={user_agent}')
  options.add_experimental_option('useAutomationExtension', False)
  jsDriver =webdriver.Chrome('chromedriver',options=options)
  jsDriver.get(url)
  return jsDriver

# COMMAND ----------

bienici_apt=spark.sql('''SELECT DISTINCT SourceLink, mt.id
FROM silver.main_table mt
LEFT JOIN silver.meta_table md ON md.id=mt.id
where SourceLink LIKE '%bienici%' and (UpdateDateImage is null OR UpdateDateImage < DATE_ADD(NOW(),-7)) ''').limit(100).toPandas()

# COMMAND ----------

if len(bienici_apt) == 0:
  dbutils.notebook.exit('Success')
else:
  bienicitable= bieniciImage(bienici_apt, "//div[@class='w']/img", "src")
  #Create SQL view
  sp_bienicitable=spark.createDataFrame(bienicitable).distinct().createOrReplaceTempView('bienici_image_updates')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze.bienici_image
# MAGIC USING bienici_image_updates
# MAGIC ON bronze.bienici_image.id=bienici_image_updates.id
# MAGIC and bronze.bienici_image.imageLink=bienici_image_updates.imageLink
# MAGIC and  bronze.bienici_image.imagePath = bienici_image_updates.imagePath
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

#Return a evalue when using dbutils.notebook.run for orchestration
dbutils.notebook.exit('Success')

# COMMAND ----------

#sp_bienicitable.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/bronze/bienici_image") 
#spark.sql("CREATE TABLE IF NOT EXISTS bronze.bienici_image USING DELTA LOCATION '/FileStore/bronze/bienici_image'")
