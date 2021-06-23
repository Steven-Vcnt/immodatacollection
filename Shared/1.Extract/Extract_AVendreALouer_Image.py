# Databricks notebook source
import pandas as pd
import requests
from selenium import webdriver
from bs4 import BeautifulSoup
pd.options.display.max_colwidth=1000
from urllib.request import urlopen, Request
from urllib.error import HTTPError
import re
from datetime import datetime
import urllib.request
import time
import lxml
import bs4.builder._lxml
import lxml.etree
import requests

# COMMAND ----------

#get all image for figaro source
def GetImageFromHtml(Provider, tag, classType):
  imageTable=pd.DataFrame()
  #Get Link Image
  
  for link, id in zip(Provider["SourceLink"], Provider["id"]):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:63.0) Gecko/20100101 Firefox/63.0'}
    imgeTable_temp=pd.DataFrame()
    print(link)
    req= Request(link, headers=headers)
    uClient= urlopen(req)
    print(uClient)
    page_html= uClient.read()
    uClient.close()
    page_soup=BeautifulSoup(page_html, "html")
    imgs = page_soup.find_all(tag, {'class': classType})
    
    i=0
    imgeDB=[]
    imagePath=[]
    #Get all images of apt
    for each in imgs:
      #imageLink_raw=page_soup.find_all(tag, {'class': classType})[i]
      imageLink = re.search("(https?:\/\/[^\"]*)", str(each)).group(0)
      imgeDB.append(imageLink)
      
      ImageName= "dbfs:/FileStore/images/"+ id +"_"+ str(i)+".jpg"
      imagePath.append(ImageName)
      try:
        urllib.request.urlretrieve(imageLink, "/tmp/imageTemp3.jpg", headers=headers)
        dbutils.fs.mv("file:/tmp/imageTemp3.jpg", ImageName)
      except:
        pass
      else:
        pass
      i=i+1
    imgeTable_temp['imageLink']=imgeDB
    imgeTable_temp['id']=id
    imgeTable_temp['imagePath']= imagePath
    imageTable=imageTable.append(imgeTable_temp)
   
  imageTable['UpdateDateImage']= datetime.now()
  return imageTable

# COMMAND ----------

aval_apt

# COMMAND ----------

#download A vendre a louer
aval_apt=spark.sql('''SELECT SourceLink, mt.id 
FROM silver.main_table mt
LEFT JOIN silver.meta_table md ON md.id=mt.id
where SourceLink LIKE '%avendrealouer%'-- and (UpdateDateImage is null OR UpdateDateImage < DATE_ADD(NOW(),-7)) ''').limit(10).toPandas()

full_image=GetImageFromHtml(aval_apt, 'img', 'SliderImages__ImgStyledSlider-sc-18z29ar-5 esrCRM adviewPhoto')
sp_avalImage=spark.createDataFrame(full_image)
sp_avalImage.distinct().createOrReplaceTempView('aval_image_updates')


# COMMAND ----------

GetImageFromHtml(aval_apt, 'img', 'SliderImages__ImgStyledSlider-sc-18z29ar-5 esrCRM adviewPhoto')

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

#sp_figaroImage.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/bronze/figaroImage") 
#spark.sql("CREATE TABLE IF NOT EXISTS bronze.figaro_image USING DELTA LOCATION '/FileStore/bronze/figaroImage'")
